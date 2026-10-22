package binlog

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/metrics"
	"github.com/github/gh-ost/go/mysql"
	"github.com/github/gh-ost/go/sql"
	"github.com/go-mysql-org/go-mysql/canal"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

// CanalEventHandler translates Canal callbacks into gh-ost's ordered event stream.
// The caller must configure Canal without a dump and start it at the requested coordinates.
type CanalEventHandler struct {
	canal.DummyEventHandler
	migrationContext         *base.MigrationContext
	entries                  chan<- *BinlogEntry
	coordinates              mysql.BinlogCoordinates
	lastTrxCoords            mysql.BinlogCoordinates
	transactionHasRows       bool
	transactionRowEventTotal int64
	transactionRowTotal      int64
	previousLastCommitted    int64
	shouldStop               func() bool
	waitForDelivery          bool
	mu                       sync.Mutex
}

var _ canal.EventHandler = (*CanalEventHandler)(nil)

func NewCanalEventHandler(ctx *base.MigrationContext, entries chan<- *BinlogEntry, start mysql.BinlogCoordinates) *CanalEventHandler {
	return &CanalEventHandler{migrationContext: ctx, entries: entries, coordinates: start.Clone()}
}

func (h *CanalEventHandler) String() string { return "gh-ost canal event handler" }

func (h *CanalEventHandler) GetCurrentBinlogCoordinates() mysql.BinlogCoordinates {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.coordinates.Clone()
}

func (h *CanalEventHandler) LastTransactionCoordinates() mysql.BinlogCoordinates {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.lastTrxCoords == nil {
		return nil
	}
	return h.lastTrxCoords.Clone()
}

func (h *CanalEventHandler) OnRotate(_ *replication.EventHeader, event *replication.RotateEvent) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if coords, ok := h.coordinates.(*mysql.FileBinlogCoordinates); ok {
		coords.LogFile = string(event.NextLogName)
		coords.LogPos = int64(event.Position)
	}
	return nil
}

func (h *CanalEventHandler) flushTransactionMetrics() {
	if h.transactionRowEventTotal != 0 {
		metrics.RecordBinlogTransactionSize(h.migrationContext.Metrics, h.transactionRowEventTotal, h.transactionRowTotal)
		h.transactionRowEventTotal = 0
		h.transactionRowTotal = 0
	}
}

func (h *CanalEventHandler) OnGTID(_ *replication.EventHeader, event gomysql.BinlogGTIDEvent) error {
	if !h.migrationContext.UseGTIDs {
		return nil
	}
	h.flushTransactionMetrics()
	if mysqlGTID, ok := event.(*replication.GTIDEvent); ok {
		if mysqlGTID.TransactionLength > 0 {
			metrics.RecordGTIDTransactionLengthBytes(h.migrationContext.Metrics, mysqlGTID.TransactionLength)
		}
		if mysqlGTID.LastCommitted != h.previousLastCommitted {
			metrics.RecordUnfilteredCommitGroupSize(h.migrationContext.Metrics, float64(mysqlGTID.SequenceNumber-mysqlGTID.LastCommitted))
		}
		h.previousLastCommitted = mysqlGTID.LastCommitted
	}
	next, err := event.GTIDNext()
	if err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	coords, ok := h.coordinates.(*mysql.GTIDBinlogCoordinates)
	if !ok {
		return nil
	}
	if coords.GTIDSet == nil {
		coords.GTIDSet = next
		return nil
	}
	return coords.GTIDSet.Update(next.String())
}

func (h *CanalEventHandler) OnRow(event *canal.RowsEvent) error {
	if event.Table == nil {
		return fmt.Errorf("canal row event has no table metadata")
	}
	var dml EventDML
	switch event.Action {
	case canal.InsertAction:
		dml = InsertDML
	case canal.UpdateAction:
		dml = UpdateDML
		if len(event.Rows)%2 != 0 {
			return fmt.Errorf("canal update event for %s.%s has an odd number of row images", event.Table.Schema, event.Table.Name)
		}
	case canal.DeleteAction:
		dml = DeleteDML
	default:
		return fmt.Errorf("unknown canal row action: %s", event.Action)
	}

	rowCount := int64(RowsInRowsEvent(len(event.Rows), dml))
	metrics.RecordBinlogRowsEventProcessed(h.migrationContext.Metrics, event.Table.Name, dml.EventTypeTag(), rowCount)
	if !strings.EqualFold(event.Table.Schema, h.migrationContext.DatabaseName) ||
		(!strings.EqualFold(event.Table.Name, h.migrationContext.OriginalTableName) &&
			!strings.EqualFold(event.Table.Name, h.migrationContext.GetChangelogTableName())) {
		metrics.RecordBinlogRowsEventFiltered(h.migrationContext.Metrics, event.Table.Name, dml.EventTypeTag(), rowCount)
		return nil
	}
	metrics.RecordBinlogRowsEventConsumed(h.migrationContext.Metrics, event.Table.Name, dml.EventTypeTag(), rowCount)
	metrics.RecordBinlogRowsInEvent(h.migrationContext.Metrics, event.Table.Name, rowCount)
	h.transactionRowEventTotal++
	h.transactionRowTotal += rowCount
	beforeChannel := time.Now()
	defer func() {
		metrics.RecordBinlogStreamerBlockedOnOutChannel(h.migrationContext.Metrics, time.Since(beforeChannel))
	}()
	h.mu.Lock()
	if file, ok := h.coordinates.(*mysql.FileBinlogCoordinates); ok {
		file.LogPos = int64(event.Header.LogPos)
		file.EventSize = int64(event.Header.EventSize)
	}
	coords := h.coordinates.Clone()
	h.mu.Unlock()

	for i, row := range event.Rows {
		if h.shouldStop != nil && h.shouldStop() {
			return errCanalStop
		}
		if dml == UpdateDML && i%2 == 1 {
			continue
		}
		entry := NewBinlogEntryAt(coords.Clone())
		entry.DmlEvent = NewBinlogDMLEvent(event.Table.Schema, event.Table.Name, dml)
		switch dml {
		case InsertDML:
			entry.DmlEvent.NewColumnValues = sql.ToColumnValues(row)
		case UpdateDML:
			entry.DmlEvent.WhereColumnValues = sql.ToColumnValues(row)
			entry.DmlEvent.NewColumnValues = sql.ToColumnValues(event.Rows[i+1])
		case DeleteDML:
			entry.DmlEvent.WhereColumnValues = sql.ToColumnValues(row)
		}
		if h.waitForDelivery {
			entry.DeliveryAck = make(chan struct{})
		}
		if err := base.SendWithContext(h.migrationContext.GetContext(), h.entries, entry); err != nil {
			return err
		}
		h.transactionHasRows = true
		if entry.DeliveryAck != nil {
			select {
			case <-entry.DeliveryAck:
			case <-h.migrationContext.GetContext().Done():
				return h.migrationContext.GetContext().Err()
			}
		}
	}
	return nil
}

func (h *CanalEventHandler) OnXID(header *replication.EventHeader, nextPos gomysql.Position) error {
	if !h.migrationContext.UseGTIDs {
		h.flushTransactionMetrics()
	}
	h.mu.Lock()
	if file, ok := h.coordinates.(*mysql.FileBinlogCoordinates); ok {
		file.LogFile = nextPos.Name
		file.LogPos = int64(nextPos.Pos)
		file.EventSize = int64(header.EventSize)
	}
	coords := h.coordinates.Clone()
	h.mu.Unlock()
	if h.transactionHasRows {
		if err := base.SendWithContext(h.migrationContext.GetContext(), h.entries, &BinlogEntry{Coordinates: coords.Clone(), TransactionComplete: true}); err != nil {
			return err
		}
		h.transactionHasRows = false
	}
	h.mu.Lock()
	h.lastTrxCoords = coords
	h.mu.Unlock()
	if h.shouldStop != nil && h.shouldStop() {
		return errCanalStop
	}
	return nil
}
