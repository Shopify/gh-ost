/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package binlog

import (
	"fmt"
	"sync"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/mysql"
	"github.com/github/gh-ost/go/sql"

	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"
	"golang.org/x/net/context"
)

type GoMySQLReader struct {
	migrationContext        *base.MigrationContext
	connectionConfig        *mysql.ConnectionConfig
	binlogSyncer            *replication.BinlogSyncer
	binlogStreamer          *replication.BinlogStreamer
	currentCoordinates      mysql.BinlogCoordinates
	currentCoordinatesMutex *sync.Mutex
	// LastTrxCoords are the coordinates of the last transaction completely read.
	// If using the file coordinates it is binlog position of the transaction's XID event.
	LastTrxCoords mysql.BinlogCoordinates
	// currentTrxCoords is set once per GTIDEvent and shared by all RowsEvents within
	// the same transaction. It points to currentCoordinates (a lazy *GTIDBinlogCoordinates),
	// which is replaced at the next GTIDEvent — so old entries retain valid references.
	// Only accessed from within the StreamEvents goroutine; no mutex needed.
	currentTrxCoords mysql.BinlogCoordinates
	// lastCommittedCoords is the GTIDBinlogCoordinates from the most recently seen
	// XIDEvent (or the initial coordinates). WithPendingGTID aliases its GTIDSet as
	// the base for each new in-flight coord, so the Clone is deferred until actually
	// needed. Only written from within StreamEvents; no mutex needed.
	lastCommittedCoords *mysql.GTIDBinlogCoordinates
}

func NewGoMySQLReader(migrationContext *base.MigrationContext) *GoMySQLReader {
	connectionConfig := migrationContext.InspectorConnectionConfig
	return &GoMySQLReader{
		migrationContext:        migrationContext,
		connectionConfig:        connectionConfig,
		currentCoordinatesMutex: &sync.Mutex{},
		binlogSyncer: replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
			ServerID:                uint32(migrationContext.ReplicaServerId),
			Flavor:                  gomysql.MySQLFlavor,
			Host:                    connectionConfig.Key.Hostname,
			Port:                    uint16(connectionConfig.Key.Port),
			User:                    connectionConfig.User,
			Password:                connectionConfig.Password,
			TLSConfig:               connectionConfig.TLSConfig(),
			UseDecimal:              true,
			TimestampStringLocation: time.UTC,
			MaxReconnectAttempts:    migrationContext.BinlogSyncerMaxReconnectAttempts,
		}),
	}
}

// ConnectBinlogStreamer
func (this *GoMySQLReader) ConnectBinlogStreamer(coordinates mysql.BinlogCoordinates) (err error) {
	if coordinates.IsEmpty() {
		return this.migrationContext.Log.Errorf("Empty coordinates at ConnectBinlogStreamer()")
	}

	this.currentCoordinatesMutex.Lock()
	defer this.currentCoordinatesMutex.Unlock()
	this.currentCoordinates = coordinates
	this.migrationContext.Log.Infof("Connecting binlog streamer at %+v", coordinates)

	// Start sync with specified GTID set or binlog file and position
	if this.migrationContext.UseGTIDs {
		coords := coordinates.(*mysql.GTIDBinlogCoordinates)
		this.lastCommittedCoords = coords
		this.binlogStreamer, err = this.binlogSyncer.StartSyncGTID(coords.GTIDSet)
	} else {
		coords := this.currentCoordinates.(*mysql.FileBinlogCoordinates)
		this.binlogStreamer, err = this.binlogSyncer.StartSync(gomysql.Position{
			Name: coords.LogFile,
			Pos:  uint32(coords.LogPos)},
		)
	}
	return err
}

func (this *GoMySQLReader) GetCurrentBinlogCoordinates() mysql.BinlogCoordinates {
	this.currentCoordinatesMutex.Lock()
	defer this.currentCoordinatesMutex.Unlock()
	return this.currentCoordinates.Clone()
}

func (this *GoMySQLReader) handleRowsEvent(ev *replication.BinlogEvent, rowsEvent *replication.RowsEvent, entriesChannel chan<- *BinlogEntry) error {
	var currentCoords mysql.BinlogCoordinates
	if this.migrationContext.UseGTIDs && this.currentTrxCoords != nil {
		currentCoords = this.currentTrxCoords
	} else {
		currentCoords = this.GetCurrentBinlogCoordinates()
	}

	dml := ToEventDML(ev.Header.EventType.String())
	if dml == NotDML {
		return fmt.Errorf("Unknown DML type: %v", ev.Header.EventType)
	}

	// Convert schema and table names once per RowsEvent, not per row
	schemaName := string(rowsEvent.Table.Schema)
	tableName := string(rowsEvent.Table.Table)

	for i, row := range rowsEvent.Rows {
		if dml == UpdateDML && i%2 == 1 {
			// An update has two rows (WHERE+SET)
			// We do both at the same time
			continue
		}
		binlogEntry := NewBinlogEntryAt(currentCoords)
		binlogEntry.DmlEvent = NewBinlogDMLEvent(schemaName, tableName, dml)

		switch dml {
		case InsertDML:
			{
				binlogEntry.DmlEvent.NewColumnValues = sql.ToColumnValues(row)
			}
		case UpdateDML:
			{
				binlogEntry.DmlEvent.WhereColumnValues = sql.ToColumnValues(row)
				binlogEntry.DmlEvent.NewColumnValues = sql.ToColumnValues(rowsEvent.Rows[i+1])
			}
		case DeleteDML:
			{
				binlogEntry.DmlEvent.WhereColumnValues = sql.ToColumnValues(row)
			}
		}

		// The channel will do the throttling. Whoever is reading from the channel
		// decides whether action is taken synchronously (meaning we wait before
		// next iteration) or asynchronously (we keep pushing more events)
		// In reality, reads will be synchronous
		entriesChannel <- binlogEntry
	}
	return nil
}

// StreamEvents
func (this *GoMySQLReader) StreamEvents(canStopStreaming func() bool, entriesChannel chan<- *BinlogEntry) error {
	if canStopStreaming() {
		return nil
	}
	for {
		if canStopStreaming() {
			break
		}
		ev, err := this.binlogStreamer.GetEvent(context.Background())
		if err != nil {
			return err
		}

		// Update binlog coords if using file-based coords.
		// GTID coordinates are updated on receiving GTID events.
		if !this.migrationContext.UseGTIDs {
			this.currentCoordinatesMutex.Lock()
			coords := this.currentCoordinates.(*mysql.FileBinlogCoordinates)
			prevCoords := coords.Clone().(*mysql.FileBinlogCoordinates)
			coords.LogPos = int64(ev.Header.LogPos)
			coords.EventSize = int64(ev.Header.EventSize)
			if coords.IsLogPosOverflowBeyond4Bytes(prevCoords) {
				this.currentCoordinatesMutex.Unlock()
				return fmt.Errorf("Unexpected rows event at %+v, the binlog end_log_pos is overflow 4 bytes", coords)
			}
			this.currentCoordinatesMutex.Unlock()
		}

		switch event := ev.Event.(type) {
		case *replication.GTIDEvent:
			if !this.migrationContext.UseGTIDs {
				continue
			}
			sid, err := uuid.FromBytes(event.SID)
			if err != nil {
				return err
			}
			pending := this.lastCommittedCoords.WithPendingGTID(sid, event.GNO)
			this.currentCoordinatesMutex.Lock()
			this.currentCoordinates = pending
			this.currentCoordinatesMutex.Unlock()
			this.currentTrxCoords = pending
		case *replication.RotateEvent:
			if this.migrationContext.UseGTIDs {
				continue
			}
			this.currentCoordinatesMutex.Lock()
			coords := this.currentCoordinates.(*mysql.FileBinlogCoordinates)
			coords.LogFile = string(event.NextLogName)
			this.migrationContext.Log.Infof("rotate to next log from %s:%d to %s", coords.LogFile, int64(ev.Header.LogPos), event.NextLogName)
			this.currentCoordinatesMutex.Unlock()
		case *replication.XIDEvent:
			if this.migrationContext.UseGTIDs {
				// go-mysql allocates a fresh MysqlGTIDSet for every XIDEvent it decodes
				// from the binlog stream, so we can alias event.GSet directly without
				// cloning it. The pointer is then shared by LastTrxCoords and
				// lastCommittedCoords. lastCommittedCoords is subsequently used as the
				// base inside WithPendingGTID: it is cloned there only if a comparison
				// or string representation is actually requested, and never mutated.
				// Any future code that modifies the set after this point must Clone first.
				committed := &mysql.GTIDBinlogCoordinates{GTIDSet: event.GSet.(*gomysql.MysqlGTIDSet)}
				this.LastTrxCoords = committed
				this.lastCommittedCoords = committed
			} else {
				this.LastTrxCoords = this.currentCoordinates.Clone()
			}
		case *replication.RowsEvent:
			if err := this.handleRowsEvent(ev, event, entriesChannel); err != nil {
				return err
			}
		}
	}
	this.migrationContext.Log.Debugf("done streaming events")

	return nil
}

func (this *GoMySQLReader) Close() error {
	this.binlogSyncer.Close()
	return nil
}
