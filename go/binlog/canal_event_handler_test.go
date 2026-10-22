package binlog

import (
	"context"
	"testing"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/metrics"
	"github.com/github/gh-ost/go/mysql"
	"github.com/go-mysql-org/go-mysql/canal"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/stretchr/testify/require"
)

func TestCanalEventHandlerFilePositionsAndTransactions(t *testing.T) {
	ctx := base.NewMigrationContext()
	defer ctx.CancelContext()
	ctx.DatabaseName = "test"
	ctx.OriginalTableName = "users"
	entries := make(chan *BinlogEntry, 8)
	h := NewCanalEventHandler(ctx, entries, mysql.NewFileBinlogCoordinates("binlog.000001", 4))

	require.NoError(t, h.OnRow(&canal.RowsEvent{
		Table:  &schema.Table{Schema: "other", Name: "users"},
		Action: canal.InsertAction, Rows: [][]any{{int64(99)}},
		Header: &replication.EventHeader{LogPos: 50},
	}))
	require.Empty(t, entries)
	require.NoError(t, h.OnRow(&canal.RowsEvent{
		Table:  &schema.Table{Schema: "test", Name: "users"},
		Action: canal.UpdateAction, Rows: [][]any{{int64(1), "old"}, {int64(1), "new"}},
		Header: &replication.EventHeader{LogPos: 100, EventSize: 20},
	}))
	require.NoError(t, h.OnXID(&replication.EventHeader{EventSize: 15},
		gomysql.Position{Name: "binlog.000001", Pos: 115}))

	row := <-entries
	require.Equal(t, UpdateDML, row.DmlEvent.DML)
	require.Equal(t, []any{int64(1), "old"}, row.DmlEvent.WhereColumnValues.AbstractValues())
	require.Equal(t, []any{int64(1), "new"}, row.DmlEvent.NewColumnValues.AbstractValues())
	require.Equal(t, int64(100), row.Coordinates.(*mysql.FileBinlogCoordinates).LogPos)
	commit := <-entries
	require.True(t, commit.TransactionComplete)
	require.Equal(t, int64(115), commit.Coordinates.(*mysql.FileBinlogCoordinates).LogPos)
	require.Equal(t, int64(115), h.LastTransactionCoordinates().(*mysql.FileBinlogCoordinates).LogPos)

	require.NoError(t, h.OnRotate(nil, &replication.RotateEvent{NextLogName: []byte("binlog.000002"), Position: 4}))
	require.Equal(t, "binlog.000002", h.GetCurrentBinlogCoordinates().(*mysql.FileBinlogCoordinates).LogFile)
	require.NoError(t, h.OnXID(&replication.EventHeader{}, gomysql.Position{Name: "binlog.000002", Pos: 8}))
	require.Empty(t, entries)
}

func TestCanalEventHandlerGTIDCommit(t *testing.T) {
	ctx := base.NewMigrationContext()
	defer ctx.CancelContext()
	ctx.UseGTIDs = true
	ctx.DatabaseName = "test"
	ctx.OriginalTableName = "users"
	start, err := mysql.NewGTIDBinlogCoordinates(mysql.MySQLFlavor, "00000000-0000-0000-0000-000000000000:1")
	require.NoError(t, err)
	entries := make(chan *BinlogEntry, 2)
	h := NewCanalEventHandler(ctx, entries, start)
	require.NoError(t, h.OnGTID(nil, &replication.GTIDEvent{SID: make([]byte, 16), GNO: 2}))
	require.NoError(t, h.OnRow(&canal.RowsEvent{
		Table:  &schema.Table{Schema: "test", Name: "users"},
		Action: canal.InsertAction, Rows: [][]any{{int64(1)}},
		Header: &replication.EventHeader{LogPos: 100},
	}))
	require.NoError(t, h.OnXID(&replication.EventHeader{}, gomysql.Position{}))
	require.Equal(t, "00000000-0000-0000-0000-000000000000:1-2", (<-entries).Coordinates.DisplayString())
	require.Equal(t, "00000000-0000-0000-0000-000000000000:1-2", (<-entries).Coordinates.DisplayString())
	require.Equal(t, "00000000-0000-0000-0000-000000000000:1-2", h.LastTransactionCoordinates().DisplayString())
	// DDL has a GTID but no XID callback; the next transaction keeps it.
	require.NoError(t, h.OnGTID(nil, &replication.GTIDEvent{SID: make([]byte, 16), GNO: 3}))
	require.NoError(t, h.OnGTID(nil, &replication.GTIDEvent{SID: make([]byte, 16), GNO: 4}))
	require.NoError(t, h.OnXID(&replication.EventHeader{}, gomysql.Position{}))
	require.Equal(t, "00000000-0000-0000-0000-000000000000:1-4", h.LastTransactionCoordinates().DisplayString())
	require.Empty(t, entries)
}

func TestCanalEventHandlerRejectsIncompleteUpdate(t *testing.T) {
	ctx := base.NewMigrationContext()
	defer ctx.CancelContext()
	ctx.DatabaseName = "test"
	ctx.OriginalTableName = "users"
	h := NewCanalEventHandler(ctx, make(chan *BinlogEntry, 1), mysql.NewFileBinlogCoordinates("binlog.000001", 4))
	require.ErrorContains(t, h.OnRow(&canal.RowsEvent{
		Table:  &schema.Table{Schema: "test", Name: "users"},
		Action: canal.UpdateAction, Rows: [][]any{{int64(1)}},
		Header: &replication.EventHeader{LogPos: 12},
	}), "odd number")
}

type canalMetricsSpy struct {
	metrics.Emitter
	histograms map[string][]float64
	gauges     map[string][]float64
}

func (s *canalMetricsSpy) Histogram(name string, value float64, _ ...string) {
	s.histograms[name] = append(s.histograms[name], value)
}

func (s *canalMetricsSpy) Gauge(name string, value float64, _ ...string) {
	s.gauges[name] = append(s.gauges[name], value)
}

func TestCanalEventHandlerGTIDMetrics(t *testing.T) {
	ctx := base.NewMigrationContext()
	defer ctx.CancelContext()
	ctx.UseGTIDs = true
	spy := &canalMetricsSpy{Emitter: metrics.Noop, histograms: make(map[string][]float64), gauges: make(map[string][]float64)}
	ctx.Metrics = spy
	h := NewCanalEventHandler(ctx, make(chan *BinlogEntry, 1), &mysql.GTIDBinlogCoordinates{})
	h.transactionRowEventTotal, h.transactionRowTotal, h.previousLastCommitted = 2, 3, 4
	gtid := &replication.GTIDEvent{SID: make([]byte, 16), GNO: 1, TransactionLength: 128, LastCommitted: 5, SequenceNumber: 8}
	require.NoError(t, h.OnGTID(nil, gtid))
	require.NoError(t, h.OnGTID(nil, gtid))
	require.Equal(t, []float64{2}, spy.histograms["transaction_num_row_events"])
	require.Equal(t, []float64{3}, spy.histograms["transaction_num_rows"])
	require.Equal(t, []float64{128, 128}, spy.histograms["gtid_event_transaction_length_bytes"])
	require.Equal(t, []float64{3}, spy.gauges["unfiltered_commit_group_size"])
}

func TestCanalEventHandlerStopsBlockedDelivery(t *testing.T) {
	ctx := base.NewMigrationContext()
	ctx.DatabaseName = "test"
	ctx.OriginalTableName = "users"
	h := NewCanalEventHandler(ctx, make(chan *BinlogEntry), mysql.NewFileBinlogCoordinates("binlog.000001", 4))
	ctx.CancelContext()
	require.ErrorIs(t, h.OnRow(&canal.RowsEvent{
		Table:  &schema.Table{Schema: "test", Name: "users"},
		Action: canal.InsertAction, Rows: [][]any{{int64(1)}},
		Header: &replication.EventHeader{LogPos: 12},
	}), context.Canceled)
}
