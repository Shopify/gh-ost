package binlog

import (
	"encoding/binary"
	"testing"

	"github.com/github/gh-ost/go/metrics"
	"github.com/github/gh-ost/go/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/require"
)

type binlogMetricCount struct {
	name  string
	value int64
	tags  []string
}

type binlogMetricsSpy struct {
	metrics.Emitter
	counts     []binlogMetricCount
	histograms map[string][]float64
}

func (s *binlogMetricsSpy) Count(name string, value int64, tags ...string) {
	s.counts = append(s.counts, binlogMetricCount{name, value, append([]string(nil), tags...)})
}

func (s *binlogMetricsSpy) Histogram(name string, value float64, tags ...string) {
	s.histograms[name] = append(s.histograms[name], value)
}

func TestStreamRowsEventMetrics(t *testing.T) {
	for _, eventType := range []replication.EventType{
		replication.WRITE_ROWS_EVENTv1,
		replication.UPDATE_ROWS_EVENTv1,
		replication.DELETE_ROWS_EVENTv1,
	} {
		t.Run(eventType.String(), func(t *testing.T) {
			for _, tt := range []struct {
				name         string
				database     string
				table        string
				relevant     bool
				skipDecoding bool
			}{
				{name: "original", database: "testdb", table: "testing", relevant: true},
				{name: "changelog", database: "testdb", table: "_testing_ghc", relevant: true},
				{name: "case insensitive", database: "TESTDB", table: "TESTING", relevant: true},
				{name: "unrelated table", database: "testdb", table: "ignored"},
				{name: "unrelated database", database: "otherdb", table: "testing"},
				// A relevant table can also be header-filtered before its listener
				// is registered. Its event is counted, but its row count is unknown.
				{name: "relevant header only", database: "testdb", table: "testing", relevant: true, skipDecoding: true},
			} {
				t.Run(tt.name, func(t *testing.T) {
					reader := transactionTestReader(false)
					defer reader.migrationContext.CancelContext()
					emit := &binlogMetricsSpy{Emitter: metrics.Noop, histograms: make(map[string][]float64)}
					reader.migrationContext.Metrics = emit

					rows := newTestRowsEvent(tt.database, tt.table)
					dml := ToEventDML(eventType.String())
					setUnexportedField(rows, "eventType", eventType)
					setUnexportedField(rows, "needBitmap2", dml == UpdateDML)
					// A single LONG column, with two logical rows per event.
					data := []byte{7, 0, 0, 0, 0, 0, 0, 0, 1, 1}
					images := 2
					if dml == UpdateDML {
						data = append(data, 1) // after-image column bitmap
						images *= 2
					}
					decoded := tt.relevant && !tt.skipDecoding
					if decoded {
						for i := 1; i <= images; i++ {
							data = append(data, 0) // non-NULL column
							data = binary.LittleEndian.AppendUint32(data, uint32(i))
						}
					} else {
						// Truncated payload: decoding these unrelated rows would fail.
						data = append(data, 0)
					}
					decode := newRowsEventDecodeFunc(func(database, table string) bool {
						return reader.isRelevantTable(database, table) && !tt.skipDecoding
					})
					require.NoError(t, decode(rows, data))

					entries := transactionTestStream(t, reader, &replication.BinlogEvent{
						Header: &replication.EventHeader{EventType: eventType, LogPos: 200},
						Event:  rows,
					})
					wantCounts := []binlogMetricCount{
						{"binlog_events", 1, []string{"type:processed", "table:" + tt.table}},
					}
					if tt.relevant {
						wantCounts = append(wantCounts, binlogMetricCount{
							"binlog_events", 1, []string{"type:consumed", "table:" + tt.table, "binlog_event_type:" + dml.EventTypeTag()},
						})
						require.Equal(t, int64(1), reader.transactionRowEventTotal)
					} else {
						wantCounts = append(wantCounts, binlogMetricCount{
							"binlog_events", 1, []string{"type:filtered", "table:" + tt.table},
						})
						require.Zero(t, reader.transactionRowEventTotal)
					}
					if decoded {
						wantCounts = append(wantCounts, binlogMetricCount{
							"binlog_rows", 2, []string{"type:consumed", "table:" + tt.table, "binlog_event_type:" + dml.EventTypeTag()},
						})
						require.Len(t, rows.Rows, images)
						require.Len(t, entries, 2)
						for _, entry := range entries {
							require.Equal(t, dml, entry.DmlEvent.DML)
							require.Equal(t, tt.database, entry.DmlEvent.DatabaseName)
							require.Equal(t, tt.table, entry.DmlEvent.TableName)
							require.False(t, entry.TransactionComplete)
						}
						require.Equal(t, []float64{2}, emit.histograms["binlog_rows_in_event"])
						require.Equal(t, int64(2), reader.transactionRowTotal)
					} else {
						require.Empty(t, rows.Rows)
						require.Empty(t, entries, "header-only events must not forward DML")
						require.Empty(t, emit.histograms["binlog_rows_in_event"])
						require.Zero(t, reader.transactionRowTotal)
					}
					// Exact emission checks reject even zero-valued row counters.
					require.Equal(t, wantCounts, emit.counts)
					require.Equal(t, decoded, reader.transactionHasRows)

					commit := transactionTestStream(t, reader, &replication.BinlogEvent{
						Header: &replication.EventHeader{EventType: replication.XID_EVENT, LogPos: 220},
						Event:  &replication.XIDEvent{},
					})
					if decoded {
						require.Len(t, commit, 1)
						require.True(t, commit[0].TransactionComplete)
						require.Nil(t, commit[0].DmlEvent)
					} else {
						require.Empty(t, commit, "header-only events must not create a completion marker")
					}
					require.Equal(t, int64(220), reader.LastTrxCoords.(*mysql.FileBinlogCoordinates).LogPos)
				})
			}
		})
	}
}
