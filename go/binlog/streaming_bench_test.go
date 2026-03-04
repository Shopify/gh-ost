package binlog

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/mysql"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	guuid "github.com/google/uuid"
)

const (
	benchTxCount   = 1_000
	benchRowsPerTx = 5
)

// 02b9e2cf-9c8a-11e7-a479-42010ae7009b — one of the real servers in the set
var benchServerSID = []byte{
	0x02, 0xb9, 0xe2, 0xcf, 0x9c, 0x8a, 0x11, 0xe7,
	0xa4, 0x79, 0x42, 0x01, 0x0a, 0xe7, 0x00, 0x9b,
}

func loadProductionGTIDSet(tb testing.TB) *gomysql.MysqlGTIDSet {
	data, err := os.ReadFile("../../gtid_executed_shard21")
	if err != nil {
		tb.Fatalf("could not load gtid_executed_shard21: %v", err)
	}
	cleaned := strings.Join(strings.Fields(string(data)), ",")
	set, err := gomysql.ParseMysqlGTIDSet(cleaned)
	if err != nil {
		tb.Fatalf("could not parse GTID set: %v", err)
	}
	return set.(*gomysql.MysqlGTIDSet)
}

func buildGTIDEvents(initialSet *gomysql.MysqlGTIDSet) []*replication.BinlogEvent {
	events := make([]*replication.BinlogEvent, 0, benchTxCount*(benchRowsPerTx+2))
	accSet := initialSet.Clone().(*gomysql.MysqlGTIDSet)
	sid, _ := guuid.FromBytes(benchServerSID)

	for i := 0; i < benchTxCount; i++ {
		gno := int64(73_590_714 + i)

		events = append(events, &replication.BinlogEvent{
			Header: &replication.EventHeader{EventType: replication.GTID_EVENT},
			Event:  &replication.GTIDEvent{SID: benchServerSID, GNO: gno},
		})

		for r := 0; r < benchRowsPerTx; r++ {
			events = append(events, &replication.BinlogEvent{
				Header: &replication.EventHeader{
					EventType: replication.WRITE_ROWS_EVENTv2,
					LogPos:    uint32(i*1000 + r + 1),
					EventSize: 100,
				},
				Event: &replication.RowsEvent{
					Table: &replication.TableMapEvent{
						Schema: []byte("mydb"),
						Table:  []byte("orders"),
					},
					Rows: [][]interface{}{{int64(i), "value"}},
				},
			})
		}

		trxGset := gomysql.NewUUIDSet(sid, gomysql.Interval{Start: gno, Stop: gno + 1})
		accSet.AddSet(trxGset)

		events = append(events, &replication.BinlogEvent{
			Header: &replication.EventHeader{EventType: replication.XID_EVENT},
			Event:  &replication.XIDEvent{GSet: accSet.Clone()},
		})
	}
	return events
}

func buildFileEvents() []*replication.BinlogEvent {
	events := make([]*replication.BinlogEvent, 0, benchTxCount*(benchRowsPerTx+1))

	for i := 0; i < benchTxCount; i++ {
		for r := 0; r < benchRowsPerTx; r++ {
			events = append(events, &replication.BinlogEvent{
				Header: &replication.EventHeader{
					EventType: replication.WRITE_ROWS_EVENTv2,
					LogPos:    uint32(i*1000 + r + 1),
					EventSize: 100,
				},
				Event: &replication.RowsEvent{
					Table: &replication.TableMapEvent{
						Schema: []byte("mydb"),
						Table:  []byte("orders"),
					},
					Rows: [][]interface{}{{int64(i), "value"}},
				},
			})
		}

		events = append(events, &replication.BinlogEvent{
			Header: &replication.EventHeader{
				EventType: replication.XID_EVENT,
				LogPos:    uint32(i*1000 + benchRowsPerTx + 1),
			},
			Event: &replication.XIDEvent{},
		})
	}
	return events
}

// feedAndRun feeds events into a fresh streamer concurrently with StreamEvents.
// This avoids b.N scaling issues caused by heavy pre-fill setup dominating over
// the (very fast) file-mode processing time.
func feedAndRun(b *testing.B, label string, useGTIDs bool, events []*replication.BinlogEvent, initialCoords mysql.BinlogCoordinates) {
	b.ReportAllocs()

	var iterations atomic.Int64
	done := make(chan struct{})

	go func() {
		spinner := []string{"|", "/", "-", "\\"}
		tick := time.NewTicker(500 * time.Millisecond)
		defer tick.Stop()
		frame := 0
		for {
			select {
			case <-done:
				fmt.Fprintf(os.Stderr, "\r%-30s done (%d iters)          \n", label, iterations.Load())
				return
			case <-tick.C:
				fmt.Fprintf(os.Stderr, "\r%-30s %s  iter %d", label, spinner[frame%4], iterations.Load())
				frame++
			}
		}
	}()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Small channel — events flow through as StreamEvents consumes them.
		s := replication.NewBinlogStreamer()

		ctx := &base.MigrationContext{}
		ctx.UseGTIDs = useGTIDs
		reader := &GoMySQLReader{
			migrationContext:        ctx,
			currentCoordinatesMutex: &sync.Mutex{},
			currentCoordinates:      initialCoords.Clone(),
			binlogStreamer:          s,
		}
		entriesCh := make(chan *BinlogEntry, 100)

		// Feed events concurrently so AddEventToStreamer never blocks.
		var feedDone sync.WaitGroup
		feedDone.Add(1)
		go func() {
			defer feedDone.Done()
			for _, ev := range events {
				s.AddEventToStreamer(ev)
			}
			s.AddErrorToStreamer(io.EOF)
		}()

		// Drain entries so StreamEvents never blocks writing to entriesCh.
		var drainDone sync.WaitGroup
		drainDone.Add(1)
		go func() {
			defer drainDone.Done()
			for range entriesCh {
			}
		}()

		reader.StreamEvents(func() bool { return false }, entriesCh)
		feedDone.Wait()
		close(entriesCh)
		drainDone.Wait()

		iterations.Add(1)
	}

	close(done)
}

func BenchmarkStreamingGTID(b *testing.B) {
	initialSet := loadProductionGTIDSet(b)
	events := buildGTIDEvents(initialSet)
	initialCoords := &mysql.GTIDBinlogCoordinates{GTIDSet: initialSet}
	feedAndRun(b, "GTID (182 UUIDs)", true, events, initialCoords)
}

func BenchmarkStreamingFile(b *testing.B) {
	events := buildFileEvents()
	initialCoords := &mysql.FileBinlogCoordinates{LogFile: "mysql-bin.000001", LogPos: 0}
	feedAndRun(b, "File", false, events, initialCoords)
}
