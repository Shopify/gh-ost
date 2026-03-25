package binlog

import (
	"fmt"
	"io"
	"os"
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

const benchNumUUIDs = 182

// benchServerSID is a fixed synthetic UUID used as the "active" server in benchmarks.
var benchServerSID = guuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")

// buildSyntheticGTIDSet generates a realistic-sized GTID set with numUUIDs unique
// server UUIDs, each with a transaction range of 1 to a varying upper bound.
func buildSyntheticGTIDSet(numUUIDs int) *gomysql.MysqlGTIDSet {
	set := new(gomysql.MysqlGTIDSet)
	set.Sets = make(map[string]*gomysql.UUIDSet)
	for i := 0; i < numUUIDs; i++ {
		// Deterministic UUIDs seeded from index
		sid := guuid.MustParse(fmt.Sprintf("%08x-0000-4000-8000-%012x", i, i))
		gno := int64(1_000_000 + i*100_000)
		uuidSet := gomysql.NewUUIDSet(sid, gomysql.Interval{Start: 1, Stop: gno + 1})
		set.Sets[sid.String()] = uuidSet
	}
	// Include the benchmark server UUID so the initial set size stays consistent
	// throughout the benchmark (no extra map entry created on first AddSet).
	set.Sets[benchServerSID.String()] = gomysql.NewUUIDSet(benchServerSID, gomysql.Interval{Start: 1, Stop: 2})
	return set
}

func buildGTIDEvents(initialSet *gomysql.MysqlGTIDSet) []*replication.BinlogEvent {
	events := make([]*replication.BinlogEvent, 0, benchTxCount*(benchRowsPerTx+2))
	accSet := initialSet.Clone().(*gomysql.MysqlGTIDSet)
	sid := benchServerSID
	sidBytes, _ := sid.MarshalBinary()

	for i := 0; i < benchTxCount; i++ {
		gno := int64(i + 1)

		events = append(events, &replication.BinlogEvent{
			Header: &replication.EventHeader{EventType: replication.GTID_EVENT},
			Event:  &replication.GTIDEvent{SID: sidBytes, GNO: gno},
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
		if useGTIDs {
			reader.lastCommittedCoords = initialCoords.(*mysql.GTIDBinlogCoordinates)
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
	initialSet := buildSyntheticGTIDSet(benchNumUUIDs)
	events := buildGTIDEvents(initialSet)
	initialCoords := &mysql.GTIDBinlogCoordinates{GTIDSet: initialSet}
	feedAndRun(b, fmt.Sprintf("GTID (%d UUIDs)", benchNumUUIDs), true, events, initialCoords)
}

func BenchmarkStreamingFile(b *testing.B) {
	events := buildFileEvents()
	initialCoords := &mysql.FileBinlogCoordinates{LogFile: "mysql-bin.000001", LogPos: 0}
	feedAndRun(b, "File", false, events, initialCoords)
}
