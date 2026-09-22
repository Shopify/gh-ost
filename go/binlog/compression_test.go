package binlog

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sync"
	"testing"
	"time"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/mysql"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

// Initialize using a real format description; all other events below are built
// on the wire so these tests exercise the bundled parser as well as the reader.
func compressionTestParser(t *testing.T, filter RowsEventFilterFunc) *replication.BinlogParser {
	t.Helper()
	parser := replication.NewBinlogParser()
	parser.SetUseDecimal(true)
	parser.SetTimestampStringLocation(time.UTC)
	parser.SetRowsEventDecodeFunc(newRowsEventDecodeFunc(filter))
	parser.SetVerifyChecksum(true)
	require.NoError(t, parser.ParseFile("testdata/mysql-bin.000066", 4, func(ev *replication.BinlogEvent) error {
		format, ok := ev.Event.(*replication.FormatDescriptionEvent)
		require.True(t, ok)
		require.Equal(t, replication.BINLOG_CHECKSUM_ALG_CRC32, format.ChecksumAlgorithm)
		parser.Stop()
		return nil
	}))
	parser.Resume()
	return parser
}

func compressionTestWire(kind replication.EventType, body []byte, position uint32, checksum bool) []byte {
	size := replication.EventHeaderSize + len(body)
	if checksum {
		size += 4
	}
	wire := make([]byte, replication.EventHeaderSize, size)
	wire[4] = byte(kind)
	binary.LittleEndian.PutUint32(wire[5:9], 1)
	binary.LittleEndian.PutUint32(wire[9:13], uint32(size))
	binary.LittleEndian.PutUint32(wire[13:17], position)
	wire = append(wire, body...)
	if checksum {
		wire = binary.LittleEndian.AppendUint32(wire, crc32.ChecksumIEEE(wire))
	}
	return wire
}

func compressionTestPayload(t *testing.T, parser *replication.BinlogParser, events [][]byte, position uint32) *replication.BinlogEvent {
	t.Helper()
	var raw []byte
	for _, event := range events {
		raw = append(raw, event...)
	}
	encoder, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	compressed := encoder.EncodeAll(raw, nil)
	encoder.Close()
	var body []byte
	field := func(kind byte, value uint64) {
		body = append(body, kind, 8)
		body = binary.LittleEndian.AppendUint64(body, value)
	}
	field(replication.OTW_PAYLOAD_SIZE_FIELD, uint64(len(compressed)))
	field(replication.OTW_PAYLOAD_COMPRESSION_TYPE_FIELD, replication.ZSTD)
	field(replication.OTW_PAYLOAD_UNCOMPRESSED_SIZE_FIELD, uint64(len(raw)))
	body = append(body, replication.OTW_PAYLOAD_HEADER_END_MARK)
	body = append(body, compressed...)
	event, err := parser.Parse(compressionTestWire(replication.TRANSACTION_PAYLOAD_EVENT, body, position, true))
	require.NoError(t, err)
	require.IsType(t, &replication.TransactionPayloadEvent{}, event.Event)
	return event
}

// A transaction on testing(id INT, amount DECIMAL(5,2), created_at TIMESTAMP(0)):
// insert (1, 123.45, ...), update it to (2, 456.78, ...), then delete it.
func compressionTestTransaction(table string) [][]byte {
	tableMap := []byte{7, 0, 0, 0, 0, 0, 0, 0} // table ID and flags
	tableMap = append(tableMap, 6)
	tableMap = append(tableMap, "testdb"...)
	tableMap = append(tableMap, 0, byte(len(table)))
	tableMap = append(tableMap, table...)
	tableMap = append(tableMap, 0, 3, gomysql.MYSQL_TYPE_LONG, gomysql.MYSQL_TYPE_NEWDECIMAL, gomysql.MYSQL_TYPE_TIMESTAMP2)
	tableMap = append(tableMap, 3, 5, 2, 0, 0) // metadata length, decimal precision/scale, timestamp precision, null bitmap
	row := func(id uint32, amount []byte) []byte {
		data := binary.LittleEndian.AppendUint32([]byte{0}, id) // null bitmap + id
		data = append(data, amount...)
		return binary.BigEndian.AppendUint32(data, 1700000000)
	}
	before := row(1, []byte{0x80, 0x7b, 0x2d})
	after := row(2, []byte{0x81, 0xc8, 0x4e})
	var events [][]byte
	for _, kind := range []replication.EventType{replication.WRITE_ROWS_EVENTv2, replication.UPDATE_ROWS_EVENTv2, replication.DELETE_ROWS_EVENTv2} {
		events = append(events, compressionTestWire(replication.TABLE_MAP_EVENT, tableMap, 0, false))
		body := []byte{7, 0, 0, 0, 0, 0, 1, 0, 2, 0, 3, 7} // table ID, stmt-end flag, extra length, column count/bitmap
		switch kind {
		case replication.WRITE_ROWS_EVENTv2:
			body = append(body, before...)
		case replication.UPDATE_ROWS_EVENTv2:
			body = append(body, 7) // after-image bitmap
			body = append(body, before...)
			body = append(body, after...)
		case replication.DELETE_ROWS_EVENTv2:
			body = append(body, after...)
		}
		events = append(events, compressionTestWire(kind, body, 0, false))
	}
	return append(events, compressionTestWire(replication.XID_EVENT, binary.LittleEndian.AppendUint64(nil, 42), 0, false))
}

func compressionTestReader(useGTIDs bool) *GoMySQLReader {
	ctx := base.NewMigrationContext()
	ctx.UseGTIDs = useGTIDs
	ctx.DatabaseName = "testdb"
	ctx.OriginalTableName = "testing"
	var coords mysql.BinlogCoordinates = &mysql.FileBinlogCoordinates{LogFile: "mysql-bin.000001", LogPos: 4}
	if useGTIDs {
		coords = &mysql.GTIDBinlogCoordinates{}
	}
	return &GoMySQLReader{
		migrationContext:        ctx,
		binlogStreamer:          replication.NewBinlogStreamer(),
		currentCoordinates:      coords,
		currentCoordinatesMutex: &sync.Mutex{},
	}
}

func compressionTestStream(t *testing.T, reader *GoMySQLReader, events ...*replication.BinlogEvent) []*BinlogEntry {
	t.Helper()
	for _, event := range events {
		require.NoError(t, reader.binlogStreamer.AddEventToStreamer(event))
	}
	entries := make(chan *BinlogEntry, 32)
	iterations := 0
	require.NoError(t, reader.StreamEvents(func() bool {
		iterations++
		return iterations > len(events)
	}, entries))
	close(entries)
	var result []*BinlogEntry
	for entry := range entries {
		result = append(result, entry)
	}
	return result
}

func TestCompressedTransaction(t *testing.T) {
	const sid = "24bc7850-2c16-11e6-a073-0242ac110002"
	gtid := func(gno int64) *replication.BinlogEvent {
		id := uuid.MustParse(sid)
		return &replication.BinlogEvent{
			Header: &replication.EventHeader{EventType: replication.GTID_EVENT},
			Event:  &replication.GTIDEvent{SID: id[:], GNO: gno},
		}
	}
	for _, useGTIDs := range []bool{false, true} {
		t.Run(fmt.Sprintf("GTID=%t", useGTIDs), func(t *testing.T) {
			parser := compressionTestParser(t, nil)
			payload := compressionTestPayload(t, parser, compressionTestTransaction("testing"), 4096)
			reader := compressionTestReader(useGTIDs)
			if useGTIDs {
				compressionTestStream(t, reader, gtid(1))
			}
			entries := compressionTestStream(t, reader, payload)
			require.Len(t, entries, 3)
			require.Equal(t, InsertDML, entries[0].DmlEvent.DML)
			require.Equal(t, UpdateDML, entries[1].DmlEvent.DML)
			require.Equal(t, DeleteDML, entries[2].DmlEvent.DML)
			before := []interface{}{int32(1), decimal.RequireFromString("123.45"), "2023-11-14 22:13:20"}
			after := []interface{}{int32(2), decimal.RequireFromString("456.78"), "2023-11-14 22:13:20"}
			require.Equal(t, before, entries[0].DmlEvent.NewColumnValues.AbstractValues())
			require.Equal(t, before, entries[1].DmlEvent.WhereColumnValues.AbstractValues())
			require.Equal(t, after, entries[1].DmlEvent.NewColumnValues.AbstractValues())
			require.Equal(t, after, entries[2].DmlEvent.WhereColumnValues.AbstractValues())
			var expected mysql.BinlogCoordinates = &mysql.FileBinlogCoordinates{
				LogFile: "mysql-bin.000001", LogPos: 4096, EventSize: int64(payload.Header.EventSize),
			}
			if useGTIDs {
				var err error
				expected, err = mysql.NewGTIDBinlogCoordinates(mysql.MySQLFlavor, sid+":1")
				require.NoError(t, err)
			}
			require.Equal(t, expected, reader.LastTrxCoords)
			for _, entry := range entries {
				require.Equal(t, expected, entry.Coordinates)
			}
			if useGTIDs {
				// A subsequent GTID must not mutate the committed checkpoint or
				// coordinates attached to already-emitted rows.
				compressionTestStream(t, reader, gtid(2))
				require.Equal(t, expected, reader.LastTrxCoords)
				require.Equal(t, expected, entries[0].Coordinates)
				require.Equal(t, sid+":1-2", reader.GetCurrentBinlogCoordinates().String())
			}
			require.Zero(t, reader.transactionRowEventTotal)
			require.Zero(t, reader.transactionRowTotal)

			// Interleaving an ordinary transaction still works, including the
			// syncer's normal XID GSet handling.
			var ordinary []*replication.BinlogEvent
			for i, wire := range compressionTestTransaction("testing") {
				ev, err := parser.Parse(compressionTestWire(replication.EventType(wire[4]), wire[replication.EventHeaderSize:], uint32(8192+i*100), true))
				require.NoError(t, err)
				if xid, ok := ev.Event.(*replication.XIDEvent); ok && useGTIDs {
					xid.GSet = reader.GetCurrentBinlogCoordinates().(*mysql.GTIDBinlogCoordinates).GTIDSet
				}
				ordinary = append(ordinary, ev)
			}
			plain := compressionTestStream(t, reader, ordinary...)
			require.Len(t, plain, len(entries))
			for i := range plain {
				require.Equal(t, entries[i].DmlEvent, plain[i].DmlEvent)
			}
			require.Equal(t, reader.GetCurrentBinlogCoordinates(), reader.LastTrxCoords)
		})
	}
}

func TestCompressedTransactionRowFilter(t *testing.T) {
	var tables []string
	parser := compressionTestParser(t, func(database, table string) bool {
		require.Equal(t, "testdb", database)
		tables = append(tables, table)
		return table == "testing"
	})
	// Both tables belong to one transaction, with a single final XID.
	ignored := compressionTestTransaction("ignored")
	events := append(ignored[:len(ignored)-1], compressionTestTransaction("testing")...)
	payload := compressionTestPayload(t, parser, events, 4096)
	require.Equal(t, []string{"ignored", "ignored", "ignored", "testing", "testing", "testing"}, tables)
	reader := compressionTestReader(false)
	entries := compressionTestStream(t, reader, payload)
	require.Len(t, entries, 3)
	for _, entry := range entries {
		require.Equal(t, "testing", entry.DmlEvent.TableName)
	}
	require.NotNil(t, reader.LastTrxCoords)
}

func TestCompressedTransactionRowErrorDoesNotCommit(t *testing.T) {
	reader := compressionTestReader(false)
	reader.LastTrxCoords = reader.GetCurrentBinlogCoordinates()
	checkpoint := reader.LastTrxCoords.Clone()
	payload := compressionTestPayload(t, compressionTestParser(t, nil), compressionTestTransaction("testing"), 4096)
	// Force a row-processing error after decoding, before the embedded commit.
	payload.Event.(*replication.TransactionPayloadEvent).Events[1].Header.EventType = replication.UNKNOWN_EVENT
	require.NoError(t, reader.binlogStreamer.AddEventToStreamer(payload))
	err := reader.StreamEvents(func() bool { return false }, make(chan *BinlogEntry, 32))
	require.ErrorContains(t, err, "unknown DML type")
	require.Equal(t, checkpoint, reader.LastTrxCoords)
}
