package binlog

import (
	"testing"

	"github.com/github/gh-ost/go/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/require"
)

func TestMultiRowEventDoesNotCompleteTransaction(t *testing.T) {
	reader := compressionTestReader(false)
	rows := &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.WRITE_ROWS_EVENTv2, LogPos: 200, EventSize: 100},
		Event: &replication.RowsEvent{
			Table: &replication.TableMapEvent{Schema: []byte("testdb"), Table: []byte("testing")},
			Rows:  [][]interface{}{{1}, {2}, {3}},
		},
	}
	// Stop before XID. All rows share the event end position, but none of
	// them may independently acknowledge that position for checkpointing.
	entries := compressionTestStream(t, reader, rows)
	require.Len(t, entries, 3)
	for _, entry := range entries {
		require.Equal(t, int64(200), entry.Coordinates.(*mysql.FileBinlogCoordinates).LogPos)
		require.False(t, entry.TransactionComplete)
	}
	require.Nil(t, reader.LastTrxCoords)

	commit := &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.XID_EVENT, LogPos: 220, EventSize: 20},
		Event:  &replication.XIDEvent{},
	}
	entries = compressionTestStream(t, reader, commit)
	require.Len(t, entries, 1)
	require.True(t, entries[0].TransactionComplete)
	require.Nil(t, entries[0].DmlEvent)
	require.Equal(t, int64(220), entries[0].Coordinates.(*mysql.FileBinlogCoordinates).LogPos)
	require.Equal(t, reader.LastTrxCoords, entries[0].Coordinates)
}
