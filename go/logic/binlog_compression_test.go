package logic

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/github/gh-ost/go/binlog"
	"github.com/github/gh-ost/go/mysql"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	testmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
)

func TestBinlogTransactionCompressionIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	container, err := testmysql.Run(ctx, testMysqlContainerImage,
		testmysql.WithDatabase(testMysqlDatabase),
		testmysql.WithUsername(testMysqlUser),
		testmysql.WithPassword(testMysqlPass),
		testcontainers.WithCmdArgs("--server-id=1", "--gtid-mode=ON", "--enforce-gtid-consistency=ON"),
	)
	require.NoError(t, err)
	defer testcontainers.TerminateContainer(container)
	dsn, err := container.ConnectionString(ctx)
	require.NoError(t, err)
	db, err := gosql.Open("mysql", dsn)
	require.NoError(t, err)
	defer db.Close()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	for _, query := range []string{
		"SET SESSION binlog_transaction_compression=ON",
		"SET SESSION time_zone='+00:00'",
		"CREATE TABLE testing (id INT PRIMARY KEY, amount DECIMAL(5,2), created_at TIMESTAMP, padding TEXT)",
	} {
		_, err := conn.ExecContext(ctx, query)
		require.NoError(t, err)
	}
	status := func() (*mysql.FileBinlogCoordinates, *mysql.GTIDBinlogCoordinates) {
		var file, doDB, ignoreDB, gtid string
		var pos int64
		require.NoError(t, conn.QueryRowContext(ctx, "SHOW MASTER STATUS").Scan(&file, &pos, &doDB, &ignoreDB, &gtid))
		coords, err := mysql.NewGTIDBinlogCoordinates(mysql.MySQLFlavor, gtid)
		require.NoError(t, err)
		return mysql.NewFileBinlogCoordinates(file, pos), coords
	}
	startFile, startGTID := status()
	tx, err := conn.BeginTx(ctx, nil)
	require.NoError(t, err)
	for _, query := range []string{
		"INSERT INTO testing VALUES (1, 123.45, '2023-11-14 22:13:20', REPEAT('x', 10000))",
		"UPDATE testing SET amount=456.78 WHERE id=1",
		"DELETE FROM testing WHERE id=1",
	} {
		_, err := tx.ExecContext(ctx, query)
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())
	endFile, endGTID := status()

	// Check that MySQL actually wrote a compressed event, not just that the
	// session setting was accepted (a small transaction might not compress).
	rows, err := conn.QueryContext(ctx, fmt.Sprintf("SHOW BINLOG EVENTS IN '%s' FROM %d", startFile.LogFile, startFile.LogPos))
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	compressed := false
	for rows.Next() {
		var file, eventType, info string
		var pos, serverID, endPos uint64
		require.NoError(t, rows.Scan(&file, &pos, &eventType, &serverID, &endPos, &info))
		compressed = compressed || eventType == "Transaction_payload"
	}
	require.NoError(t, rows.Err())
	require.True(t, compressed, "server must produce a Transaction_payload event")

	config, err := getTestConnectionConfig(ctx, container)
	require.NoError(t, err)
	for _, useGTIDs := range []bool{false, true} {
		t.Run(fmt.Sprintf("GTID=%t", useGTIDs), func(t *testing.T) {
			migrationContext := newTestMigrationContext()
			migrationContext.UseGTIDs = useGTIDs
			migrationContext.InspectorConnectionConfig = config
			reader := binlog.NewGoMySQLReader(migrationContext, func(database, table string) bool {
				return database == testMysqlDatabase && table == testMysqlTableName
			})
			defer reader.Close()
			start, end := startFile.Clone(), endFile.Clone()
			if useGTIDs {
				start, end = startGTID.Clone(), endGTID.Clone()
			}
			require.NoError(t, reader.ConnectBinlogStreamer(start))
			entries := make(chan *binlog.BinlogEntry, 16)
			done := make(chan error, 1)
			//nolint:contextcheck // StreamEvents has no context parameter; timeout handling below closes the reader.
			go func() {
				done <- reader.StreamEvents(func() bool {
					return reader.LastTrxCoords != nil && reader.LastTrxCoords.Equals(end)
				}, entries)
			}()
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-ctx.Done():
				reader.Close()
				<-done
				t.Fatal("timed out reading compressed transaction")
			}
			require.True(t, reader.LastTrxCoords.Equals(end))
			require.Len(t, entries, 4)
			insert, update, deleted, commit := <-entries, <-entries, <-entries, <-entries
			require.True(t, commit.TransactionComplete)
			require.True(t, commit.Coordinates.Equals(end))
			require.Equal(t, binlog.InsertDML, insert.DmlEvent.DML)
			require.Equal(t, binlog.UpdateDML, update.DmlEvent.DML)
			require.Equal(t, binlog.DeleteDML, deleted.DmlEvent.DML)
			require.Equal(t, decimal.RequireFromString("123.45"), insert.DmlEvent.NewColumnValues.AbstractValues()[1])
			require.Equal(t, "2023-11-14 22:13:20", insert.DmlEvent.NewColumnValues.AbstractValues()[2])
			require.Equal(t, insert.DmlEvent.NewColumnValues, update.DmlEvent.WhereColumnValues)
			require.Equal(t, decimal.RequireFromString("456.78"), update.DmlEvent.NewColumnValues.AbstractValues()[1])
			require.Equal(t, update.DmlEvent.NewColumnValues, deleted.DmlEvent.WhereColumnValues)
			for _, entry := range []*binlog.BinlogEntry{insert, update, deleted} {
				require.True(t, entry.Coordinates.Equals(end))
			}
		})
	}
}
