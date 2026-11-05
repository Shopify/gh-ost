package logic

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/github/gh-ost/go/base"
)

// Override only the synchronization points needed by the regression tests.
// All other hooks retain the normal executor's behavior.
type listenerOrderHooks struct {
	base.Hooks
	validated       func() error
	rowCopyComplete func() error
}

func (h *listenerOrderHooks) OnValidated() error {
	if h.validated != nil {
		return h.validated()
	}
	return h.Hooks.OnValidated()
}

func (h *listenerOrderHooks) OnRowCopyComplete() error {
	if h.rowCopyComplete != nil {
		return h.rowCopyComplete()
	}
	return h.Hooks.OnRowCopyComplete()
}

func (suite *MigratorTestSuite) TestResumeAppliesDMLWrittenWhileStopped() {
	suite.testCheckpointListenerOrder(false, true)
}

func (suite *MigratorTestSuite) TestResumeAppliesDMLWrittenWhileStoppedFileCoordinates() {
	suite.testCheckpointListenerOrder(false, false)
}

func (suite *MigratorTestSuite) TestRevertAppliesDMLWrittenAfterCutOver() {
	suite.testCheckpointListenerOrder(true, true)
}

func (suite *MigratorTestSuite) TestRevertAppliesDMLWrittenAfterCutOverFileCoordinates() {
	suite.testCheckpointListenerOrder(true, false)
}

func (suite *MigratorTestSuite) testCheckpointListenerOrder(revert, useGTIDs bool) {
	ctx := context.Background()
	_, err := suite.db.ExecContext(ctx, "CREATE TABLE "+getTestTableName()+" (id INT PRIMARY KEY, v INT NOT NULL)")
	suite.Require().NoError(err)
	checkpointTable := fmt.Sprintf("`%s`.`_%s_ghk`", testMysqlDatabase, testMysqlTableName)
	defer func() {
		_, err := suite.db.ExecContext(ctx, "DROP TABLE IF EXISTS "+checkpointTable)
		suite.Require().NoError(err)
	}()

	// A single UPDATE transaction will exceed the production apply queue's
	// capacity even before its completion marker is enqueued.
	const numRows = 3 * base.MaxEventsBatchSize
	values := make([]string, numRows)
	for i := range numRows {
		values[i] = fmt.Sprintf("(%d, 0)", i)
	}
	_, err = suite.db.ExecContext(ctx, "INSERT INTO "+getTestTableName()+" VALUES "+strings.Join(values, ","))
	suite.Require().NoError(err)

	connectionConfig, err := getTestConnectionConfig(ctx, suite.mysqlContainer)
	suite.Require().NoError(err)
	// t.TempDir includes the long test name and can exceed Unix socket path limits.
	socketDir, err := os.MkdirTemp("", "listener-")
	suite.Require().NoError(err)
	defer os.RemoveAll(socketDir)
	newContext := func() *base.MigrationContext {
		migrationContext := newTestMigrationContext()
		migrationContext.ApplierConnectionConfig = connectionConfig
		migrationContext.InspectorConnectionConfig = connectionConfig
		migrationContext.SetConnectionConfig("innodb")
		migrationContext.AlterStatement = "ADD INDEX idx_v (v)"
		migrationContext.Checkpoint = true
		// The resume test explicitly checkpoints after copying, without waiting
		// for a timer or racing the periodic checkpoint loop.
		migrationContext.CheckpointIntervalSeconds = 3600
		migrationContext.UseGTIDs = useGTIDs
		migrationContext.ServeSocketFile = filepath.Join(socketDir, "gh-ost.sock")
		return migrationContext
	}

	firstContext := newContext()
	first := NewMigrator(firstContext, "test")
	stopped := errors.New("stop after checkpoint before cutover")
	first.hooksExecutor = &listenerOrderHooks{
		Hooks: first.hooksExecutor,
		validated: func() error {
			// Fresh migrations must not register DML/commit listeners early:
			// they need GhostTableMigrated before the apply queue is drained.
			first.eventsStreamer.listenersMutex.Lock()
			defer first.eventsStreamer.listenersMutex.Unlock()
			suite.Require().Len(first.eventsStreamer.listeners, 1)
			suite.Require().Empty(first.eventsStreamer.transactionListeners)
			return nil
		},
		rowCopyComplete: func() error {
			if revert {
				return nil // Revert needs the normal post-cutover checkpoint.
			}
			checkpointCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			if _, err := first.Checkpoint(checkpointCtx); err != nil {
				return err
			}
			return stopped
		},
	}
	err = first.Migrate()
	// An interrupted migration does not call finalCleanup/Close. Stop its
	// parser explicitly so it cannot interfere with the next stream's ID.
	suite.Require().NoError(first.eventsStreamer.Close())
	if revert {
		suite.Require().NoError(err)
	} else {
		suite.Require().ErrorIs(err, stopped)
	}
	suite.assertSingleDMLListener(first)

	// These writes are committed before the resume/revert streamer exists.
	// Every updated/deleted row has already been copied to the target table.
	_, err = suite.db.ExecContext(ctx, "UPDATE "+getTestTableName()+" SET v = id + 10000")
	suite.Require().NoError(err)
	_, err = suite.db.ExecContext(ctx, "DELETE FROM "+getTestTableName()+" WHERE id = 0")
	suite.Require().NoError(err)
	_, err = suite.db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s VALUES (%d, 100000)", getTestTableName(), numRows))
	suite.Require().NoError(err)

	secondContext := newContext()
	secondContext.Resume = !revert
	secondContext.Revert = revert
	if revert {
		secondContext.AlterStatement = ""
		secondContext.OldTableName = firstContext.GetOldTableName()
	}
	// Capture a boundary after all pending writes, using the same coordinate
	// flavor as the streamer. This helper only reads status; it never connects
	// a binlog parser or owns/closes the suite's DB connection.
	statusReader := NewEventsStreamer(secondContext)
	statusReader.db = suite.db
	suite.Require().NoError(statusReader.readCurrentBinlogCoordinates())
	backlogEnd := statusReader.initialBinlogCoordinates.Clone()

	second := NewMigrator(secondContext, "test")
	queueFilled := false
	second.hooksExecutor = &listenerOrderHooks{
		Hooks: second.hooksExecutor,
		validated: func() error {
			// Hold startup exactly in the old listener-registration gap. With
			// the fix, backpressure fills the queue. Without it, the reader
			// passes the entire backlog without decoding its rows. Waiting on
			// either observable state avoids relying on a fixed hook sleep.
			timer := time.NewTimer(30 * time.Second)
			defer timer.Stop()
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for {
				queueFilled = len(second.applyEventsQueue) == cap(second.applyEventsQueue)
				if queueFilled || backlogEnd.SmallerThan(second.eventsStreamer.GetCurrentBinlogCoordinates()) {
					second.applier.CurrentCoordinatesMutex.Lock()
					defer second.applier.CurrentCoordinatesMutex.Unlock()
					suite.Require().Nil(second.applier.AppliedTransactionCoordinates,
						"buffering rows/markers before applier startup must not acknowledge them")
					return nil
				}
				select {
				case <-timer.C:
					return errors.New("streamer neither filled the apply queue nor passed the backlog")
				case <-ticker.C:
				}
			}
		},
	}
	if revert {
		err = second.Revert()
	} else {
		err = second.Migrate()
	}
	suite.Require().NoError(err)
	suite.assertSingleDMLListener(second)

	// Assert the entire row set, not just an inner join that could miss lost
	// inserts/deletes. The retained source table must have the same contents.
	expected := make(map[int]int, numRows)
	for i := 1; i < numRows; i++ {
		expected[i] = i + 10000
	}
	expected[numRows] = 100000
	retainedTable := getTestOldTableName()
	if revert {
		retainedTable = getTestRevertedTableName()
	}
	for _, table := range []string{getTestTableName(), retainedTable} {
		rows, err := suite.db.QueryContext(ctx, "SELECT id, v FROM "+table)
		suite.Require().NoError(err)
		actual := make(map[int]int)
		for rows.Next() {
			var id, value int
			suite.Require().NoError(rows.Scan(&id, &value))
			actual[id] = value
		}
		suite.Require().NoError(rows.Err())
		suite.Require().NoError(rows.Close())
		suite.Require().Equal(expected, actual, "incorrect rows in %s", table)
	}
	suite.Require().True(queueFilled, "the backlog must exercise startup with a full apply queue")
	suite.Require().Equal(int64(numRows+2), atomic.LoadInt64(&secondContext.TotalDMLEventsApplied),
		"each pending row change must be delivered exactly once")
	second.applier.CurrentCoordinatesMutex.Lock()
	defer second.applier.CurrentCoordinatesMutex.Unlock()
	suite.Require().NotNil(second.applier.AppliedTransactionCoordinates)
	suite.Require().True(backlogEnd.SmallerThanOrEquals(second.applier.AppliedTransactionCoordinates))
}

func (suite *MigratorTestSuite) assertSingleDMLListener(migrator *Migrator) {
	migrator.eventsStreamer.listenersMutex.Lock()
	defer migrator.eventsStreamer.listenersMutex.Unlock()
	suite.Require().Len(migrator.eventsStreamer.listeners, 2, "one changelog and one original-table listener")
	suite.Require().Len(migrator.eventsStreamer.transactionListeners, 1, "one completion marker per transaction")
}
