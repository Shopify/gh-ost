package logic

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/binlog"
	"github.com/github/gh-ost/go/mysql"
	"github.com/stretchr/testify/require"
)

func newThrottledBookkeepingMigrator(t *testing.T) *Migrator {
	t.Helper()
	ctx := newTestMigrationContext()
	t.Cleanup(ctx.CancelContext)
	ctx.SetThrottled(true, "test", base.NoThrottleReasonHint)
	mgtr := NewMigrator(ctx, "test")
	mgtr.applier = NewApplier(ctx)
	mgtr.throttler = NewThrottler(ctx, mgtr.applier, nil, "test")
	return mgtr
}

func bookkeepingEvent(pos int64, heartbeat bool) *applyEventStruct {
	return &applyEventStruct{
		coords:    mysql.NewFileBinlogCoordinates("mysql-bin.000001", pos),
		heartbeat: heartbeat, transactionComplete: !heartbeat,
	}
}

func waitForBookkeepingCoordinates(t *testing.T, mgtr *Migrator, heartbeat, transaction int64) {
	t.Helper()
	require.Eventually(t, func() bool {
		mgtr.applier.CurrentCoordinatesMutex.Lock()
		defer mgtr.applier.CurrentCoordinatesMutex.Unlock()
		return mgtr.applier.CurrentCoordinates != nil && mgtr.applier.AppliedTransactionCoordinates != nil &&
			mgtr.applier.CurrentCoordinates.Equals(bookkeepingEvent(heartbeat, true).coords) &&
			mgtr.applier.AppliedTransactionCoordinates.Equals(bookkeepingEvent(transaction, false).coords)
	}, 3*time.Second, 10*time.Millisecond)
}

func waitForBookkeepingWorker(t *testing.T, done <-chan error, expected error) {
	t.Helper()
	select {
	case err := <-done:
		if expected != nil {
			require.ErrorIs(t, err, expected)
		} else {
			require.NoError(t, err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not exit")
	}
}

func TestThrottledWorkersDrainBookkeeping(t *testing.T) {
	for _, dmlOnly := range []bool{false, true} {
		name := "rowcopy-and-dml"
		if dmlOnly {
			name = "dml-only"
		}
		t.Run(name, func(t *testing.T) {
			mgtr := newThrottledBookkeepingMigrator(t)
			// A ready row-copy task must not run just because bookkeeping can.
			mgtr.copyRowsQueue = make(chan tableWriteFunc, 1)
			var copies atomic.Int64
			mgtr.copyRowsQueue <- func() error { copies.Add(1); return nil }
			done := make(chan error, 1)
			go func() {
				if dmlOnly {
					done <- mgtr.executeDMLWriteFuncs()
				} else {
					done <- mgtr.executeWriteFuncs()
				}
			}()

			mgtr.applyEventsQueue <- bookkeepingEvent(100, true)
			mgtr.applyEventsQueue <- bookkeepingEvent(110, false)
			waitForBookkeepingCoordinates(t, mgtr, 100, 110)
			// Entries arriving after the worker is already waiting also drain.
			mgtr.applyEventsQueue <- bookkeepingEvent(200, true)
			mgtr.applyEventsQueue <- bookkeepingEvent(210, false)
			waitForBookkeepingCoordinates(t, mgtr, 200, 210)
			require.Empty(t, mgtr.applyEventsQueue)
			require.Zero(t, copies.Load())
			require.Zero(t, atomic.LoadInt64(&mgtr.migrationContext.TotalDMLEventsApplied))
			throttled, _, _ := mgtr.migrationContext.IsThrottled()
			require.True(t, throttled)
			mgtr.migrationContext.CancelContext()
			waitForBookkeepingWorker(t, done, context.Canceled)
		})
	}
}

func TestThrottledBookkeepingStopsAtOrderingBarrier(t *testing.T) {
	for _, dml := range []bool{false, true} {
		for _, cancel := range []bool{false, true} {
			name := "callback"
			if dml {
				name = "dml"
			}
			if cancel {
				name += "-cancel"
			}
			t.Run(name, func(t *testing.T) {
				mgtr := newThrottledBookkeepingMigrator(t)
				var calls atomic.Int64
				var callback tableWriteFunc = func() error { calls.Add(1); return nil }
				barrier := newApplyEventStructByFunc(&callback)
				if dml {
					barrier = &applyEventStruct{dmlEvent: &binlog.BinlogDMLEvent{}}
				}
				mgtr.applyEventsQueue <- bookkeepingEvent(100, true)
				mgtr.applyEventsQueue <- bookkeepingEvent(110, false)
				mgtr.applyEventsQueue <- barrier
				mgtr.applyEventsQueue <- bookkeepingEvent(200, true)
				mgtr.applyEventsQueue <- bookkeepingEvent(210, false)
				done := make(chan error, 1)
				var pending *applyEventStruct
				go func() {
					var err error
					pending, err = mgtr.throttleApplyEvents()
					done <- err
				}()
				require.Eventually(t, func() bool {
					return mgtr.pendingThrottledEvent.Load()
				}, 3*time.Second, 10*time.Millisecond)
				// Allow multiple throttle ticks: later bookkeeping must stay queued.
				time.Sleep(550 * time.Millisecond)
				waitForBookkeepingCoordinates(t, mgtr, 100, 110)
				require.Len(t, mgtr.applyEventsQueue, 2)
				require.Equal(t, 3, mgtr.migrationProgressSnapshot().applyEventsBacklog)
				require.Zero(t, calls.Load())
				if cancel {
					mgtr.migrationContext.CancelContext()
					waitForBookkeepingWorker(t, done, context.Canceled)
					require.Nil(t, pending)
				} else {
					mgtr.migrationContext.SetThrottled(false, "", base.NoThrottleReasonHint)
					waitForBookkeepingWorker(t, done, nil)
					require.Same(t, barrier, pending)
				}
			})
		}
	}
}

func TestThrottledWorkersResumeBarrierBeforeLaterBookkeeping(t *testing.T) {
	for _, dmlOnly := range []bool{false, true} {
		t.Run(map[bool]string{false: "rowcopy-and-dml", true: "dml-only"}[dmlOnly], func(t *testing.T) {
			mgtr := newThrottledBookkeepingMigrator(t)
			var calls atomic.Int64
			var barrier tableWriteFunc = func() error {
				calls.Add(1)
				mgtr.applier.CurrentCoordinatesMutex.Lock()
				defer mgtr.applier.CurrentCoordinatesMutex.Unlock()
				if mgtr.applier.AppliedTransactionCoordinates != nil {
					return context.Canceled // Later marker must not have run yet.
				}
				return nil
			}
			var finish tableWriteFunc = func() error {
				atomic.StoreInt64(&mgtr.finishedMigrating, 1)
				return nil
			}
			mgtr.applyEventsQueue <- newApplyEventStructByFunc(&barrier)
			mgtr.applyEventsQueue <- bookkeepingEvent(210, false)
			mgtr.applyEventsQueue <- newApplyEventStructByFunc(&finish)
			done := make(chan error, 1)
			go func() {
				if dmlOnly {
					done <- mgtr.executeDMLWriteFuncs()
				} else {
					done <- mgtr.executeWriteFuncs()
				}
			}()
			require.Eventually(t, func() bool {
				return mgtr.pendingThrottledEvent.Load()
			}, 3*time.Second, 10*time.Millisecond)
			require.Zero(t, calls.Load())
			mgtr.migrationContext.SetThrottled(false, "", base.NoThrottleReasonHint)
			waitForBookkeepingWorker(t, done, nil)
			require.EqualValues(t, 1, calls.Load())
			require.True(t, mgtr.applier.AppliedTransactionCoordinates.Equals(bookkeepingEvent(210, false).coords))
			require.Empty(t, mgtr.applyEventsQueue)
		})
	}
}
