package binlog

import (
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/mysql"
	"github.com/go-mysql-org/go-mysql/canal"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
)

var errCanalStop = errors.New("canal reached stop coordinates")

// CanalReader streams row changes through Canal callbacks.
type CanalReader struct {
	migrationContext *base.MigrationContext
	start            mysql.BinlogCoordinates
	canal            *canal.Canal
	Handler          *CanalEventHandler
}

func NewCanalReader(ctx *base.MigrationContext, start mysql.BinlogCoordinates, entries chan<- *BinlogEntry) (*CanalReader, error) {
	if start == nil || start.IsEmpty() {
		return nil, fmt.Errorf("cannot start canal without binlog coordinates")
	}
	connection := ctx.InspectorConnectionConfig
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", connection.Key.Hostname, connection.Key.Port)
	cfg.User = connection.User
	cfg.Password = connection.Password
	cfg.TLSConfig = connection.TLSConfig()
	cfg.ServerID = uint32(ctx.ReplicaServerId)
	cfg.Flavor = mysql.FlavorFor(ctx.InspectorMySQLVersion)
	cfg.UseDecimal = true
	cfg.TimestampStringLocation = time.UTC
	cfg.MaxReconnectAttempts = ctx.BinlogSyncerMaxReconnectAttempts
	cfg.Dump.ExecutionPath = ""
	cfg.IncludeTableRegex = []string{
		"(?i)^" + regexp.QuoteMeta(ctx.DatabaseName) + "\\.(" +
			regexp.QuoteMeta(ctx.OriginalTableName) + "|" + regexp.QuoteMeta(ctx.GetChangelogTableName()) + ")$",
	}
	c, err := canal.NewCanal(cfg)
	if err != nil {
		return nil, fmt.Errorf("create canal: %w", err)
	}
	handler := NewCanalEventHandler(ctx, entries, start)
	c.SetEventHandler(handler)
	return &CanalReader{migrationContext: ctx, start: start.Clone(), canal: c, Handler: handler}, nil
}

var _ BinlogReader = (*CanalReader)(nil)

func (r *CanalReader) GetCurrentBinlogCoordinates() mysql.BinlogCoordinates {
	return r.Handler.GetCurrentBinlogCoordinates()
}

func (r *CanalReader) LastTransactionCoordinates() mysql.BinlogCoordinates {
	return r.Handler.LastTransactionCoordinates()
}

// EnableDeliveryAck waits for the EventsStreamer listener after each row.
func (r *CanalReader) EnableDeliveryAck() { r.Handler.waitForDelivery = true }

// StreamEvents runs Canal until migration cancellation or canStopStreaming is true.
func (r *CanalReader) StreamEvents(canStopStreaming func() bool) error {
	r.Handler.shouldStop = canStopStreaming
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-r.migrationContext.GetContext().Done():
				r.canal.Close()
				return
			case <-ticker.C:
				if canStopStreaming() {
					r.canal.Close()
					return
				}
			}
		}
	}()
	var err error
	switch start := r.start.(type) {
	case *mysql.FileBinlogCoordinates:
		err = r.canal.RunFrom(gomysql.Position{Name: start.LogFile, Pos: uint32(start.LogPos)})
	case *mysql.GTIDBinlogCoordinates:
		err = r.canal.StartFromGTID(start.GTIDSet.Clone())
	default:
		return fmt.Errorf("unsupported canal start coordinates: %T", r.start)
	}
	if errors.Is(err, errCanalStop) || canStopStreaming() {
		return nil
	}
	return err
}

func (r *CanalReader) Close() { r.canal.Close() }
