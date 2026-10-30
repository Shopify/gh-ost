/*
   Copyright 2026 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package tracing

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const (
	SpanApplyBatch    = "Apply Batch"
	SpanExecuteSQL    = "dml.execute_sql"
	SpanGetConnection = "dml.get_connection"
	SpanSetSession    = "dml.set_session"
	SpanBeginTx       = "dml.begin_tx"
	SpanBuildQueries  = "dml.build_queries"
	SpanExecuteBatch  = "dml.execute_batch"
	SpanCommitTx      = "dml.commit_tx"
)

// DMLEventAttributes returns common attributes for a binlog DML event / apply batch.
func DMLEventAttributes(database, table, dmlType string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("db.name", database),
		attribute.String("db.table", table),
		attribute.String("dml.type", dmlType),
	}
}

// DMLTypeCounts returns insert/update/delete counts for a batch.
func DMLTypeCounts(inserts, updates, deletes int) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.Int("dml.insert_count", inserts),
		attribute.Int("dml.update_count", updates),
		attribute.Int("dml.delete_count", deletes),
	}
}

// OutcomeAttribute returns dml.outcome=success|error.
func OutcomeAttribute(err error) attribute.KeyValue {
	if err != nil {
		return attribute.String("dml.outcome", "error")
	}
	return attribute.String("dml.outcome", "success")
}

// StartWithError ends the span and records err when non-nil.
func StartWithError(ctx context.Context, tp trace.TracerProvider, name string, attrs ...attribute.KeyValue) (context.Context, func(error)) {
	opts := []trace.SpanStartOption{trace.WithAttributes(attrs...)}
	return StartWithErrorOptions(ctx, tp, name, opts...)
}

// StartWithErrorOptions starts a span with explicit start options.
func StartWithErrorOptions(ctx context.Context, tp trace.TracerProvider, name string, opts ...trace.SpanStartOption) (context.Context, func(error)) {
	if ctx == nil {
		ctx = context.Background()
	}
	tracer := Tracer(tp)
	ctx, span := tracer.Start(ctx, name, opts...)
	return ctx, func(err error) {
		span.SetAttributes(OutcomeAttribute(err))
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}

// StartEnded emits a child span with an explicit historical start/end time.
func StartEnded(ctx context.Context, tp trace.TracerProvider, name string, start, end time.Time, attrs ...attribute.KeyValue) {
	_, endSpan, _ := StartEndedContext(ctx, tp, name, start, end, nil, attrs...)
	endSpan()
}

// StartEndedContext starts a historical span and returns its context, end func, and
// whether the span was created. ok is false when the time range is invalid; in that
// case ctx is unchanged and end is a no-op. When err is non-nil, End records it.
func StartEndedContext(
	ctx context.Context,
	tp trace.TracerProvider,
	name string,
	start, end time.Time,
	err error,
	attrs ...attribute.KeyValue,
) (context.Context, func(), bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	if start.IsZero() || end.IsZero() || end.Before(start) {
		return ctx, func() {}, false
	}
	ctx, span := Tracer(tp).Start(
		ctx,
		name,
		trace.WithTimestamp(start),
		trace.WithAttributes(attrs...),
	)
	return ctx, func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End(trace.WithTimestamp(end))
	}, true
}

// Phase is a timed named slice of work emitted later via EmitTree.
type Phase struct {
	Name  string
	Start time.Time
	End   time.Time
	Attrs []attribute.KeyValue
	Err   error
}

// PhaseNode is a Phase that may contain sibling children (not a nested chain).
type PhaseNode struct {
	Phase
	Children []PhaseNode
}

// Measure runs fn and records its wall-clock phase. Export is deferred so
// span End() (and any processor work) does not inflate spacing or parent duration.
func Measure(name string, fn func() error, attrs ...attribute.KeyValue) (Phase, error) {
	start := time.Now()
	err := fn()
	return Phase{Name: name, Start: start, End: time.Now(), Attrs: attrs, Err: err}, err
}

// EmitTree emits parentName covering parentStart..parentEnd, then each child as a
// sibling under that parent (and recursively under nested PhaseNode children).
// parentErr is recorded on the parent span; each Phase.Err is recorded on that node.
func EmitTree(
	ctx context.Context,
	tp trace.TracerProvider,
	parentName string,
	parentStart, parentEnd time.Time,
	parentAttrs []attribute.KeyValue,
	children []PhaseNode,
	parentErr error,
) {
	if parentAttrs == nil {
		parentAttrs = []attribute.KeyValue{}
	}
	parentAttrs = append(parentAttrs, OutcomeAttribute(parentErr))
	parentCtx, endParent, ok := StartEndedContext(ctx, tp, parentName, parentStart, parentEnd, parentErr, parentAttrs...)
	if !ok {
		return
	}
	endFns := emitPhaseNodes(parentCtx, tp, children)
	endFns = append(endFns, endParent)
	// End deepest children first, then parents.
	for i := len(endFns) - 1; i >= 0; i-- {
		endFns[i]()
	}
}

func emitPhaseNodes(ctx context.Context, tp trace.TracerProvider, nodes []PhaseNode) []func() {
	var endFns []func()
	for _, node := range nodes {
		nodeCtx, endNode, ok := StartEndedContext(ctx, tp, node.Name, node.Start, node.End, node.Err, node.Attrs...)
		if !ok {
			continue
		}
		childEnds := emitPhaseNodes(nodeCtx, tp, node.Children)
		endFns = append(endFns, childEnds...)
		endFns = append(endFns, endNode)
	}
	return endFns
}
