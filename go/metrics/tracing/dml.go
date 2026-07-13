/*
   Copyright 2026 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package tracing

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const (
	SpanApplyBatch      = "dml.apply_batch"
	SpanExecuteSQL      = "dml.execute_sql"
	SpanGetConnection   = "dml.get_connection"
	SpanSetSession      = "dml.set_session"
	SpanBeginTx         = "dml.begin_tx"
	SpanBuildQueries    = "dml.build_queries"
	SpanExecuteBatch    = "dml.execute_batch"
	SpanCommitTx        = "dml.commit_tx"
)

// DMLEventAttributes returns common attributes for a binlog DML event.
func DMLEventAttributes(database, table, dmlType string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("db.name", database),
		attribute.String("db.table", table),
		attribute.String("dml.type", dmlType),
	}
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
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}
