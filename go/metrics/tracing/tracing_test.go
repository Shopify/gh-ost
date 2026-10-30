/*
   Copyright 2026 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package tracing

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestNewProviderEmptyEndpoint(t *testing.T) {
	provider, err := NewProvider(context.Background(), "")
	require.NoError(t, err)
	require.Nil(t, provider)
}

func TestAttributesFromTags(t *testing.T) {
	attrs, err := AttributesFromTags([]string{"env:prod", "team:db,region:us"})
	require.NoError(t, err)
	require.Equal(t, []attribute.KeyValue{
		attribute.String("env", "prod"),
		attribute.String("team", "db"),
		attribute.String("region", "us"),
	}, attrs)
}

func TestAttributesFromTagsInvalid(t *testing.T) {
	_, err := AttributesFromTags([]string{"notag"})
	require.Error(t, err)
}

func TestStartWithErrorNilSafe(t *testing.T) {
	ctx, end := StartWithError(nil, nil, SpanApplyBatch, DMLEventAttributes("db", "tbl", "insert")...)
	require.NotNil(t, ctx)
	end(nil)
}

func TestStartWithErrorRecordsError(t *testing.T) {
	exporter := &capturingExporter{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	ctx, end := StartWithError(context.Background(), tp, SpanExecuteSQL)
	end(errors.New("boom"))
	require.NotNil(t, ctx)

	spans := exporter.get()
	require.Len(t, spans, 1)
	require.Equal(t, codes.Error, spans[0].Status().Code)
	require.Equal(t, attribute.StringValue("error"), attrValue(spans[0], "dml.outcome"))
}

func TestDMLEventAttributes(t *testing.T) {
	attrs := DMLEventAttributes("mydb", "orders", "update")
	require.Len(t, attrs, 3)
}

func TestDMLTypeCounts(t *testing.T) {
	attrs := DMLTypeCounts(2, 3, 1)
	require.Equal(t, []attribute.KeyValue{
		attribute.Int("dml.insert_count", 2),
		attribute.Int("dml.update_count", 3),
		attribute.Int("dml.delete_count", 1),
	}, attrs)
}

func TestSpanApplyBatchName(t *testing.T) {
	require.Equal(t, "Apply Batch", SpanApplyBatch)
}

type capturingExporter struct {
	mu    sync.Mutex
	spans []sdktrace.ReadOnlySpan
}

func (e *capturingExporter) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.spans = append(e.spans, spans...)
	return nil
}

func (e *capturingExporter) Shutdown(context.Context) error { return nil }

func (e *capturingExporter) get() []sdktrace.ReadOnlySpan {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]sdktrace.ReadOnlySpan, len(e.spans))
	copy(out, e.spans)
	return out
}

func attrValue(span sdktrace.ReadOnlySpan, key string) attribute.Value {
	for _, attr := range span.Attributes() {
		if string(attr.Key) == key {
			return attr.Value
		}
	}
	return attribute.Value{}
}

func TestStartEndedPreservesHistoricalTimestamps(t *testing.T) {
	exporter := &capturingExporter{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	parentStart := time.Now().Add(-50 * time.Millisecond).UTC().Truncate(time.Microsecond)
	parentCtx, endParent := StartWithErrorOptions(
		context.Background(),
		tp,
		SpanApplyBatch,
		trace.WithTimestamp(parentStart),
	)

	applyStart := parentStart
	applyEnd := parentStart.Add(10 * time.Millisecond)
	_, endExec, ok := StartEndedContext(parentCtx, tp, SpanExecuteSQL, applyStart, applyEnd, nil)
	require.True(t, ok)
	endExec()
	endParent(nil)

	spans := exporter.get()
	byName := map[string][]sdktrace.ReadOnlySpan{}
	for _, span := range spans {
		byName[span.Name()] = append(byName[span.Name()], span)
	}
	require.Contains(t, byName, SpanApplyBatch)
	require.Contains(t, byName, SpanExecuteSQL)
	require.Equal(t, byName[SpanApplyBatch][0].SpanContext().SpanID(), byName[SpanExecuteSQL][0].Parent().SpanID())
	require.Equal(t, applyStart, byName[SpanExecuteSQL][0].StartTime().UTC().Truncate(time.Microsecond))
	require.Equal(t, applyEnd, byName[SpanExecuteSQL][0].EndTime().UTC().Truncate(time.Microsecond))
}

func TestMeasureAndEmitTreeSiblings(t *testing.T) {
	exporter := &capturingExporter{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	parentStart := time.Now().Add(-50 * time.Millisecond).UTC().Truncate(time.Microsecond)
	parentCtx, endParent := StartWithErrorOptions(
		context.Background(),
		tp,
		SpanApplyBatch,
		trace.WithTimestamp(parentStart),
	)

	conn, err := Measure(SpanGetConnection, func() error {
		time.Sleep(2 * time.Millisecond)
		return nil
	})
	require.NoError(t, err)
	session, err := Measure(SpanSetSession, func() error {
		time.Sleep(2 * time.Millisecond)
		return nil
	})
	require.NoError(t, err)

	txStart := time.Now()
	build, err := Measure(SpanBuildQueries, func() error {
		time.Sleep(2 * time.Millisecond)
		return nil
	})
	require.NoError(t, err)
	exec, err := Measure(SpanExecuteBatch, func() error {
		time.Sleep(3 * time.Millisecond)
		return nil
	})
	require.NoError(t, err)
	commit, err := Measure(SpanCommitTx, func() error {
		time.Sleep(2 * time.Millisecond)
		return nil
	})
	require.NoError(t, err)
	txEnd := time.Now()

	applyEnd := time.Now()
	EmitTree(parentCtx, tp, SpanExecuteSQL, parentStart.Add(5*time.Millisecond), applyEnd, nil, []PhaseNode{
		{Phase: conn},
		{Phase: session},
		{
			Phase: Phase{Name: SpanBeginTx, Start: txStart, End: txEnd},
			Children: []PhaseNode{
				{Phase: build},
				{Phase: exec},
				{Phase: commit},
			},
		},
	}, nil)
	endParent(nil)

	spans := exporter.get()
	byName := map[string][]sdktrace.ReadOnlySpan{}
	for _, span := range spans {
		byName[span.Name()] = append(byName[span.Name()], span)
	}
	execSQL := byName[SpanExecuteSQL][0]
	beginTx := byName[SpanBeginTx][0]
	require.Equal(t, execSQL.SpanContext().SpanID(), byName[SpanGetConnection][0].Parent().SpanID())
	require.Equal(t, execSQL.SpanContext().SpanID(), byName[SpanSetSession][0].Parent().SpanID())
	require.Equal(t, execSQL.SpanContext().SpanID(), beginTx.Parent().SpanID())
	require.Equal(t, beginTx.SpanContext().SpanID(), byName[SpanBuildQueries][0].Parent().SpanID())
	require.Equal(t, beginTx.SpanContext().SpanID(), byName[SpanExecuteBatch][0].Parent().SpanID())
	require.Equal(t, beginTx.SpanContext().SpanID(), byName[SpanCommitTx][0].Parent().SpanID())
	require.Equal(t, attribute.StringValue("success"), attrValue(execSQL, "dml.outcome"))
}

func TestEmitTreeRecordsPhaseErrors(t *testing.T) {
	exporter := &capturingExporter{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	parentCtx, endParent := StartWithError(context.Background(), tp, SpanApplyBatch)
	defer endParent(errors.New("apply failed"))

	boom := errors.New("conn failed")
	phase, err := Measure(SpanGetConnection, func() error { return boom })
	require.ErrorIs(t, err, boom)

	start := time.Now().Add(-10 * time.Millisecond)
	end := time.Now()
	EmitTree(parentCtx, tp, SpanExecuteSQL, start, end, nil, []PhaseNode{{Phase: phase}}, boom)

	spans := exporter.get()
	byName := map[string]sdktrace.ReadOnlySpan{}
	for _, span := range spans {
		byName[span.Name()] = span
	}
	require.Equal(t, codes.Error, byName[SpanExecuteSQL].Status().Code)
	require.Equal(t, codes.Error, byName[SpanGetConnection].Status().Code)
	require.Equal(t, attribute.StringValue("error"), attrValue(byName[SpanExecuteSQL], "dml.outcome"))
}

func TestStartEndedIgnoresInvalidRanges(t *testing.T) {
	exporter := &capturingExporter{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	parentCtx, endParent := StartWithError(context.Background(), tp, SpanApplyBatch)
	defer endParent(nil)

	start := time.Now().Add(-10 * time.Millisecond)
	end := start.Add(5 * time.Millisecond)
	StartEnded(parentCtx, tp, SpanExecuteSQL, end, start)
	StartEnded(parentCtx, tp, SpanExecuteSQL, time.Time{}, time.Now())
	require.Empty(t, exporter.get())
}
