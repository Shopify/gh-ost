/*
   Copyright 2026 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package tracing

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/openark/golib/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

const instrumentationName = "github.com/github/gh-ost"

// ServiceName is the OpenTelemetry service.name resource attribute.
const ServiceName = "gh-ost"

// Batch export tuning. Defaults drop spans when the queue is full; we block
// instead so TraceQL reporting stays complete under apply load.
const (
	bspMaxQueueSize       = 65536
	bspMaxExportBatchSize = 512
	bspBatchTimeout       = 500 * time.Millisecond
	bspExportTimeout      = 30 * time.Second
)

// NoopProvider is used when tracing is disabled.
var NoopProvider = noop.NewTracerProvider()

// Provider wraps an OTLP trace exporter and SDK tracer provider.
type Provider struct {
	tp *sdktrace.TracerProvider
}

// NewProvider configures OTLP/HTTP export to endpoint. Returns nil, nil when endpoint is empty.
// tags are global key:value pairs (from --otel-attributes) attached as resource
// attributes on every exported span.
func NewProvider(ctx context.Context, endpoint string, tags ...string) (*Provider, error) {
	if endpoint == "" {
		return nil, nil
	}

	exporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpointURL(endpoint),
	)
	if err != nil {
		return nil, fmt.Errorf("tracing: create OTLP exporter: %w", err)
	}

	resAttrs := []attribute.KeyValue{
		semconv.ServiceName(ServiceName),
	}
	tagAttrs, err := AttributesFromTags(tags)
	if err != nil {
		return nil, fmt.Errorf("tracing: %w", err)
	}
	resAttrs = append(resAttrs, tagAttrs...)

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(semconv.SchemaURL, resAttrs...),
	)
	if err != nil {
		return nil, fmt.Errorf("tracing: create resource: %w", err)
	}

	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		log.Errorf("tracing: OpenTelemetry SDK error: %v", err)
	}))

	tp := sdktrace.NewTracerProvider(
		// Batch export off the apply goroutine. WithBlocking avoids silent
		// span drops when the collector/Tempo cannot keep up; apply may stall
		// briefly instead of under-reporting TraceQL metrics.
		sdktrace.WithBatcher(exporter,
			sdktrace.WithBlocking(),
			sdktrace.WithMaxQueueSize(bspMaxQueueSize),
			sdktrace.WithMaxExportBatchSize(bspMaxExportBatchSize),
			sdktrace.WithBatchTimeout(bspBatchTimeout),
			sdktrace.WithExportTimeout(bspExportTimeout),
		),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return &Provider{tp: tp}, nil
}

// AttributesFromTags parses key:value attributes into OTEL attributes.
// Each entry may be a single key:value or a comma-separated list of them.
func AttributesFromTags(tags []string) ([]attribute.KeyValue, error) {
	attrs := make([]attribute.KeyValue, 0, len(tags))
	for _, raw := range tags {
		for _, part := range strings.Split(raw, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			key, value, ok := strings.Cut(part, ":")
			key = strings.TrimSpace(key)
			value = strings.TrimSpace(value)
			if !ok || key == "" {
				return nil, fmt.Errorf("invalid tag %q (want key:value)", part)
			}
			attrs = append(attrs, attribute.String(key, value))
		}
	}
	return attrs, nil
}

// TracerProvider returns the configured provider or NoopProvider.
func (p *Provider) TracerProvider() trace.TracerProvider {
	if p == nil || p.tp == nil {
		return NoopProvider
	}
	return p.tp
}

// Shutdown flushes and stops the exporter.
func (p *Provider) Shutdown(ctx context.Context) error {
	if p == nil || p.tp == nil {
		return nil
	}
	return p.tp.Shutdown(ctx)
}

// Tracer returns a tracer for gh-ost instrumentation.
func Tracer(tp trace.TracerProvider) trace.Tracer {
	if tp == nil {
		tp = NoopProvider
	}
	return tp.Tracer(instrumentationName)
}

// ShutdownTimeout is the default grace period for flushing spans on exit.
const ShutdownTimeout = 30 * time.Second
