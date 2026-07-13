/*
   Copyright 2026 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package tracing

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
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

// NoopProvider is used when tracing is disabled.
var NoopProvider = noop.NewTracerProvider()

// Provider wraps an OTLP trace exporter and SDK tracer provider.
type Provider struct {
	tp *sdktrace.TracerProvider
}

// NewProvider configures OTLP/HTTP export to endpoint. Returns nil, nil when endpoint is empty.
func NewProvider(ctx context.Context, endpoint string) (*Provider, error) {
	if endpoint == "" {
		return nil, nil
	}

	exporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpointURL(endpoint),
	)
	if err != nil {
		return nil, fmt.Errorf("tracing: create OTLP exporter: %w", err)
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(ServiceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("tracing: create resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		// SimpleSpanProcessor exports each span on End(), avoiding Tempo
		// "root span not yet received" orphans from async batch ordering.
		sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(exporter)),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return &Provider{tp: tp}, nil
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
const ShutdownTimeout = 5 * time.Second
