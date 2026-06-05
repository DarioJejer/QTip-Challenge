package observability

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/DarioJejer/go-email-queue/internal/config"
)

// InitTracer initialises the OpenTelemetry TracerProvider.
//
// If cfg.Observability.OTELEndpoint is empty a noop TracerProvider is installed
// and no network connection is attempted — safe for local development and all
// unit tests. Set OTEL_EXPORTER_OTLP_ENDPOINT to enable real tracing.
//
// When an endpoint is provided, an OTLP gRPC exporter is created.
// The URL scheme controls transport security:
//
//	http://collector:4317  — plaintext gRPC (local collector / sidecar)
//	https://collector:4317 — TLS gRPC     (production)
//
// W3C TraceContext and Baggage propagators are always registered regardless of
// whether real tracing is enabled. This ensures trace IDs written into task
// payloads by the producer can be extracted by workers on the consumer side
// (ADR-007).
//
// The returned shutdown function must be called (typically deferred in main)
// to flush pending spans before the process exits. It is safe to call on the
// noop provider.
func InitTracer(ctx context.Context, cfg *config.Config) (shutdown func(context.Context) error, err error) {
	// Register W3C propagators unconditionally so that helpers like
	// otel.GetTextMapPropagator().Inject/Extract work in all environments.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	if cfg.Observability.OTELEndpoint == "" {
		// Noop mode: spans are discarded without any I/O. Zero overhead.
		otel.SetTracerProvider(noop.NewTracerProvider())
		return func(_ context.Context) error { return nil }, nil
	}

	// OTLP gRPC exporter.
	// WithEndpointURL accepts a full URL so the scheme controls TLS:
	//   http://  → plaintext   https:// → TLS
	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpointURL(cfg.Observability.OTELEndpoint),
	)
	if err != nil {
		return nil, fmt.Errorf("observability: create OTLP gRPC exporter: %w", err)
	}

	res := resource.NewSchemaless(
		attribute.String("service.name", cfg.Observability.ServiceName),
		attribute.String("service.version", Version),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)

	return func(ctx context.Context) error {
		if err := tp.Shutdown(ctx); err != nil {
			return fmt.Errorf("observability: tracer shutdown: %w", err)
		}
		return nil
	}, nil
}
