// Package observability initialises the three observability pillars —
// structured logging (zerolog), distributed tracing (OpenTelemetry), and
// application metrics (Prometheus) — as defined in ADR-007.
//
// All components are constructed with injected dependencies (no global state
// beyond the OTel TracerProvider, which the SDK requires to be global for
// instrumentation libraries to pick it up automatically).
package observability

import (
	"context"
	"io"
	"os"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Version is injected at build time:
//
//	go build -ldflags "-X github.com/DarioJejer/go-email-queue/internal/observability.Version=$(git describe --tags --always)"
//
// It is embedded in every log line and the OTel service.version resource
// attribute so that alerts and traces can be correlated to a specific release.
var Version = "dev"

// loggerKey is an unexported type used as the context key for the
// zerolog.Logger. Using a named struct (not a raw string) ensures that a lookup
// with the key type loggerKey{} cannot collide with keys added by other
// packages, even if they chose the same string constant (ADR-007).
type loggerKey struct{}

// NewLogger creates a zerolog.Logger configured with the given level and
// format. It attaches service and version as permanent default fields so
// every log line satisfies the minimum schema required by ADR-007.
//
// level must be one of "debug", "info" (default), "warn", "error".
// format must be "json" (default, production) or "console" (colored, local dev).
func NewLogger(level, format string) zerolog.Logger {
	var lvl zerolog.Level
	switch level {
	case "debug":
		lvl = zerolog.DebugLevel
	case "warn":
		lvl = zerolog.WarnLevel
	case "error":
		lvl = zerolog.ErrorLevel
	default:
		lvl = zerolog.InfoLevel
	}

	var w io.Writer = os.Stdout
	if format == "console" {
		w = zerolog.NewConsoleWriter()
	}

	return zerolog.New(w).
		Level(lvl).
		With().
		Timestamp().
		Str("service", "email-queue").
		Str("version", Version).
		Logger()
}

// WithLogger stores logger in ctx and returns the new context.
// Downstream handlers and adapters retrieve it via LoggerFromContext to avoid
// threading a *zerolog.Logger through every function signature.
func WithLogger(ctx context.Context, logger zerolog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, logger)
}

// LoggerFromContext retrieves the zerolog.Logger stored in ctx by WithLogger.
// If no logger was stored, it returns zerolog's global logger (log.Logger) so
// callers never need to nil-check the return value.
func LoggerFromContext(ctx context.Context) zerolog.Logger {
	if l, ok := ctx.Value(loggerKey{}).(zerolog.Logger); ok {
		return l
	}
	return log.Logger
}

// WithTaskFields returns a derived logger enriched with the four standard
// task-level fields required by ADR-007: task_id, tenant_id, task_type,
// attempt. Call this at the start of processTask so every log line emitted
// during task execution carries the full task context automatically.
func WithTaskFields(logger zerolog.Logger, taskID, tenantID, taskType string, attempt int) zerolog.Logger {
	return logger.With().
		Str("task_id", taskID).
		Str("tenant_id", tenantID).
		Str("task_type", taskType).
		Int("attempt", attempt).
		Logger()
}
