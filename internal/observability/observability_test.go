package observability_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/DarioJejer/go-email-queue/internal/config"
	"github.com/DarioJejer/go-email-queue/internal/domain"
	"github.com/DarioJejer/go-email-queue/internal/observability"
)

// ---------------------------------------------------------------------------
// Logger tests
// ---------------------------------------------------------------------------

func TestNewLogger_JSON(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	// NewLogger writes to os.Stdout by default; .Output redirects for testing.
	logger := observability.NewLogger("info", "json").Output(&buf)

	logger.Info().Str("key", "value").Msg("test message")

	require.NotEmpty(t, buf.Bytes(), "expected log output")
	var m map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &m), "output must be valid JSON")
	assert.Equal(t, "info", m["level"])
	assert.Equal(t, "test message", m["message"])
	assert.Equal(t, "value", m["key"])
}

func TestNewLogger_Console(t *testing.T) {
	t.Parallel()
	// Console mode wraps output in a zerolog.ConsoleWriter. We verify the
	// logger can be created in console mode and produces non-empty output.
	var buf bytes.Buffer
	logger := observability.NewLogger("debug", "console").Output(&buf)
	logger.Debug().Msg("console test")
	assert.NotEmpty(t, buf.Bytes(), "expected output from console logger")
}

func TestNewLogger_DefaultsToInfoLevel(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	logger := observability.NewLogger("info", "json").Output(&buf)

	// DEBUG should be suppressed at INFO level.
	logger.Debug().Msg("should be suppressed")
	require.Empty(t, buf.Bytes(), "debug message should be suppressed at info level")

	logger.Info().Msg("should appear")
	require.NotEmpty(t, buf.Bytes())
	var m map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &m))
	assert.Equal(t, "should appear", m["message"])
}

func TestWithLogger_RoundTrip(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	original := observability.NewLogger("debug", "json").Output(&buf)
	ctx := observability.WithLogger(context.Background(), original)
	retrieved := observability.LoggerFromContext(ctx)
	retrieved.Info().Msg("roundtrip")
	require.NotEmpty(t, buf.Bytes(), "retrieved logger must write to same underlying writer")
}

func TestLoggerFromContext_FallbackToGlobal(t *testing.T) {
	t.Parallel()
	// Empty context: must return the global logger without panic.
	logger := observability.LoggerFromContext(context.Background())
	// Absence of panic is the assertion; redirect to /dev/null for cleanliness.
	var buf bytes.Buffer
	logger.Output(&buf)
	logger.Info().Msg("fallback")

}

func TestWithTaskFields(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	base := observability.NewLogger("debug", "json").Output(&buf)
	child := observability.WithTaskFields(base, "task-abc", "tenant-xyz", "security", 3)
	child.Info().Msg("task fields test")

	var m map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &m))
	assert.Equal(t, "task-abc", m["task_id"])
	assert.Equal(t, "tenant-xyz", m["tenant_id"])
	assert.Equal(t, "security", m["task_type"])
	assert.Equal(t, float64(3), m["attempt"])
}

// ---------------------------------------------------------------------------
// Tracing tests
// ---------------------------------------------------------------------------

func TestInitTracer_NoopOnEmptyEndpoint(t *testing.T) {
	cfg := minimalConfig()
	cfg.Observability.OTELEndpoint = ""

	shutdown, err := observability.InitTracer(context.Background(), cfg)
	require.NoError(t, err, "InitTracer must not error with empty endpoint")
	require.NotNil(t, shutdown)
	require.NoError(t, shutdown(context.Background()), "noop shutdown must not error")
}

// ---------------------------------------------------------------------------
// Metrics tests
// ---------------------------------------------------------------------------

func TestPrometheusRecorder_RecordEnqueued(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	rec := observability.NewPrometheusRecorder(reg)

	rec.RecordEnqueued("tenant-1", "registration", "high")
	rec.RecordEnqueued("tenant-1", "registration", "high")
	rec.RecordEnqueued("tenant-2", "billing", "normal")

	assert.Equal(t, float64(2),
		testutil.ToFloat64(rec.EnqueuedTotal().WithLabelValues("tenant-1", "registration", "high")))
	assert.Equal(t, float64(1),
		testutil.ToFloat64(rec.EnqueuedTotal().WithLabelValues("tenant-2", "billing", "normal")))
}

func TestPrometheusRecorder_RecordProcessed(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	rec := observability.NewPrometheusRecorder(reg)

	rec.RecordProcessed("tenant-1", "billing", "success", 0.25)
	rec.RecordProcessed("tenant-1", "billing", "success", 0.75)
	rec.RecordProcessed("tenant-1", "billing", "failed", 1.5)

	assert.Equal(t, float64(2),
		testutil.ToFloat64(rec.ProcessedTotal().WithLabelValues("tenant-1", "billing", "success")))
	assert.Equal(t, float64(1),
		testutil.ToFloat64(rec.ProcessedTotal().WithLabelValues("tenant-1", "billing", "failed")))
}

func TestPrometheusRecorder_RecordQueueDepth(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	rec := observability.NewPrometheusRecorder(reg)

	rec.RecordQueueDepth("{queue}:email:high", 42)
	assert.Equal(t, float64(42),
		testutil.ToFloat64(rec.QueueDepthGauge().WithLabelValues("{queue}:email:high")))

	// Gauge overwrite.
	rec.RecordQueueDepth("{queue}:email:high", 10)
	assert.Equal(t, float64(10),
		testutil.ToFloat64(rec.QueueDepthGauge().WithLabelValues("{queue}:email:high")))
}

func TestPrometheusRecorder_RecordDLQDepth(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	rec := observability.NewPrometheusRecorder(reg)

	rec.RecordDLQDepth("tenant-1", "marketing", 7)
	assert.Equal(t, float64(7),
		testutil.ToFloat64(rec.DLQDepthGauge().WithLabelValues("tenant-1", "marketing")))
}

func TestPrometheusRecorder_RecordWorkerStats(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	rec := observability.NewPrometheusRecorder(reg)

	rec.RecordWorkerStats(domain.WorkerStats{ActiveWorkers: 12, PoolSize: 50})

	assert.Equal(t, float64(12), testutil.ToFloat64(rec.WorkerActiveGauge()))
	assert.Equal(t, float64(50), testutil.ToFloat64(rec.WorkerPoolSizeGauge()))
}

func TestPrometheusRecorder_AllMetricsRegistered(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	rec := observability.NewPrometheusRecorder(reg)

	// Trigger at least one observation per metric so Gather returns them all.
	rec.RecordEnqueued("t", "registration", "low")
	rec.RecordProcessed("t", "registration", "success", 0.1)
	rec.RecordQueueDepth("{queue}:email:low", 1)
	rec.RecordDLQDepth("t", "registration", 0)
	rec.RecordWorkerStats(domain.WorkerStats{ActiveWorkers: 1, PoolSize: 10})

	mfs, err := reg.Gather()
	require.NoError(t, err)

	names := make(map[string]bool, len(mfs))
	for _, mf := range mfs {
		names[mf.GetName()] = true
	}

	for _, name := range []string{
		"email_queue_enqueued_total",
		"email_queue_processed_total",
		"email_queue_processing_duration_seconds",
		"email_queue_depth",
		"email_queue_dlq_depth",
		"email_queue_worker_active",
		"email_queue_worker_pool_size",
	} {
		assert.True(t, names[name], "expected metric %q to be present in Gather output", name)
	}
}

func TestNewPrometheusRecorder_PanicsOnDoubleRegister(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	_ = observability.NewPrometheusRecorder(reg)
	assert.Panics(t, func() {
		_ = observability.NewPrometheusRecorder(reg)
	}, "registering the same metrics twice against the same registry must panic")
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// minimalConfig returns a *config.Config with safe defaults for tests.
// Only the Observability sub-struct is populated; all other fields are zeroed.
func minimalConfig() *config.Config {
	cfg := &config.Config{}
	cfg.Observability.ServiceName = "email-queue-test"
	cfg.Observability.OTELEndpoint = ""
	return cfg
}
