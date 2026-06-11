package worker_test

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/DarioJejer/go-email-queue/internal/adapters/stubs"
	"github.com/DarioJejer/go-email-queue/internal/config"
	"github.com/DarioJejer/go-email-queue/internal/observability"
	"github.com/DarioJejer/go-email-queue/internal/ports"
	"github.com/DarioJejer/go-email-queue/internal/worker"
)

type fixedDepthDLQWriter struct {
	stubs.StubDLQWriter
	depth int64
}

func (f *fixedDepthDLQWriter) DLQDepth(_ context.Context, _, _ string) (int64, error) {
	return f.depth, nil
}

type recordingDLQMetrics struct {
	stubs.StubMetricsRecorder
	depths []struct {
		tenantID string
		taskType string
		depth    float64
	}
}

func (r *recordingDLQMetrics) RecordDLQDepth(tenantID, taskType string, depth float64) {
	r.depths = append(r.depths, struct {
		tenantID string
		taskType string
		depth    float64
	}{tenantID, taskType, depth})
}

func TestDLQMonitor_Run(t *testing.T) {
	cfg := &config.Config{
		Retry: config.RetryConfig{
			DLQMonitorInterval: 40 * time.Millisecond,
			DLQAlertThreshold:  100,
		},
	}
	metrics := &recordingDLQMetrics{}
	monitor := worker.NewDLQMonitor(
		cfg,
		&fixedDepthDLQWriter{depth: 5},
		metrics,
		[]string{"queue:dlq:tenant-a:transactional"},
	)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- monitor.Run(ctx) }()

	deadline := time.After(3 * time.Second)
	for len(metrics.depths) < 2 {
		select {
		case <-deadline:
			t.Fatalf("expected at least 2 scrape ticks, got %d", len(metrics.depths))
		case <-time.After(10 * time.Millisecond):
		}
	}

	cancel()
	require.NoError(t, <-runDone)
}

func TestDLQMonitor_WarnsOnThresholdExceeded(t *testing.T) {
	var buf bytes.Buffer
	logger := zerolog.New(&buf).Level(zerolog.WarnLevel)
	ctx, cancel := context.WithCancel(observability.WithLogger(context.Background(), logger))

	cfg := &config.Config{
		Retry: config.RetryConfig{
			DLQMonitorInterval: 20 * time.Millisecond,
			DLQAlertThreshold:  10,
		},
	}
	monitor := worker.NewDLQMonitor(
		cfg,
		&fixedDepthDLQWriter{depth: 11},
		stubs.NewStubMetricsRecorder(),
		[]string{"queue:dlq:tenant-a:transactional"},
	)

	runDone := make(chan error, 1)
	go func() { runDone <- monitor.Run(ctx) }()

	require.Eventually(t, func() bool {
		return strings.Contains(buf.String(), "dlq.depth_threshold_exceeded")
	}, 3*time.Second, 20*time.Millisecond, "expected threshold warning log")

	cancel()
	require.NoError(t, <-runDone)
}

// Compile-time check that fixedDepthDLQWriter satisfies DLQWriter.
var _ ports.DLQWriter = (*fixedDepthDLQWriter)(nil)

// Compile-time check that recordingDLQMetrics satisfies MetricsRecorder.
var _ ports.MetricsRecorder = (*recordingDLQMetrics)(nil)
