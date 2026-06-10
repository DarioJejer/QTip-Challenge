package worker_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/DarioJejer/go-email-queue/internal/adapters/stubs"
	"github.com/DarioJejer/go-email-queue/internal/config"
	"github.com/DarioJejer/go-email-queue/internal/domain"
	"github.com/DarioJejer/go-email-queue/internal/worker"
)

// countingFlushScheduler is a ports.RetryScheduler test double that sends on
// a buffered channel each time FlushReady is called, allowing the test to
// wait for an exact number of invocations without polling.
type countingFlushScheduler struct {
	flushCalls chan struct{}
}

func newCountingFlushScheduler() *countingFlushScheduler {
	return &countingFlushScheduler{flushCalls: make(chan struct{}, 100)}
}

func (s *countingFlushScheduler) ScheduleRetry(_ context.Context, _ *domain.EmailTask, _ time.Duration) error {
	return nil
}

func (s *countingFlushScheduler) FlushReady(_ context.Context) ([]*domain.EmailTask, error) {
	s.flushCalls <- struct{}{}
	return nil, nil
}

// TestDelayedScheduler_Run verifies that Run calls FlushReady on each ticker
// tick and exits cleanly (returning nil) when the context is cancelled.
func TestDelayedScheduler_Run(t *testing.T) {
	cfg := &config.Config{
		Retry: config.RetryConfig{
			SchedulerInterval: 40 * time.Millisecond,
		},
	}
	sched := newCountingFlushScheduler()
	ds := worker.NewDelayedScheduler(cfg, sched, stubs.NewStubMetricsRecorder())

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- ds.Run(ctx) }()

	// Drain at least 2 flush calls -- confirms the ticker fires more than once.
	for i := 0; i < 2; i++ {
		select {
		case <-sched.flushCalls:
		case <-time.After(3 * time.Second):
			t.Fatalf("FlushReady was not called within 3s (call %d/2)", i+1)
		}
	}

	cancel()
	require.NoError(t, <-runDone, "Run must return nil on context cancellation")
}
