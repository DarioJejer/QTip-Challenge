// Package stubs provides no-op implementations of every port interface.
// They are used in:
//   - Unit tests that only care about one component and want all dependencies silent.
//   - The M2 skeleton binary (before real adapters are wired in M3).
//   - Local development when running without live external services.
//
// Each stub satisfies its interface at compile time via a blank-identifier
// assignment. All methods return zero values and nil errors.
package stubs

import (
	"context"
	"time"

	"github.com/DarioJejer/go-email-queue/internal/domain"
	"github.com/DarioJejer/go-email-queue/internal/ports"
)

// ---------------------------------------------------------------------------
// Compile-time interface satisfaction checks.
// If any of these fail, the build breaks immediately with a clear error.
// ---------------------------------------------------------------------------

var (
	_ ports.TaskProducer     = (*StubProducer)(nil)
	_ ports.TaskConsumer     = (*StubConsumer)(nil)
	_ ports.RetryScheduler   = (*StubRetryScheduler)(nil)
	_ ports.DLQWriter        = (*StubDLQWriter)(nil)
	_ ports.IdempotencyStore = (*StubIdempotencyStore)(nil)
	_ ports.EmailSender      = (*StubEmailSender)(nil)
	_ ports.MetricsRecorder  = (*StubMetricsRecorder)(nil)
	_ ports.QueueDepthChecker = (*StubQueueDepthChecker)(nil)
)

// ---------------------------------------------------------------------------
// StubProducer — ports.TaskProducer
// ---------------------------------------------------------------------------

// StubProducer is a no-op TaskProducer. All enqueue calls succeed silently.
type StubProducer struct{}

// NewStubProducer returns a zero-value StubProducer.
func NewStubProducer() *StubProducer { return &StubProducer{} }

func (s *StubProducer) Enqueue(_ context.Context, _ *domain.EmailTask) error { return nil }
func (s *StubProducer) EnqueueBatch(_ context.Context, _ []*domain.EmailTask) error { return nil }
func (s *StubProducer) EnqueueDelayed(_ context.Context, _ *domain.EmailTask, _ time.Duration) error {
	return nil
}

// ---------------------------------------------------------------------------
// StubConsumer — ports.TaskConsumer
// ---------------------------------------------------------------------------

// StubConsumer is a no-op TaskConsumer. Consume returns a channel that is
// immediately closed (no tasks will ever be delivered).
type StubConsumer struct{}

// NewStubConsumer returns a zero-value StubConsumer.
func NewStubConsumer() *StubConsumer { return &StubConsumer{} }

func (s *StubConsumer) Consume(_ context.Context) (<-chan *domain.EmailTask, error) {
	ch := make(chan *domain.EmailTask)
	close(ch)
	return ch, nil
}
func (s *StubConsumer) Acknowledge(_ context.Context, _ *domain.EmailTask) error { return nil }
func (s *StubConsumer) Nack(_ context.Context, _ *domain.EmailTask, _ error) error { return nil }
func (s *StubConsumer) ClaimStale(_ context.Context, _ time.Duration) ([]*domain.EmailTask, error) {
	return nil, nil
}

// ---------------------------------------------------------------------------
// StubRetryScheduler — ports.RetryScheduler
// ---------------------------------------------------------------------------

// StubRetryScheduler is a no-op RetryScheduler. Retries are silently dropped.
type StubRetryScheduler struct{}

// NewStubRetryScheduler returns a zero-value StubRetryScheduler.
func NewStubRetryScheduler() *StubRetryScheduler { return &StubRetryScheduler{} }

func (s *StubRetryScheduler) ScheduleRetry(_ context.Context, _ *domain.EmailTask, _ time.Duration) error {
	return nil
}
func (s *StubRetryScheduler) FlushReady(_ context.Context) ([]*domain.EmailTask, error) {
	return nil, nil
}

// ---------------------------------------------------------------------------
// StubDLQWriter — ports.DLQWriter
// ---------------------------------------------------------------------------

// StubDLQWriter is a no-op DLQWriter. Dead-lettered entries are silently discarded.
type StubDLQWriter struct{}

// NewStubDLQWriter returns a zero-value StubDLQWriter.
func NewStubDLQWriter() *StubDLQWriter { return &StubDLQWriter{} }

func (s *StubDLQWriter) SendToDLQ(_ context.Context, _ *domain.DLQEntry) error { return nil }
func (s *StubDLQWriter) ListDLQ(_ context.Context, _, _ string, _ int) ([]*domain.DLQEntry, error) {
	return nil, nil
}
func (s *StubDLQWriter) DLQDepth(_ context.Context, _, _ string) (int64, error) { return 0, nil }

// ---------------------------------------------------------------------------
// StubIdempotencyStore — ports.IdempotencyStore
// ---------------------------------------------------------------------------

// StubIdempotencyStore always acquires the processing lock (returns acquired=true)
// and never reports any task as completed. This allows the full processing path
// to execute during unit tests.
type StubIdempotencyStore struct{}

// NewStubIdempotencyStore returns a zero-value StubIdempotencyStore.
func NewStubIdempotencyStore() *StubIdempotencyStore { return &StubIdempotencyStore{} }

func (s *StubIdempotencyStore) SetProcessing(_ context.Context, _, _ string) (bool, error) {
	return true, nil
}
func (s *StubIdempotencyStore) SetCompleted(_ context.Context, _ string) error { return nil }
func (s *StubIdempotencyStore) IsCompleted(_ context.Context, _ string) (bool, error) {
	return false, nil
}

// ---------------------------------------------------------------------------
// StubEmailSender — ports.EmailSender
// ---------------------------------------------------------------------------

// StubEmailSender is a no-op EmailSender. All sends succeed without any I/O.
type StubEmailSender struct{}

// NewStubEmailSender returns a zero-value StubEmailSender.
func NewStubEmailSender() *StubEmailSender { return &StubEmailSender{} }

func (s *StubEmailSender) Send(_ context.Context, _ *domain.EmailTask) error { return nil }
func (s *StubEmailSender) SendBatch(_ context.Context, _ []*domain.EmailTask) error { return nil }
func (s *StubEmailSender) HealthCheck(_ context.Context) error                      { return nil }

// ---------------------------------------------------------------------------
// StubMetricsRecorder — ports.MetricsRecorder
// ---------------------------------------------------------------------------

// StubMetricsRecorder discards all metric observations. Use it in tests where
// the metrics output is not under test.
type StubMetricsRecorder struct{}

// NewStubMetricsRecorder returns a zero-value StubMetricsRecorder.
func NewStubMetricsRecorder() *StubMetricsRecorder { return &StubMetricsRecorder{} }

func (s *StubMetricsRecorder) RecordEnqueued(_, _, _ string)                         {}
func (s *StubMetricsRecorder) RecordProcessed(_, _, _ string, _ float64)             {}
func (s *StubMetricsRecorder) RecordQueueDepth(_ string, _ float64)                  {}
func (s *StubMetricsRecorder) RecordDLQDepth(_, _ string, _ float64)                 {}
func (s *StubMetricsRecorder) RecordWorkerStats(_ domain.WorkerStats)                {}

// ---------------------------------------------------------------------------
// StubQueueDepthChecker — ports.QueueDepthChecker
// ---------------------------------------------------------------------------

// StubQueueDepthChecker always reports zero depth. Useful when queue-depth
// metrics are not relevant to the test under execution.
type StubQueueDepthChecker struct{}

// NewStubQueueDepthChecker returns a zero-value StubQueueDepthChecker.
func NewStubQueueDepthChecker() *StubQueueDepthChecker { return &StubQueueDepthChecker{} }

func (s *StubQueueDepthChecker) QueueDepth(_ context.Context, _ string) (int64, error) {
	return 0, nil
}
func (s *StubQueueDepthChecker) AllQueueDepths(_ context.Context) (map[string]int64, error) {
	return map[string]int64{}, nil
}
