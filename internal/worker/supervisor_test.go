package worker_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/DarioJejer/go-email-queue/internal/adapters/stubs"
	"github.com/DarioJejer/go-email-queue/internal/config"
	"github.com/DarioJejer/go-email-queue/internal/domain"
	"github.com/DarioJejer/go-email-queue/internal/ports"
	"github.com/DarioJejer/go-email-queue/internal/worker"
)

// ---------------------------------------------------------------------------
// Test doubles
// ---------------------------------------------------------------------------

// chanConsumer is a controllable TaskConsumer backed by a Go channel.
// Tests push tasks via send(); Acknowledge/Nack counts are tracked atomically.
type chanConsumer struct {
	ch     chan *domain.EmailTask
	acked  int64
	nacked int64
}

func newChanConsumer(buf int) *chanConsumer {
	return &chanConsumer{ch: make(chan *domain.EmailTask, buf)}
}

func (c *chanConsumer) Consume(_ context.Context) (<-chan *domain.EmailTask, error) {
	return c.ch, nil
}
func (c *chanConsumer) Acknowledge(_ context.Context, _ *domain.EmailTask) error {
	atomic.AddInt64(&c.acked, 1)
	return nil
}
func (c *chanConsumer) Nack(_ context.Context, _ *domain.EmailTask, _ error) error {
	atomic.AddInt64(&c.nacked, 1)
	return nil
}
func (c *chanConsumer) ClaimStale(_ context.Context, _ time.Duration) ([]*domain.EmailTask, error) {
	return nil, nil
}
func (c *chanConsumer) send(task *domain.EmailTask) { c.ch <- task }

// countingSender records calls and can be configured to return an error or panic.
type countingSender struct {
	calls   int64
	retErr  error
	doPanic bool
}

func (s *countingSender) Send(_ context.Context, _ *domain.EmailTask) error {
	atomic.AddInt64(&s.calls, 1)
	if s.doPanic {
		panic("deliberate test panic")
	}
	return s.retErr
}
func (s *countingSender) SendBatch(_ context.Context, _ []*domain.EmailTask) error { return nil }
func (s *countingSender) HealthCheck(_ context.Context) error                      { return nil }

var _ ports.EmailSender = (*countingSender)(nil)

// neverAcquireStore is an IdempotencyStore that always returns acquired=false,
// simulating a duplicate task delivery.
type neverAcquireStore struct{}

func (n *neverAcquireStore) SetProcessing(_ context.Context, _, _ string) (bool, error) {
	return false, nil
}
func (n *neverAcquireStore) SetCompleted(_ context.Context, _ string) error { return nil }
func (n *neverAcquireStore) IsCompleted(_ context.Context, _ string) (bool, error) {
	return true, nil
}
func (n *neverAcquireStore) ClearProcessing(_ context.Context, _ string) error { return nil }

var _ ports.IdempotencyStore = (*neverAcquireStore)(nil)

// countingDLQWriter counts SendToDLQ calls.
type countingDLQWriter struct {
	calls int64
	*stubs.StubDLQWriter
}

func newCountingDLQ() *countingDLQWriter {
	return &countingDLQWriter{StubDLQWriter: stubs.NewStubDLQWriter()}
}

func (d *countingDLQWriter) SendToDLQ(ctx context.Context, entry *domain.DLQEntry) error {
	atomic.AddInt64(&d.calls, 1)
	return d.StubDLQWriter.SendToDLQ(ctx, entry)
}

// countingRetryScheduler counts ScheduleRetry calls.
type countingRetryScheduler struct {
	calls int64
	*stubs.StubRetryScheduler
}

func newCountingRetry() *countingRetryScheduler {
	return &countingRetryScheduler{StubRetryScheduler: stubs.NewStubRetryScheduler()}
}

func (r *countingRetryScheduler) ScheduleRetry(ctx context.Context, task *domain.EmailTask, delay time.Duration) error {
	atomic.AddInt64(&r.calls, 1)
	return r.StubRetryScheduler.ScheduleRetry(ctx, task, delay)
}

// trackingIdempotencyStore records ClearProcessing calls to verify they happen
// before Nacks in error paths.
type trackingIdempotencyStore struct {
	cleared int64
	*stubs.StubIdempotencyStore
}

func newTrackingIdempotency() *trackingIdempotencyStore {
	return &trackingIdempotencyStore{StubIdempotencyStore: stubs.NewStubIdempotencyStore()}
}

func (t *trackingIdempotencyStore) ClearProcessing(ctx context.Context, taskID string) error {
	atomic.AddInt64(&t.cleared, 1)
	return t.StubIdempotencyStore.ClearProcessing(ctx, taskID)
}

// failingRetryScheduler returns an error on ScheduleRetry.
type failingRetryScheduler struct{ *stubs.StubRetryScheduler }

func newFailingRetry() *failingRetryScheduler {
	return &failingRetryScheduler{StubRetryScheduler: stubs.NewStubRetryScheduler()}
}

func (r *failingRetryScheduler) ScheduleRetry(_ context.Context, _ *domain.EmailTask, _ time.Duration) error {
	return fmt.Errorf("redis unavailable")
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

func makeTestConfig() *config.Config {
	return &config.Config{
		Worker: config.WorkerConfig{
			PoolSize:           4,
			ConsumerGroup:      "test-group",
			ConsumerName:       "test-consumer",
			BlockTimeout:       50 * time.Millisecond,
			ClaimIdleThreshold: time.Hour, // disable stale claims during tests
			DrainTimeout:       5 * time.Second,
		},
	}
}

func validSupervisorTask(id string) *domain.EmailTask {
	return &domain.EmailTask{
		ID:          id,
		TenantID:    "tenant-1",
		Type:        domain.TaskTypeTransactional,
		Priority:    domain.PriorityNormal,
		Recipient:   "user@example.com",
		EnqueuedAt:  time.Now(),
		Attempt:     0,
		MaxAttempts: 3,
		Status:      domain.StatusPending,
		Metadata:    map[string]string{"_stream_id": "1-1", "_stream_key": "{queue}:email:normal"},
	}
}

func newSupervisor(
	cfg *config.Config,
	consumer ports.TaskConsumer,
	sender ports.EmailSender,
	idempotency ports.IdempotencyStore,
	retryScheduler ports.RetryScheduler,
	dlq ports.DLQWriter,
) *worker.Supervisor {
	return worker.NewSupervisor(
		cfg, consumer, sender, idempotency, retryScheduler, dlq,
		stubs.NewStubMetricsRecorder(),
		noop.NewTracerProvider().Tracer("test"),
	)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestSupervisor_ProcessesTaskSuccessfully verifies the happy path:
// task is consumed, sender is called, consumer.Acknowledge is called.
func TestSupervisor_ProcessesTaskSuccessfully(t *testing.T) {
	cfg := makeTestConfig()
	cons := newChanConsumer(4)
	sender := &countingSender{}
	sv := newSupervisor(cfg, cons, sender,
		stubs.NewStubIdempotencyStore(),
		stubs.NewStubRetryScheduler(),
		stubs.NewStubDLQWriter(),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan error, 1)
	go func() { runDone <- sv.Run(ctx) }()

	cons.send(validSupervisorTask("task-success-01"))

	assert.Eventually(t, func() bool {
		return atomic.LoadInt64(&sender.calls) == 1
	}, 3*time.Second, 10*time.Millisecond, "sender must be called once")

	assert.Eventually(t, func() bool {
		return atomic.LoadInt64(&cons.acked) == 1
	}, 3*time.Second, 10*time.Millisecond, "task must be acknowledged")

	cancel()
	require.NoError(t, <-runDone)
}

// TestSupervisor_PanicRecovery verifies that a panicking sender does not crash
// the supervisor and that the task is nacked (flagged as poison).
func TestSupervisor_PanicRecovery(t *testing.T) {
	cfg := makeTestConfig()
	cons := newChanConsumer(4)
	sender := &countingSender{doPanic: true}
	sv := newSupervisor(cfg, cons, sender,
		stubs.NewStubIdempotencyStore(),
		stubs.NewStubRetryScheduler(),
		stubs.NewStubDLQWriter(),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan error, 1)
	go func() { runDone <- sv.Run(ctx) }()

	cons.send(validSupervisorTask("task-panic-01"))

	assert.Eventually(t, func() bool {
		return atomic.LoadInt64(&cons.nacked) == 1
	}, 3*time.Second, 10*time.Millisecond, "panicking task must be nacked")

	// Supervisor must still be running — send another task successfully.
	sender.doPanic = false
	sender.retErr = nil
	cons.send(validSupervisorTask("task-after-panic-01"))

	assert.Eventually(t, func() bool {
		return atomic.LoadInt64(&sender.calls) >= 2
	}, 3*time.Second, 10*time.Millisecond, "supervisor must continue after panic")

	cancel()
	require.NoError(t, <-runDone)
}

// TestSupervisor_IdempotentSkip verifies that when the idempotency gate
// denies the lock (duplicate delivery), sender is not called and task is ACK'd.
func TestSupervisor_IdempotentSkip(t *testing.T) {
	cfg := makeTestConfig()
	cons := newChanConsumer(4)
	sender := &countingSender{}
	sv := newSupervisor(cfg, cons, sender,
		&neverAcquireStore{},
		stubs.NewStubRetryScheduler(),
		stubs.NewStubDLQWriter(),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan error, 1)
	go func() { runDone <- sv.Run(ctx) }()

	cons.send(validSupervisorTask("task-dedup-01"))

	assert.Eventually(t, func() bool {
		return atomic.LoadInt64(&cons.acked) == 1
	}, 3*time.Second, 10*time.Millisecond, "duplicate task must be acknowledged")

	assert.Equal(t, int64(0), atomic.LoadInt64(&sender.calls), "sender must not be called for duplicate")

	cancel()
	require.NoError(t, <-runDone)
}

// TestSupervisor_RetriesOnFailure verifies that a retryable send failure causes
// ScheduleRetry to be called and the task to be ACK'd from the PEL.
func TestSupervisor_RetriesOnFailure(t *testing.T) {
	cfg := makeTestConfig()
	cons := newChanConsumer(4)
	sender := &countingSender{retErr: fmt.Errorf("transient smtp error")}
	retryScheduler := newCountingRetry()
	sv := newSupervisor(cfg, cons, sender,
		stubs.NewStubIdempotencyStore(),
		retryScheduler,
		stubs.NewStubDLQWriter(),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan error, 1)
	go func() { runDone <- sv.Run(ctx) }()

	cons.send(validSupervisorTask("task-retry-01"))

	assert.Eventually(t, func() bool {
		return atomic.LoadInt64(&retryScheduler.calls) == 1
	}, 3*time.Second, 10*time.Millisecond, "ScheduleRetry must be called")

	assert.Eventually(t, func() bool {
		return atomic.LoadInt64(&cons.acked) == 1
	}, 3*time.Second, 10*time.Millisecond, "task must be ACK'd after retry scheduling")

	cancel()
	require.NoError(t, <-runDone)
}

// TestSupervisor_SendsToDLQOnMaxAttempts verifies that a task with Attempt >=
// MaxAttempts (no retries remaining) is routed to the DLQ.
func TestSupervisor_SendsToDLQOnMaxAttempts(t *testing.T) {
	cfg := makeTestConfig()
	cons := newChanConsumer(4)
	sender := &countingSender{retErr: fmt.Errorf("smtp error")}
	dlq := newCountingDLQ()
	sv := newSupervisor(cfg, cons, sender,
		stubs.NewStubIdempotencyStore(),
		stubs.NewStubRetryScheduler(),
		dlq,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan error, 1)
	go func() { runDone <- sv.Run(ctx) }()

	task := validSupervisorTask("task-dlq-01")
	task.Attempt = task.MaxAttempts // IsRetryable() returns false
	cons.send(task)

	assert.Eventually(t, func() bool {
		return atomic.LoadInt64(&dlq.calls) == 1
	}, 3*time.Second, 10*time.Millisecond, "task must be sent to DLQ")

	cancel()
	require.NoError(t, <-runDone)
}

// TestSupervisor_GracefulShutdown verifies that in-flight tasks complete before
// Run returns after context cancellation.
func TestSupervisor_GracefulShutdown(t *testing.T) {
	cfg := makeTestConfig()
	cons := newChanConsumer(8)

	var processCount int64
	sender := &slowSender{delay: 50 * time.Millisecond, counter: &processCount}
	sv := newSupervisor(cfg, cons, sender,
		stubs.NewStubIdempotencyStore(),
		stubs.NewStubRetryScheduler(),
		stubs.NewStubDLQWriter(),
	)

	ctx, cancel := context.WithCancel(context.Background())

	runDone := make(chan error, 1)
	go func() { runDone <- sv.Run(ctx) }()

	for i := 0; i < 4; i++ {
		cons.send(validSupervisorTask(fmt.Sprintf("drain-%d", i)))
	}

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-runDone:
		require.NoError(t, err)
	case <-time.After(6 * time.Second):
		t.Fatal("supervisor did not stop within drain timeout")
	}

	assert.Equal(t, int64(4), atomic.LoadInt64(&processCount),
		"all in-flight tasks must complete during drain")
}

// TestSupervisor_ScheduleRetryFailure_ClearsLock verifies that when
// ScheduleRetry fails, ClearProcessing is called before Nacking so that the
// PEL reclaim can re-acquire the idempotency lock instead of being dropped.
func TestSupervisor_ScheduleRetryFailure_ClearsLock(t *testing.T) {
	cfg := makeTestConfig()
	cons := newChanConsumer(4)
	sender := &countingSender{retErr: fmt.Errorf("transient smtp error")}
	idempotency := newTrackingIdempotency()
	sv := newSupervisor(cfg, cons, sender,
		idempotency,
		newFailingRetry(),
		stubs.NewStubDLQWriter(),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan error, 1)
	go func() { runDone <- sv.Run(ctx) }()

	cons.send(validSupervisorTask("task-schedretry-fail-01"))

	// Task must be nacked (stays in PEL).
	assert.Eventually(t, func() bool {
		return atomic.LoadInt64(&cons.nacked) == 1
	}, 3*time.Second, 10*time.Millisecond, "task must be nacked when ScheduleRetry fails")

	// ClearProcessing must have been called so the reclaim can re-acquire.
	assert.Equal(t, int64(1), atomic.LoadInt64(&idempotency.cleared),
		"ClearProcessing must be called before Nack to unblock PEL reclaim")

	cancel()
	require.NoError(t, <-runDone)
}

// slowSender simulates work by sleeping before returning success.
type slowSender struct {
	delay   time.Duration
	counter *int64
}

func (s *slowSender) Send(_ context.Context, _ *domain.EmailTask) error {
	time.Sleep(s.delay)
	atomic.AddInt64(s.counter, 1)
	return nil
}
func (s *slowSender) SendBatch(_ context.Context, _ []*domain.EmailTask) error { return nil }
func (s *slowSender) HealthCheck(_ context.Context) error                      { return nil }

var _ ports.EmailSender = (*slowSender)(nil)

// TestErrNonRetryable_ErrorsIs verifies sentinel error wrapping.
func TestErrNonRetryable_ErrorsIs(t *testing.T) {
	wrapped := fmt.Errorf("provider 422: %w", ports.ErrNonRetryable)
	assert.True(t, errors.Is(wrapped, ports.ErrNonRetryable))
	assert.False(t, errors.Is(wrapped, ports.ErrCircuitOpen))
}

// TestSupervisor_NonRetryableGoesToDLQ verifies that a non-retryable error
// routes to DLQ even when Attempt < MaxAttempts.
func TestSupervisor_NonRetryableGoesToDLQ(t *testing.T) {
	cfg := makeTestConfig()
	cons := newChanConsumer(4)
	nr := fmt.Errorf("address rejected: %w", ports.ErrNonRetryable)
	sender := &countingSender{retErr: nr}
	dlq := newCountingDLQ()
	retryScheduler := newCountingRetry()
	sv := newSupervisor(cfg, cons, sender,
		stubs.NewStubIdempotencyStore(),
		retryScheduler,
		dlq,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan error, 1)
	go func() { runDone <- sv.Run(ctx) }()

	task := validSupervisorTask("task-nonretry-01")
	assert.True(t, task.IsRetryable())
	cons.send(task)

	assert.Eventually(t, func() bool {
		return atomic.LoadInt64(&dlq.calls) == 1
	}, 3*time.Second, 10*time.Millisecond, "non-retryable error must go to DLQ")

	assert.Equal(t, int64(0), atomic.LoadInt64(&retryScheduler.calls),
		"retry scheduler must NOT be called for non-retryable error")

	cancel()
	require.NoError(t, <-runDone)
}
