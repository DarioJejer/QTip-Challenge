// Package worker implements the bounded-concurrency worker pool supervisor
// that drives task processing (ADR-004).
package worker

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/DarioJejer/go-email-queue/internal/config"
	"github.com/DarioJejer/go-email-queue/internal/domain"
	"github.com/DarioJejer/go-email-queue/internal/observability"
	"github.com/DarioJejer/go-email-queue/internal/ports"
)

// Supervisor manages a bounded pool of worker goroutines that consume tasks
// from Redis Streams, deliver emails, and handle retries and DLQ routing.
//
// Concurrency is bounded by a buffered-channel semaphore of size PoolSize
// (ADR-004). Panic recovery ensures a panicking worker does not crash the
// supervisor process.
type Supervisor struct {
	cfg            *config.Config
	consumer       ports.TaskConsumer
	sender         ports.EmailSender
	idempotency    ports.IdempotencyStore
	retryScheduler ports.RetryScheduler
	dlqWriter      ports.DLQWriter
	metrics        ports.MetricsRecorder
	tracer         trace.Tracer

	// sem is the semaphore that limits concurrent worker goroutines.
	// Acquiring = sending a token; releasing = receiving a token.
	sem chan struct{}

	// Atomic counters updated by worker goroutines without a mutex.
	activeWorkers  int64
	totalProcessed int64
	totalFailed    int64
	totalRetried   int64
}

// NewSupervisor constructs a Supervisor. All dependencies must be non-nil.
func NewSupervisor(
	cfg *config.Config,
	consumer ports.TaskConsumer,
	sender ports.EmailSender,
	idempotency ports.IdempotencyStore,
	retryScheduler ports.RetryScheduler,
	dlqWriter ports.DLQWriter,
	metrics ports.MetricsRecorder,
	tracer trace.Tracer,
) *Supervisor {
	return &Supervisor{
		cfg:            cfg,
		consumer:       consumer,
		sender:         sender,
		idempotency:    idempotency,
		retryScheduler: retryScheduler,
		dlqWriter:      dlqWriter,
		metrics:        metrics,
		tracer:         tracer,
		sem:            make(chan struct{}, cfg.Worker.PoolSize),
	}
}

// Run starts the main dispatch loop. It blocks until ctx is cancelled, then
// drains all in-flight worker goroutines before returning nil.
//
// Integrate into main.go via errgroup:
//
//	g.Go(func() error { return supervisor.Run(gCtx) })
func (s *Supervisor) Run(ctx context.Context) error {
	logger := observability.LoggerFromContext(ctx)
	logger.Info().
		Int("pool_size", s.cfg.Worker.PoolSize).
		Dur("claim_idle_threshold", s.cfg.Worker.ClaimIdleThreshold).
		Msg("worker supervisor starting")

	taskCh, err := s.consumer.Consume(ctx)
	if err != nil {
		return fmt.Errorf("supervisor: start consumer: %w", err)
	}

	claimTicker := time.NewTicker(s.cfg.Worker.ClaimIdleThreshold)
	defer claimTicker.Stop()

	s.metrics.RecordWorkerStats(domain.WorkerStats{PoolSize: s.cfg.Worker.PoolSize})

	for {
		select {
		case task, ok := <-taskCh:
			if !ok {
				// Channel closed by consumer when ctx was cancelled.
				s.waitForWorkers(ctx)
				return nil
			}
			s.dispatch(ctx, task)

		case <-claimTicker.C:
			claimed, claimErr := s.consumer.ClaimStale(ctx, s.cfg.Worker.ClaimIdleThreshold)
			if claimErr != nil {
				logger.Error().Err(claimErr).Msg("supervisor: ClaimStale error")
				continue
			}
			for _, t := range claimed {
				s.dispatch(ctx, t)
			}

		case <-ctx.Done():
			s.waitForWorkers(ctx)
			return nil
		}
	}
}

// dispatch acquires a semaphore slot and launches a worker goroutine.
// On context cancellation while waiting for the semaphore, the task is nacked.
func (s *Supervisor) dispatch(ctx context.Context, task *domain.EmailTask) {
	select {
	case s.sem <- struct{}{}:
		go s.processTask(ctx, task)
	case <-ctx.Done():
		_ = s.consumer.Nack(ctx, task, ctx.Err())
	}
}

// attemptKey returns the idempotency store key for this specific delivery
// attempt. Scoping to attempt number means:
//   - Two workers racing on the same attempt are deduplicated (acquired=false
//     for the loser) — the core idempotency guarantee.
//   - A later retry attempt (higher number) is NOT blocked by the previous
//     attempt's record, so retries can proceed normally.
func attemptKey(taskID string, attempt int) string {
	return fmt.Sprintf("%s#%d", taskID, attempt)
}

// processTask is the per-task worker goroutine. It handles:
//   - Panic recovery with poison-flag and nack
//   - Idempotency gate (skip duplicate deliveries of the same attempt)
//   - Email sending
//   - Retry scheduling on transient failures
//   - DLQ routing on permanent failures or exhausted retries
//
// The task context is detached from ctx so that graceful-shutdown cancellation
// does not abort in-flight sends (context.WithoutCancel, Go 1.21+).
func (s *Supervisor) processTask(ctx context.Context, task *domain.EmailTask) {
	atomic.AddInt64(&s.activeWorkers, 1)

	// Defer 1: semaphore release + stats (runs LAST due to LIFO).
	defer func() {
		<-s.sem
		atomic.AddInt64(&s.activeWorkers, -1)
		s.metrics.RecordWorkerStats(s.Stats())
	}()

	// Defer 2: panic recovery (runs FIRST due to LIFO — catches panics in body).
	defer func() {
		if r := recover(); r != nil {
			observability.LoggerFromContext(ctx).Error().
				Str("task_id", task.ID).
				Interface("panic", r).
				Msg("worker: panic recovered — nacking task")
			if task.Metadata == nil {
				task.Metadata = make(map[string]string)
			}
			task.Metadata["poison"] = "true"
			atomic.AddInt64(&s.totalFailed, 1)
			_ = s.consumer.Nack(ctx, task, fmt.Errorf("panic: %v", r))
		}
	}()

	// Detach from the parent cancellation so in-flight tasks complete during drain.
	taskCtx := context.WithoutCancel(ctx)
	taskCtx = s.buildTraceCtx(taskCtx, task)
	taskCtx, span := s.tracer.Start(taskCtx, "worker.process",
		trace.WithAttributes(
			attribute.String("task.id", task.ID),
			attribute.String("task.tenant_id", task.TenantID),
			attribute.String("task.type", string(task.Type)),
			attribute.Int("task.attempt", task.Attempt),
		),
	)
	defer span.End()

	// --------------- Idempotency gate ----------------------------------------
	//
	// Key is scoped to (taskID, attempt) so that:
	//   - Concurrent re-deliveries of the same attempt are deduplicated.
	//   - Later retry attempts (incremented Attempt field) are not blocked.

	idKey := attemptKey(task.ID, task.Attempt)
	acquired, idErr := s.idempotency.SetProcessing(taskCtx, idKey, s.cfg.Worker.ConsumerName)
	if idErr != nil {
		observability.LoggerFromContext(taskCtx).Warn().
			Err(idErr).Str("task_id", task.ID).
			Msg("worker: idempotency check failed — processing anyway")
	}
	if !acquired {
		_ = s.consumer.Acknowledge(taskCtx, task)
		s.metrics.RecordProcessed(task.TenantID, string(task.Type), "deduped", 0)
		return
	}

	// --------------- Send email -----------------------------------------------

	start := time.Now()
	sendErr := s.sender.Send(taskCtx, task)
	duration := time.Since(start).Seconds()

	if sendErr == nil {
		_ = s.consumer.Acknowledge(taskCtx, task)
		_ = s.idempotency.SetCompleted(taskCtx, idKey)
		atomic.AddInt64(&s.totalProcessed, 1)
		s.metrics.RecordProcessed(task.TenantID, string(task.Type), "success", duration)
		observability.LoggerFromContext(taskCtx).Info().
			Str("task_id", task.ID).
			Float64("duration_seconds", duration).
			Msg("task.processed")
		return
	}

	// --------------- Error path -----------------------------------------------

	atomic.AddInt64(&s.totalFailed, 1)
	task.LastError = sendErr.Error()
	task.Status = domain.StatusFailed

	nonRetryable := errors.Is(sendErr, ports.ErrNonRetryable) ||
		errors.Is(sendErr, ports.ErrCircuitOpen)

	if nonRetryable || !task.IsRetryable() {
		reason := "max attempts exceeded"
		if nonRetryable {
			reason = "non-retryable error"
		}
		entry := &domain.DLQEntry{
			Task:          task,
			DeadAt:        time.Now(),
			FailureReason: reason,
			FinalError:    sendErr.Error(),
		}
		// Only ACK once the DLQ write is confirmed. If it fails, Nack so the
		// message stays in the PEL and is reclaimed by the next ClaimStale tick.
		if dlqErr := s.dlqWriter.SendToDLQ(taskCtx, entry); dlqErr != nil {
			observability.LoggerFromContext(taskCtx).Error().
				Err(dlqErr).Str("task_id", task.ID).
				Msg("worker: SendToDLQ failed — nacking to preserve PEL entry")
			_ = s.consumer.Nack(taskCtx, task, dlqErr)
			return
		}
		_ = s.consumer.Acknowledge(taskCtx, task)
		s.metrics.RecordProcessed(task.TenantID, string(task.Type), "dead", duration)
		observability.LoggerFromContext(taskCtx).Warn().
			Str("task_id", task.ID).Str("reason", reason).
			Msg("task.dead_lettered")
		return
	}

	// Retryable: increment attempt count, persist to retry sorted set, then
	// ACK from the PEL. Incrementing before ScheduleRetry ensures the stored
	// task carries the correct next-attempt count so IsRetryable() will
	// eventually exhaust MaxAttempts.
	//
	// If ScheduleRetry fails, Nack so the message stays in the PEL and is
	// reclaimed by the next ClaimStale tick — never drop a retryable task.
	task.Attempt++
	delay := task.NextRetryDelay()
	if schedErr := s.retryScheduler.ScheduleRetry(taskCtx, task, delay); schedErr != nil {
		observability.LoggerFromContext(taskCtx).Error().
			Err(schedErr).Str("task_id", task.ID).
			Msg("worker: ScheduleRetry failed — nacking to preserve PEL entry")
		_ = s.consumer.Nack(taskCtx, task, schedErr)
		return
	}
	_ = s.consumer.Acknowledge(taskCtx, task)
	atomic.AddInt64(&s.totalRetried, 1)
	s.metrics.RecordProcessed(task.TenantID, string(task.Type), "failed", duration)
	observability.LoggerFromContext(taskCtx).Warn().
		Str("task_id", task.ID).
		Int("attempt", task.Attempt).
		Dur("retry_delay", delay).
		Err(sendErr).
		Msg("task.retry_scheduled")
}

// buildTraceCtx constructs a context that carries the producer's remote span
// as the parent, enabling end-to-end distributed trace continuity (ADR-007).
func (s *Supervisor) buildTraceCtx(ctx context.Context, task *domain.EmailTask) context.Context {
	if task.TraceID == "" {
		return ctx
	}
	traceID, err := trace.TraceIDFromHex(task.TraceID)
	if err != nil {
		return ctx
	}
	spanID, err := trace.SpanIDFromHex(task.SpanID)
	if err != nil {
		return ctx
	}
	remote := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	return trace.ContextWithRemoteSpanContext(ctx, remote)
}

// waitForWorkers blocks until all in-flight goroutines release the semaphore
// or cfg.Worker.DrainTimeout elapses, whichever comes first.
func (s *Supervisor) waitForWorkers(ctx context.Context) {
	logger := observability.LoggerFromContext(ctx)

	drainCtx, cancel := context.WithTimeout(context.Background(), s.cfg.Worker.DrainTimeout)
	defer cancel()

	logTicker := time.NewTicker(5 * time.Second)
	defer logTicker.Stop()

	for {
		if atomic.LoadInt64(&s.activeWorkers) == 0 {
			logger.Info().Msg("supervisor: all workers drained")
			return
		}
		select {
		case <-drainCtx.Done():
			logger.Warn().
				Int64("active_workers", atomic.LoadInt64(&s.activeWorkers)).
				Msg("supervisor: drain timeout — forcing shutdown")
			return
		case <-logTicker.C:
			logger.Info().
				Int64("active_workers", atomic.LoadInt64(&s.activeWorkers)).
				Msg("supervisor: draining in-flight workers")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// Drain waits for all in-flight workers to finish within timeout. It is called
// by the shutdown coordinator after stopping the HTTP server and before closing
// Redis, ensuring workers can still make Redis calls to ACK/NACK their tasks
// (ADR-008).
func (s *Supervisor) Drain(timeout time.Duration) error {
	drainCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	logTicker := time.NewTicker(5 * time.Second)
	defer logTicker.Stop()

	for {
		if atomic.LoadInt64(&s.activeWorkers) == 0 {
			return nil
		}
		select {
		case <-drainCtx.Done():
			return fmt.Errorf("supervisor: drain timeout: %d workers still active",
				atomic.LoadInt64(&s.activeWorkers))
		case <-logTicker.C:
			// periodic log is handled by the calling goroutine if needed
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// Stats returns a point-in-time snapshot of the supervisor's operational
// counters using atomic loads.
func (s *Supervisor) Stats() domain.WorkerStats {
	return domain.WorkerStats{
		ActiveWorkers:  atomic.LoadInt64(&s.activeWorkers),
		PoolSize:       s.cfg.Worker.PoolSize,
		TotalProcessed: atomic.LoadInt64(&s.totalProcessed),
		TotalFailed:    atomic.LoadInt64(&s.totalFailed),
		TotalRetried:   atomic.LoadInt64(&s.totalRetried),
	}
}
