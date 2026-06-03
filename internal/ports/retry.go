package ports

import (
	"context"
	"time"

	"github.com/DarioJejer/go-email-queue/internal/domain"
)

// RetryScheduler manages the delayed-retry sorted set. On failure the worker
// calls ScheduleRetry; the DelayedScheduler goroutine calls FlushReady on a
// ticker to move ready tasks back to the main Streams queue (ADR-005).
//
// Redis key: queue:email:retry:delayed (sorted set, score = Unix timestamp).
type RetryScheduler interface {
	// ScheduleRetry increments task.Attempt, marshals the task, and writes it
	// to the delayed sorted set with ZADD NX score=(now+delay). The NX flag
	// makes the operation idempotent: re-scheduling an already-queued task ID
	// is a no-op.
	//
	// delay is typically computed by domain.RetryPolicy.ComputeDelay(task.Attempt).
	ScheduleRetry(ctx context.Context, task *domain.EmailTask, delay time.Duration) error

	// FlushReady reads all entries with score ≤ time.Now().Unix() via
	// ZRANGEBYSCORE, re-enqueues each to its priority Stream via XADD, and
	// removes processed entries from the sorted set with ZREM — all in a single
	// Redis pipeline for atomicity. It returns the list of flushed tasks.
	//
	// The DelayedScheduler calls this on a cfg.RetrySchedulerInterval ticker
	// (default 1 s). Partial pipeline failures are logged but do not halt
	// processing of remaining entries.
	FlushReady(ctx context.Context) ([]*domain.EmailTask, error)
}
