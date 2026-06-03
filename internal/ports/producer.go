// Package ports defines all application-boundary interfaces (the "ports" in
// Ports-and-Adapters / Hexagonal Architecture). Every external dependency —
// Redis, email provider, metrics backend — is accessed exclusively through
// one of these interfaces.
//
// Rules:
//   - Ports may only import internal/domain and the Go standard library.
//   - Ports must never import internal/adapters or internal/app.
//   - Every method must accept context.Context as its first parameter.
//   - Every method must return an error as its last return value.
package ports

import (
	"context"
	"time"

	"github.com/DarioJejer/go-email-queue/internal/domain"
)

// TaskProducer publishes email tasks to the queue backend.
// The concrete implementation writes to Redis Streams (ADR-002).
// In local development and tests, a stub or in-memory implementation is used.
type TaskProducer interface {
	// Enqueue validates task and writes it to the appropriate priority Redis
	// Stream determined by task.Priority.QueueName(). The task must pass
	// domain.EmailTask.Validate() before publishing; implementations may
	// return a *domain.ValidationError if the task is malformed.
	//
	// Callers should treat a non-nil error as a transient queue-unavailable
	// condition and surface a 503 to the HTTP client.
	Enqueue(ctx context.Context, task *domain.EmailTask) error

	// EnqueueBatch publishes multiple tasks in a single Redis pipeline
	// round-trip. If some tasks fail validation or the pipeline returns
	// partial errors, the implementation returns a *domain.MultiError listing
	// every failed task ID. Successfully enqueued tasks are not rolled back.
	EnqueueBatch(ctx context.Context, tasks []*domain.EmailTask) error

	// EnqueueDelayed schedules a task for future delivery by writing it to
	// the delayed sorted set with score = time.Now().Add(delay).Unix().
	// The DelayedScheduler moves the task to the main Stream once the
	// scheduled time is reached (ADR-005).
	EnqueueDelayed(ctx context.Context, task *domain.EmailTask, delay time.Duration) error
}
