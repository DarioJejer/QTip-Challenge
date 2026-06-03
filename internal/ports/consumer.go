package ports

import (
	"context"
	"time"

	"github.com/DarioJejer/go-email-queue/internal/domain"
)

// TaskConsumer reads email tasks from the queue backend and manages message
// acknowledgement. The concrete implementation uses Redis Streams consumer
// groups (XREADGROUP / XACK / XAUTOCLAIM — ADR-002, ADR-004).
type TaskConsumer interface {
	// Consume starts a background polling loop and returns a channel from which
	// the caller receives tasks. The channel is closed when ctx is cancelled.
	//
	// The implementation calls XREADGROUP across all four priority Streams,
	// unmarshals each message, and sends it to the returned channel. The
	// channel is buffered to cfg.WorkerPoolSize to decouple reading from
	// processing (ADR-004 backpressure).
	//
	// The caller must call either Acknowledge or Nack for every received task;
	// failing to do so leaks the message into the Redis PEL indefinitely.
	Consume(ctx context.Context) (<-chan *domain.EmailTask, error)

	// Acknowledge sends XACK for the task's Redis Stream message ID, removing
	// it from the consumer group's PEL. Call this after successful processing
	// or after routing the task to the DLQ (even on failure, to prevent
	// infinite PEL growth).
	Acknowledge(ctx context.Context, task *domain.EmailTask) error

	// Nack explicitly does NOT acknowledge the task. The message remains in the
	// PEL and will be redelivered after the idle threshold expires and
	// ClaimStale is called. Use this when the worker is shutting down mid-task
	// or when an unrecoverable error prevents safe acknowledgement.
	Nack(ctx context.Context, task *domain.EmailTask, reason error) error

	// ClaimStale uses XAUTOCLAIM to reassign messages that have been in the PEL
	// longer than idleThreshold without acknowledgement — typically because the
	// worker that claimed them crashed. The claimed tasks are returned so the
	// supervisor can re-dispatch them (ADR-004).
	ClaimStale(ctx context.Context, idleThreshold time.Duration) ([]*domain.EmailTask, error)
}
