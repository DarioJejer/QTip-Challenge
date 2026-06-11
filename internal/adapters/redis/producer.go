package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/DarioJejer/go-email-queue/internal/config"
	"github.com/DarioJejer/go-email-queue/internal/domain"
	"github.com/DarioJejer/go-email-queue/internal/observability"
	"github.com/DarioJejer/go-email-queue/internal/ports"
)

// delayedQueueKey is the Redis sorted-set key used for future-scheduled tasks.
// Score = Unix timestamp of the desired execution time (ADR-005).
const delayedQueueKey = "queue:email:delayed"

// RedisProducer implements ports.TaskProducer using Redis Streams (ADR-002).
// All writes go through a *redis.Client injected at construction time; no
// global Redis state is used.
type RedisProducer struct {
	client  *redis.Client
	cfg     *config.Config
	metrics ports.MetricsRecorder
	tracer  trace.Tracer
}

// Compile-time interface satisfaction check.
var _ ports.TaskProducer = (*RedisProducer)(nil)

// NewRedisProducer constructs a production-grade Redis Streams producer.
// The tracer may be a noop tracer when OTEL is disabled (ADR-007).
func NewRedisProducer(
	client *redis.Client,
	cfg *config.Config,
	metrics ports.MetricsRecorder,
	tracer trace.Tracer,
) *RedisProducer {
	return &RedisProducer{
		client:  client,
		cfg:     cfg,
		metrics: metrics,
		tracer:  tracer,
	}
}

// Enqueue validates task, writes it to the appropriate priority Redis Stream
// via XADD, creates an OTel span, records enqueue metrics, and logs at INFO.
//
// The trace and span IDs of the producer span are stored on the task so the
// consuming worker can continue the distributed trace end-to-end (ADR-007).
//
// On context cancellation the method returns ctx.Err() without touching Redis.
// All Redis errors are wrapped with the task ID for traceability.
func (p *RedisProducer) Enqueue(ctx context.Context, task *domain.EmailTask) error {
	if err := task.Validate(); err != nil {
		return err
	}

	ctx, span := p.tracer.Start(ctx, "producer.enqueue",
		trace.WithAttributes(
			attribute.String("task.id", task.ID),
			attribute.String("task.tenant_id", task.TenantID),
			attribute.String("task.type", string(task.Type)),
			attribute.String("task.priority", task.Priority.String()),
		),
	)
	defer span.End()

	if ctx.Err() != nil {
		return ctx.Err()
	}

	// Propagate trace context through the queue for end-to-end tracing.
	if sc := span.SpanContext(); sc.IsValid() {
		task.TraceID = sc.TraceID().String()
		task.SpanID = sc.SpanID().String()
	}

	payload, err := MarshalTaskPayload(task)
	if err != nil {
		return fmt.Errorf("producer: %w", err)
	}

	streamKey := task.Priority.QueueName()

	if err := p.client.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey,
		ID:     "*",
		Values: XAddValuesBuilder(task, payload),
	}).Err(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("producer: enqueue task %s: %w", task.ID, err)
	}

	p.metrics.RecordEnqueued(task.TenantID, string(task.Type), task.Priority.String())

	logger := observability.LoggerFromContext(ctx)
	logger.Info().
		Str("task_id", task.ID).
		Str("tenant_id", task.TenantID).
		Str("task_type", string(task.Type)).
		Str("priority", task.Priority.String()).
		Str("stream", streamKey).
		Msg("task.enqueued")

	return nil
}

// EnqueueBatch publishes multiple tasks in a single Redis pipeline round-trip.
// All tasks are validated before the pipeline opens; any validation failure
// aborts the entire batch and returns a *domain.MultiError.
//
// On partial Redis pipeline failure the method returns a *domain.MultiError
// listing every failed task ID. Tasks that were written successfully are NOT
// rolled back — the caller is responsible for deduplication via the
// IdempotencyStore (ADR-006).
func (p *RedisProducer) EnqueueBatch(ctx context.Context, tasks []*domain.EmailTask) error {
	if len(tasks) == 0 {
		return nil
	}

	// Validate all tasks before touching Redis so a trivially invalid batch
	// does not produce a partial write.
	var valErrs []error
	for _, t := range tasks {
		if err := t.Validate(); err != nil {
			valErrs = append(valErrs, fmt.Errorf("task %s: %w", t.ID, err))
		}
	}
	if len(valErrs) > 0 {
		return &domain.MultiError{Errors: valErrs}
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	// Marshal all payloads before opening the pipeline to avoid holding the
	// connection open during allocation-heavy JSON encoding.
	payloads := make([]string, len(tasks))
	for i, task := range tasks {
		payload, err := MarshalTaskPayload(task)
		if err != nil {
			return fmt.Errorf("producer: %w", err)
		}
		payloads[i] = payload
	}

	pipe := p.client.Pipeline()
	for i, task := range tasks {
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: task.Priority.QueueName(),
			ID:     "*",
			Values: XAddValuesBuilder(task, payloads[i]),
		})
	}

	results, err := pipe.Exec(ctx)
	if err != nil && ctx.Err() != nil {
		return ctx.Err()
	}

	var execErrs []error
	for i, res := range results {
		if res.Err() != nil {
			execErrs = append(execErrs, fmt.Errorf("task %s: %w", tasks[i].ID, res.Err()))
		}
	}
	if len(execErrs) > 0 {
		return &domain.MultiError{Errors: execErrs}
	}

	for _, task := range tasks {
		p.metrics.RecordEnqueued(task.TenantID, string(task.Type), task.Priority.String())
	}

	logger := observability.LoggerFromContext(ctx)
	logger.Info().
		Int("batch_size", len(tasks)).
		Msg("task.batch_enqueued")

	return nil
}

// EnqueueDelayed schedules a task for future delivery by writing it to the
// delayed sorted set with score = Unix timestamp of the scheduled time.
// The DelayedScheduler moves it to the main priority Stream once
// time.Now() >= *task.ScheduledFor (ADR-005).
func (p *RedisProducer) EnqueueDelayed(ctx context.Context, task *domain.EmailTask, delay time.Duration) error {
	if err := task.Validate(); err != nil {
		return err
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	scheduledTime := time.Now().Add(delay)
	task.ScheduledFor = &scheduledTime

	payload, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("producer: marshal task %s: %w", task.ID, err)
	}

	if err := p.client.ZAdd(ctx, delayedQueueKey, redis.Z{
		Score:  float64(scheduledTime.Unix()),
		Member: string(payload),
	}).Err(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("producer: enqueue delayed task %s: %w", task.ID, err)
	}

	logger := observability.LoggerFromContext(ctx)
	logger.Info().
		Str("task_id", task.ID).
		Str("tenant_id", task.TenantID).
		Str("task_type", string(task.Type)).
		Time("scheduled_for", scheduledTime).
		Msg("task.scheduled_delayed")

	return nil
}
