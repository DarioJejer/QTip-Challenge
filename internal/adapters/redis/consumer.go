package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/trace"

	"github.com/DarioJejer/go-email-queue/internal/config"
	"github.com/DarioJejer/go-email-queue/internal/domain"
	"github.com/DarioJejer/go-email-queue/internal/observability"
	"github.com/DarioJejer/go-email-queue/internal/ports"
)

// priorityStreams lists all four Redis Stream keys in descending priority
// order (critical → low). The order is used both for consumer-group
// initialisation and for the STREAMS argument to XREADGROUP so that
// higher-priority messages are returned first when multiple queues are
// non-empty (ADR-002).
var priorityStreams = []string{
	domain.PriorityCritical.QueueName(),
	domain.PriorityHigh.QueueName(),
	domain.PriorityNormal.QueueName(),
	domain.PriorityLow.QueueName(),
}

// Internal Metadata keys stamped on tasks during consumption so that
// Acknowledge can route the XACK to the correct stream and message ID.
// Keys are prefixed with "_" (underscore) to signal they are internal
// infrastructure fields that must not be persisted or forwarded.
const (
	metaStreamID  = "_stream_id"
	metaStreamKey = "_stream_key"
)

// claimBatchSize is the maximum number of stale PEL entries reclaimed in a
// single XAUTOCLAIM call (ADR-004).
const claimBatchSize = 100

// RedisConsumer implements ports.TaskConsumer using Redis Streams consumer
// groups (XREADGROUP / XACK / XAUTOCLAIM — ADR-002, ADR-004).
type RedisConsumer struct {
	client  *goredis.Client
	cfg     *config.Config
	metrics ports.MetricsRecorder
	tracer  trace.Tracer
}

// Compile-time interface satisfaction check.
var _ ports.TaskConsumer = (*RedisConsumer)(nil)

// NewRedisConsumer constructs a production-grade Redis Streams consumer.
// The tracer may be a noop tracer when OTEL is disabled (ADR-007).
func NewRedisConsumer(
	client *goredis.Client,
	cfg *config.Config,
	metrics ports.MetricsRecorder,
	tracer trace.Tracer,
) *RedisConsumer {
	return &RedisConsumer{
		client:  client,
		cfg:     cfg,
		metrics: metrics,
		tracer:  tracer,
	}
}

// ensureConsumerGroup creates the consumer group for streamKey if it does not
// already exist. The MKSTREAM flag ensures the stream key is created even
// before the first producer message, so workers can start polling immediately.
//
// BUSYGROUP errors are silently ignored — they mean the group already exists,
// which is the desired idempotent outcome (ADR-002).
func (c *RedisConsumer) ensureConsumerGroup(ctx context.Context, streamKey, group string) error {
	err := c.client.XGroupCreateMkStream(ctx, streamKey, group, "$").Err()
	if err != nil && !isBusyGroup(err) {
		return fmt.Errorf("consumer: create group %q on %q: %w", group, streamKey, err)
	}
	return nil
}

// isBusyGroup reports whether err is the Redis BUSYGROUP error, which means
// the consumer group already exists.
func isBusyGroup(err error) bool {
	return err != nil && strings.Contains(err.Error(), "BUSYGROUP")
}

// isNoGroup reports whether err is the Redis NOGROUP error, which means the
// consumer group was deleted (e.g. after a Redis restart that lost data).
func isNoGroup(err error) bool {
	return err != nil && strings.Contains(err.Error(), "NOGROUP")
}

// Consume initialises consumer groups on all four priority streams, then
// launches a background goroutine that continuously calls XREADGROUP and
// forwards deserialised tasks to the returned channel.
//
// The channel is buffered to cfg.Worker.PoolSize to decouple reading from
// processing and provide backpressure (ADR-004). The goroutine exits and
// closes the channel when ctx is cancelled.
func (c *RedisConsumer) Consume(ctx context.Context) (<-chan *domain.EmailTask, error) {
	group := c.cfg.Worker.ConsumerGroup

	// Ensure all four consumer groups exist synchronously before returning the
	// channel so that the first XREADGROUP call never encounters a NOGROUP error.
	for _, stream := range priorityStreams {
		if err := c.ensureConsumerGroup(ctx, stream, group); err != nil {
			return nil, err
		}
	}

	ch := make(chan *domain.EmailTask, c.cfg.Worker.PoolSize)
	go c.pollLoop(ctx, ch)
	return ch, nil
}

// pollLoop is the long-lived goroutine that drives Consume. It calls
// XREADGROUP in a tight loop, forwarding each valid message to ch.
// The loop exits when ctx is cancelled.
func (c *RedisConsumer) pollLoop(ctx context.Context, ch chan<- *domain.EmailTask) {
	defer close(ch)

	group := c.cfg.Worker.ConsumerGroup
	consumer := c.cfg.Worker.ConsumerName

	// Build the STREAMS slice: all 4 keys followed by 4 ">" IDs.
	// ">" instructs XREADGROUP to deliver only new (not yet delivered) messages.
	streamArgs := make([]string, len(priorityStreams)*2)
	for i, s := range priorityStreams {
		streamArgs[i] = s
		streamArgs[len(priorityStreams)+i] = ">"
	}

	for {
		if ctx.Err() != nil {
			return
		}

		streams, err := c.client.XReadGroup(ctx, &goredis.XReadGroupArgs{
			Group:    group,
			Consumer: consumer,
			Streams:   streamArgs,
			Count:    10,
			Block:    c.cfg.Worker.BlockTimeout,
			NoAck:    false,
		}).Result()

		if err != nil {
			// Context cancelled — exit cleanly.
			if ctx.Err() != nil {
				return
			}
			// NOGROUP: the consumer group was lost (Redis restart). Recreate and retry.
			if isNoGroup(err) {
				for _, stream := range priorityStreams {
					_ = c.ensureConsumerGroup(ctx, stream, group)
				}
				continue
			}
			// redis.Nil means BLOCK timed out with no messages — normal, keep looping.
			if errors.Is(err, goredis.Nil) {
				continue
			}
			// Unexpected error — log and keep looping to avoid crashing the pool.
			logger := observability.LoggerFromContext(ctx)
			logger.Error().
				Err(err).
				Msg("consumer: XREADGROUP error")
			continue
		}

		for _, stream := range streams {
			for _, msg := range stream.Messages {
				task, parseErr := taskFromMessage(stream.Stream, msg)
				if parseErr != nil {
					logger := observability.LoggerFromContext(ctx)
					logger.Error().
						Err(parseErr).
						Str("stream", stream.Stream).
						Str("msg_id", msg.ID).
						Msg("consumer: failed to parse message — skipping")
					continue
				}

				select {
				case ch <- task:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

// taskFromMessage deserialises a Redis Stream message into a *domain.EmailTask
// and stamps the internal routing metadata needed for subsequent XACK calls.
func taskFromMessage(streamKey string, msg goredis.XMessage) (*domain.EmailTask, error) {
	payloadRaw, ok := msg.Values["payload"]
	if !ok {
		return nil, fmt.Errorf("message %s missing 'payload' field", msg.ID)
	}
	payloadStr, ok := payloadRaw.(string)
	if !ok {
		return nil, fmt.Errorf("message %s: 'payload' is not a string", msg.ID)
	}

	var task domain.EmailTask
	if err := json.Unmarshal([]byte(payloadStr), &task); err != nil {
		return nil, fmt.Errorf("message %s: unmarshal: %w", msg.ID, err)
	}

	// Stamp routing metadata so Acknowledge knows which stream+ID to XACK.
	if task.Metadata == nil {
		task.Metadata = make(map[string]string)
	}
	task.Metadata[metaStreamID] = msg.ID
	task.Metadata[metaStreamKey] = streamKey

	return &task, nil
}

// Acknowledge sends XACK for the task's Redis Stream message ID, removing it
// from the consumer group's PEL. Must be called after successful processing
// or after routing to the DLQ to prevent infinite PEL growth (ADR-004).
func (c *RedisConsumer) Acknowledge(ctx context.Context, task *domain.EmailTask) error {
	streamKey := task.Metadata[metaStreamKey]
	msgID := task.Metadata[metaStreamID]

	if err := c.client.XAck(ctx, streamKey, c.cfg.Worker.ConsumerGroup, msgID).Err(); err != nil {
		return fmt.Errorf("consumer: XACK stream=%s id=%s: %w", streamKey, msgID, err)
	}

	logger := observability.LoggerFromContext(ctx)
	logger.Debug().
		Str("task_id", task.ID).
		Str("stream", streamKey).
		Str("msg_id", msgID).
		Msg("task.acknowledged")

	return nil
}

// Nack explicitly does NOT acknowledge the task. The message remains in the
// PEL and will be redelivered once ClaimStale reclaims it after the idle
// threshold elapses (ADR-004). Use this when a worker shuts down mid-task.
func (c *RedisConsumer) Nack(ctx context.Context, task *domain.EmailTask, reason error) error {
	logger := observability.LoggerFromContext(ctx)
	logger.Warn().
		Str("task_id", task.ID).
		Str("tenant_id", task.TenantID).
		Str("msg_id", task.Metadata[metaStreamID]).
		Err(reason).
		Msg("task.nacked")

	return nil
}

// ClaimStale calls XAUTOCLAIM on every priority stream and returns tasks whose
// PEL entries have been idle longer than idleThreshold, typically because the
// worker that claimed them crashed before acknowledging (ADR-004).
//
// Typical caller: WorkerSupervisor ticker running every cfg.Worker.ClaimIdleThreshold.
func (c *RedisConsumer) ClaimStale(ctx context.Context, idleThreshold time.Duration) ([]*domain.EmailTask, error) {
	group := c.cfg.Worker.ConsumerGroup
	consumer := c.cfg.Worker.ConsumerName

	var claimed []*domain.EmailTask

	for _, streamKey := range priorityStreams {
		msgs, _, err := c.client.XAutoClaim(ctx, &goredis.XAutoClaimArgs{
			Stream:   streamKey,
			Group:    group,
			Consumer: consumer,
			MinIdle:  idleThreshold,
			Start:    "0-0",
			Count:    claimBatchSize,
		}).Result()

		if err != nil {
			if errors.Is(err, goredis.Nil) {
				continue
			}
			return nil, fmt.Errorf("consumer: XAUTOCLAIM stream=%s: %w", streamKey, err)
		}

		for _, msg := range msgs {
			task, parseErr := taskFromMessage(streamKey, msg)
			if parseErr != nil {
				logger := observability.LoggerFromContext(ctx)
				logger.Error().
					Err(parseErr).
					Str("stream", streamKey).
					Str("msg_id", msg.ID).
					Msg("consumer: ClaimStale: failed to parse claimed message — skipping")
				continue
			}
			claimed = append(claimed, task)
		}
	}

	return claimed, nil
}
