package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/DarioJejer/go-email-queue/internal/config"
	"github.com/DarioJejer/go-email-queue/internal/domain"
	"github.com/DarioJejer/go-email-queue/internal/observability"
	"github.com/DarioJejer/go-email-queue/internal/ports"
)

// retryDelayedKey is the Redis sorted-set key for the retry delay queue.
// Score = Unix timestamp of the earliest eligible re-enqueue time (ADR-005).
const retryDelayedKey = "queue:email:retry:delayed"

// RedisRetryScheduler implements ports.RetryScheduler using a Redis sorted
// set as a delay queue (ADR-005). Tasks are persisted with score = Unix
// timestamp of the earliest eligible re-enqueue time. FlushReady moves ready
// tasks into their priority streams via a single pipeline round-trip.
type RedisRetryScheduler struct {
	client  *redis.Client
	cfg     *config.Config
	metrics ports.MetricsRecorder
}

// Compile-time interface satisfaction check.
var _ ports.RetryScheduler = (*RedisRetryScheduler)(nil)

// NewRedisRetryScheduler returns a production-ready RedisRetryScheduler.
func NewRedisRetryScheduler(
	client *redis.Client,
	cfg *config.Config,
	metrics ports.MetricsRecorder,
) *RedisRetryScheduler {
	return &RedisRetryScheduler{client: client, cfg: cfg, metrics: metrics}
}

// ScheduleRetry marshals task to JSON and writes it to the retry sorted set
// via ZADD NX with score = time.Now().Add(delay).Unix(). The NX flag makes
// the operation idempotent: calling ScheduleRetry twice with identical task
// state produces only one sorted-set entry.
//
// NOTE: the worker supervisor increments task.Attempt before calling
// ScheduleRetry. This method stores the task as-is and does NOT increment
// the attempt counter again.
func (s *RedisRetryScheduler) ScheduleRetry(ctx context.Context, task *domain.EmailTask, delay time.Duration) error {
	payload, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("retry scheduler: marshal task %s: %w", task.ID, err)
	}

	scheduledFor := time.Now().Add(delay)
	if err := s.client.ZAddArgs(ctx, retryDelayedKey, redis.ZAddArgs{
		NX:      true,
		Members: []redis.Z{{Score: float64(scheduledFor.Unix()), Member: string(payload)}},
	}).Err(); err != nil {
		return fmt.Errorf("retry scheduler: ZADD task %s: %w", task.ID, err)
	}

	observability.LoggerFromContext(ctx).Info().
		Str("task_id", task.ID).
		Str("tenant_id", task.TenantID).
		Int("attempt", task.Attempt).
		Dur("delay", delay).
		Time("scheduled_for", scheduledFor).
		Msg("task.retry_scheduled")

	return nil
}

// FlushReady reads all entries with score <= time.Now().Unix() via
// ZRANGEBYSCORE (up to 100 per call), pipelines an XADD for each into its
// priority stream and a single ZREM to remove them from the sorted set.
// Partial pipeline failures are logged but do not halt processing of
// remaining entries -- tasks left in the set will be re-flushed on the next
// tick, and duplicate deliveries are deduplicated by the idempotency store.
func (s *RedisRetryScheduler) FlushReady(ctx context.Context) ([]*domain.EmailTask, error) {
	logger := observability.LoggerFromContext(ctx)
	now := time.Now().Unix()

	members, err := s.client.ZRangeByScore(ctx, retryDelayedKey, &redis.ZRangeBy{
		Min:   "-inf",
		Max:   strconv.FormatInt(now, 10),
		Count: 100,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("retry scheduler: ZRANGEBYSCORE: %w", err)
	}
	if len(members) == 0 {
		return nil, nil
	}

	// Unmarshal all members; skip and log any corrupt entries.
	tasks := make([]*domain.EmailTask, 0, len(members))
	validMembers := make([]interface{}, 0, len(members))
	for _, m := range members {
		var t domain.EmailTask
		if jsonErr := json.Unmarshal([]byte(m), &t); jsonErr != nil {
			logger.Error().Err(jsonErr).
				Msg("retry scheduler: corrupt sorted-set entry -- skipping")
			continue
		}
		tasks = append(tasks, &t)
		validMembers = append(validMembers, m)
	}
	if len(tasks) == 0 {
		return nil, nil
	}

	// Pipeline all XADDs + a single ZREM in one round-trip.
	// XADD re-enqueues each task into its priority stream so the consumer can
	// pick it up and process it as the next attempt.
	// ZREM removes all processed entries at once; if it fails the entries will
	// be re-flushed on the next tick (safe -- idempotency store deduplicates).
	pipe := s.client.Pipeline()
	xaddCmds := make([]*redis.StringCmd, len(tasks))
	for i, t := range tasks {
		p, _ := json.Marshal(t)
		xaddCmds[i] = pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: t.Priority.QueueName(),
			Values: map[string]any{
				"id":      t.ID,
				"payload": string(p),
				"attempt": t.Attempt,
			},
		})
	}
	zremCmd := pipe.ZRem(ctx, retryDelayedKey, validMembers...)
	_, _ = pipe.Exec(ctx)

	// Report per-XADD failures and build the flushed list.
	flushed := make([]*domain.EmailTask, 0, len(tasks))
	for i, cmd := range xaddCmds {
		if cmd.Err() != nil {
			logger.Error().Err(cmd.Err()).Str("task_id", tasks[i].ID).
				Msg("retry scheduler: XADD failed -- task will retry on next flush tick")
			continue
		}
		flushed = append(flushed, tasks[i])
	}
	if zremErr := zremCmd.Err(); zremErr != nil {
		logger.Error().Err(zremErr).Int("count", len(validMembers)).
			Msg("retry scheduler: ZRem failed -- flushed tasks may reappear on next tick")
	}

	s.metrics.RecordQueueDepth("retry:delayed", float64(len(members)-len(flushed)))

	if len(flushed) > 0 {
		logger.Debug().Int("count", len(flushed)).Msg("retry scheduler: flushed ready tasks")
	}
	return flushed, nil
}
