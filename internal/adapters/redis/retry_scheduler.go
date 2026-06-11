package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/DarioJejer/go-email-queue/internal/config"
	"github.com/DarioJejer/go-email-queue/internal/domain"
	"github.com/DarioJejer/go-email-queue/internal/observability"
	"github.com/DarioJejer/go-email-queue/internal/ports"
)

// retryDelayedKey is the Redis sorted-set key for the retry delay queue.
// Score = Unix timestamp of the earliest eligible re-enqueue time (ADR-005).
const retryDelayedKey = "queue:email:retry:delayed"

// retryPoisonKey is the Redis LIST that holds unparseable retry sorted-set
// members quarantined by FlushReady. Operators inspect via LRANGE; entries
// share the DLQ TTL (ADR-005).
const retryPoisonKey = "queue:retry:poison"

// retryPoisonEntry is the JSON envelope written to retryPoisonKey for each
// corrupt sorted-set member so ops can diagnose the raw payload.
type retryPoisonEntry struct {
	RawMember     string    `json:"raw_member"`
	ParseError    string    `json:"parse_error"`
	QuarantinedAt time.Time `json:"quarantined_at"`
	SourceKey     string    `json:"source_key"`
}

// RedisRetryScheduler implements ports.RetryScheduler using a Redis sorted
// set as a delay queue (ADR-005). Tasks are persisted with score = Unix
// timestamp of the earliest eligible re-enqueue time. FlushReady moves ready
// tasks into their priority streams via a single pipeline round-trip.
type RedisRetryScheduler struct {
	client  *goredis.Client
	cfg     *config.Config
	metrics ports.MetricsRecorder
}

// Compile-time interface satisfaction check.
var _ ports.RetryScheduler = (*RedisRetryScheduler)(nil)

// NewRedisRetryScheduler returns a production-ready RedisRetryScheduler.
func NewRedisRetryScheduler(
	client *goredis.Client,
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
	if err := s.client.ZAddArgs(ctx, retryDelayedKey, goredis.ZAddArgs{
		NX:      true,
		Members: []goredis.Z{{Score: float64(scheduledFor.Unix()), Member: string(payload)}},
	}).Err(); err != nil {
		return fmt.Errorf("retry scheduler: ZADD task %s: %w", task.ID, err)
	}

	logger := observability.LoggerFromContext(ctx)
	logger.Info().
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

	members, err := s.client.ZRangeByScore(ctx, retryDelayedKey, &goredis.ZRangeBy{
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

	// Unmarshal all members; quarantine any that cannot be parsed.
	tasks := make([]*domain.EmailTask, 0, len(members))
	validMembers := make([]interface{}, 0, len(members))
	corruptMembers := make([]string, 0)
	corruptErrors := make([]error, 0)
	for _, m := range members {
		var t domain.EmailTask
		if jsonErr := json.Unmarshal([]byte(m), &t); jsonErr != nil {
			logger.Error().Err(jsonErr).
				Msg("retry scheduler: corrupt sorted-set entry -- quarantining")
			corruptMembers = append(corruptMembers, m)
			corruptErrors = append(corruptErrors, jsonErr)
			continue
		}
		tasks = append(tasks, &t)
		validMembers = append(validMembers, m)
	}
	if len(corruptMembers) > 0 {
		s.quarantineCorruptMembers(ctx, corruptMembers, corruptErrors)
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
	xaddCmds := make([]*goredis.StringCmd, 0, len(tasks))
	xaddTasks := make([]*domain.EmailTask, 0, len(tasks))
	for _, t := range tasks {
		payload, marshalErr := MarshalTaskPayload(t)
		if marshalErr != nil {
			logger.Error().Err(marshalErr).Str("task_id", t.ID).
				Msg("retry scheduler: marshal failed -- skipping XADD")
			continue
		}
		xaddCmds = append(xaddCmds, pipe.XAdd(ctx, &goredis.XAddArgs{
			Stream: t.Priority.QueueName(),
			Values: XAddValuesBuilder(t, payload),
		}))
		xaddTasks = append(xaddTasks, t)
	}
	zremCmd := pipe.ZRem(ctx, retryDelayedKey, validMembers...)
	if _, execErr := pipe.Exec(ctx); execErr != nil {
		logger.Error().Err(execErr).
			Int("xadd_count", len(xaddCmds)).
			Int("zrem_count", len(validMembers)).
			Msg("retry scheduler: flush pipeline failed")
	}

	// Report per-XADD failures and build the flushed list.
	flushed := make([]*domain.EmailTask, 0, len(xaddTasks))
	for i, cmd := range xaddCmds {
		if cmd.Err() != nil {
			logger.Error().Err(cmd.Err()).Str("task_id", xaddTasks[i].ID).
				Msg("retry scheduler: XADD failed -- task will retry on next flush tick")
			continue
		}
		flushed = append(flushed, xaddTasks[i])
	}
	if zremErr := zremCmd.Err(); zremErr != nil {
		logger.Error().Err(zremErr).Int("count", len(validMembers)).
			Msg("retry scheduler: ZRem failed -- flushed tasks may reappear on next tick")
	}

	depth, depthErr := s.client.ZCard(ctx, retryDelayedKey).Result()
	if depthErr != nil {
		logger.Warn().Err(depthErr).Msg("retry scheduler: ZCARD failed -- queue depth gauge not updated")
	} else {
		s.metrics.RecordQueueDepth("retry:delayed", float64(depth))
	}

	if len(flushed) > 0 {
		logger.Debug().Int("count", len(flushed)).Msg("retry scheduler: flushed ready tasks")
	}
	return flushed, nil
}

// quarantineCorruptMembers RPUSHes each corrupt member to the poison list and
// ZREMoves it from the retry delay sorted set in a single pipeline round-trip.
// Unrecoverable entries must not remain in the delay queue — they would be
// re-read every scheduler tick and can block the flush batch limit.
func (s *RedisRetryScheduler) quarantineCorruptMembers(
	ctx context.Context,
	members []string,
	parseErrors []error,
) {
	logger := observability.LoggerFromContext(ctx)
	pipe := s.client.Pipeline()

	quarantinedAt := time.Now().UTC()
	toRemove := make([]interface{}, 0, len(members))
	for i, m := range members {
		entry, err := json.Marshal(retryPoisonEntry{
			RawMember:     m,
			ParseError:    parseErrors[i].Error(),
			QuarantinedAt: quarantinedAt,
			SourceKey:     retryDelayedKey,
		})
		if err != nil {
			logger.Error().Err(err).Msg("retry scheduler: marshal poison entry failed")
			continue
		}
		pipe.RPush(ctx, retryPoisonKey, string(entry))
		toRemove = append(toRemove, m)
	}
	if len(toRemove) == 0 {
		return
	}

	zremCmd := pipe.ZRem(ctx, retryDelayedKey, toRemove...)
	// Refresh the poison-list TTL on every quarantine so the 7-day inspection
	// window slides forward while corrupt entries are still being discovered.
	if s.cfg.Retry.DLQTTLSeconds > 0 {
		pipe.Expire(ctx, retryPoisonKey, time.Duration(s.cfg.Retry.DLQTTLSeconds)*time.Second)
	}

	// A pipeline-level failure means we cannot tell which commands ran; the
	// corrupt members may remain in the delay set and will be retried next tick.
	if _, err := pipe.Exec(ctx); err != nil {
		logger.Error().Err(err).Int("count", len(members)).
			Msg("retry scheduler: failed to quarantine corrupt entries")
		return
	}
	// Exec can succeed while an individual command fails (e.g. member already
	// removed). Without this check corrupt entries could be RPUSH'd but not
	// ZREM'd, causing duplicate poison records on every flush tick.
	if zremErr := zremCmd.Err(); zremErr != nil {
		logger.Error().Err(zremErr).Int("count", len(members)).
			Msg("retry scheduler: ZREM failed after poison RPUSH")
		return
	}

	logger.Warn().Int("count", len(members)).
		Str("poison_key", retryPoisonKey).
		Msg("retry scheduler: corrupt entries quarantined")
}
