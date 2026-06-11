package redis_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	redisadapter "github.com/DarioJejer/go-email-queue/internal/adapters/redis"
	"github.com/DarioJejer/go-email-queue/internal/config"
	"github.com/DarioJejer/go-email-queue/internal/domain"
)

// testRetryDelayedKey mirrors the unexported constant in the adapter.
const testRetryDelayedKey = "queue:email:retry:delayed"

// testRetryPoisonKey mirrors the unexported poison-list constant in the adapter.
const testRetryPoisonKey = "queue:retry:poison"

// ---------------------------------------------------------------------------
// Test environment
// ---------------------------------------------------------------------------

type retryTestEnv struct {
	scheduler *redisadapter.RedisRetryScheduler
	client    *redis.Client
	mr        *miniredis.Miniredis
}

func newRetryTestEnv(t *testing.T) *retryTestEnv {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	s := redisadapter.NewRedisRetryScheduler(client, &config.Config{}, &noopMetrics{})
	return &retryTestEnv{scheduler: s, client: client, mr: mr}
}

// validRetryTask builds a minimal EmailTask suitable for retry scheduling.
func validRetryTask(id string, attempt int) *domain.EmailTask {
	return &domain.EmailTask{
		ID:          id,
		TenantID:    "tenant-1",
		Type:        domain.TaskTypeTransactional,
		Priority:    domain.PriorityNormal,
		Recipient:   "user@example.com",
		TemplateID:  "welcome",
		EnqueuedAt:  time.Now(),
		Attempt:     attempt,
		MaxAttempts: 3,
		Status:      domain.StatusFailed,
		Metadata:    map[string]string{},
	}
}

// ---------------------------------------------------------------------------
// ScheduleRetry tests
// ---------------------------------------------------------------------------

// TestScheduleRetry_AddsToSortedSet verifies that ScheduleRetry persists
// exactly one entry in the retry sorted set.
func TestScheduleRetry_AddsToSortedSet(t *testing.T) {
	env := newRetryTestEnv(t)
	ctx := context.Background()
	task := validRetryTask("retry-01", 1)

	require.NoError(t, env.scheduler.ScheduleRetry(ctx, task, 5*time.Minute))

	assert.Equal(t, int64(1), zcard(t, env.client, testRetryDelayedKey),
		"ScheduleRetry must add exactly one entry to the sorted set")
}

// TestScheduleRetry_Idempotent verifies that calling ScheduleRetry twice with
// the same task JSON (ZADD NX) results in only one sorted-set entry.
func TestScheduleRetry_Idempotent(t *testing.T) {
	env := newRetryTestEnv(t)
	ctx := context.Background()
	task := validRetryTask("retry-idem-01", 1)

	require.NoError(t, env.scheduler.ScheduleRetry(ctx, task, 5*time.Minute))
	require.NoError(t, env.scheduler.ScheduleRetry(ctx, task, 5*time.Minute))

	assert.Equal(t, int64(1), zcard(t, env.client, testRetryDelayedKey),
		"duplicate ScheduleRetry call must be a no-op (NX flag)")
}

// ---------------------------------------------------------------------------
// FlushReady tests
// ---------------------------------------------------------------------------

// TestFlushReady_MovesReadyTasksToMainQueue verifies that tasks whose score
// is in the past are removed from the sorted set and written to their
// priority Redis Stream.
func TestFlushReady_MovesReadyTasksToMainQueue(t *testing.T) {
	env := newRetryTestEnv(t)
	ctx := context.Background()
	task := validRetryTask("flush-ready-01", 1)

	// Seed with a score 1 minute in the past -- ready immediately.
	payload, err := json.Marshal(task)
	require.NoError(t, err)
	require.NoError(t, env.client.ZAdd(ctx, testRetryDelayedKey, redis.Z{
		Score:  float64(time.Now().Add(-time.Minute).Unix()),
		Member: string(payload),
	}).Err())

	flushed, err := env.scheduler.FlushReady(ctx)
	require.NoError(t, err)
	assert.Len(t, flushed, 1, "one task must be flushed")

	// Entry must be removed from the sorted set.
	assert.Equal(t, int64(0), zcard(t, env.client, testRetryDelayedKey),
		"sorted set must be empty after flush")

	// Task must appear in the priority stream.
	assert.Equal(t, int64(1), xlen(t, env.client, task.Priority.QueueName()),
		"flushed task must be XADD'd to its priority stream")
}

// TestFlushReady_LeavesNotReadyTasks verifies that tasks with a score in the
// future are left untouched in the sorted set.
func TestFlushReady_LeavesNotReadyTasks(t *testing.T) {
	env := newRetryTestEnv(t)
	ctx := context.Background()
	task := validRetryTask("not-ready-01", 1)

	// Seed with a score 1 hour in the future -- not yet ready.
	payload, err := json.Marshal(task)
	require.NoError(t, err)
	require.NoError(t, env.client.ZAdd(ctx, testRetryDelayedKey, redis.Z{
		Score:  float64(time.Now().Add(time.Hour).Unix()),
		Member: string(payload),
	}).Err())

	flushed, err := env.scheduler.FlushReady(ctx)
	require.NoError(t, err)
	assert.Empty(t, flushed, "no tasks must be flushed when none are ready")

	assert.Equal(t, int64(1), zcard(t, env.client, testRetryDelayedKey),
		"future task must remain in sorted set")
}

// TestFlushReady_QuarantinesCorruptMembers verifies that unparseable sorted-set
// members are ZREM'd from the delay queue and RPUSH'd to the poison list.
func TestFlushReady_QuarantinesCorruptMembers(t *testing.T) {
	env := newRetryTestEnv(t)
	ctx := context.Background()

	require.NoError(t, env.client.ZAdd(ctx, testRetryDelayedKey, redis.Z{
		Score:  float64(time.Now().Add(-time.Minute).Unix()),
		Member: "not-valid-json{{{",
	}).Err())

	flushed, err := env.scheduler.FlushReady(ctx)
	require.NoError(t, err)
	assert.Empty(t, flushed, "corrupt entry must not be flushed to a stream")

	assert.Equal(t, int64(0), zcard(t, env.client, testRetryDelayedKey),
		"corrupt member must be removed from the delay sorted set")

	poisonLen, err := env.client.LLen(ctx, testRetryPoisonKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), poisonLen, "corrupt member must be appended to poison list")

	raw, err := env.client.LIndex(ctx, testRetryPoisonKey, 0).Result()
	require.NoError(t, err)

	var entry struct {
		RawMember  string `json:"raw_member"`
		ParseError string `json:"parse_error"`
		SourceKey  string `json:"source_key"`
	}
	require.NoError(t, json.Unmarshal([]byte(raw), &entry))
	assert.Equal(t, "not-valid-json{{{", entry.RawMember)
	assert.NotEmpty(t, entry.ParseError)
	assert.Equal(t, testRetryDelayedKey, entry.SourceKey)
}

// TestFlushReady_QuarantinesCorruptAndFlushesValid verifies that a batch
// containing both corrupt and valid members processes each appropriately.
func TestFlushReady_QuarantinesCorruptAndFlushesValid(t *testing.T) {
	env := newRetryTestEnv(t)
	ctx := context.Background()
	task := validRetryTask("flush-mixed-01", 1)
	validPayload, err := json.Marshal(task)
	require.NoError(t, err)

	pipe := env.client.Pipeline()
	pipe.ZAdd(ctx, testRetryDelayedKey, redis.Z{
		Score:  float64(time.Now().Add(-time.Minute).Unix()),
		Member: "corrupt{{{",
	})
	pipe.ZAdd(ctx, testRetryDelayedKey, redis.Z{
		Score:  float64(time.Now().Add(-time.Minute).Unix()),
		Member: string(validPayload),
	})
	_, err = pipe.Exec(ctx)
	require.NoError(t, err)

	flushed, err := env.scheduler.FlushReady(ctx)
	require.NoError(t, err)
	require.Len(t, flushed, 1)
	assert.Equal(t, task.ID, flushed[0].ID)

	assert.Equal(t, int64(0), zcard(t, env.client, testRetryDelayedKey))
	assert.Equal(t, int64(1), xlen(t, env.client, task.Priority.QueueName()))

	poisonLen, err := env.client.LLen(ctx, testRetryPoisonKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), poisonLen)
}
