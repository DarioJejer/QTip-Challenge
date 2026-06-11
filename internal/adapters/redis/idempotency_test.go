package redis_test

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	redisadapter "github.com/DarioJejer/go-email-queue/internal/adapters/redis"
	"github.com/DarioJejer/go-email-queue/internal/config"
)

// ---------------------------------------------------------------------------
// Test environment
// ---------------------------------------------------------------------------

type idempotencyTestEnv struct {
	store  *redisadapter.RedisIdempotencyStore
	client *redis.Client
	mr     *miniredis.Miniredis
	cfg    *config.Config
}

func newIdempotencyTestEnv(t *testing.T) *idempotencyTestEnv {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	// Load scripts into miniredis; the returned SHAs are used by the store.
	scripts, err := redisadapter.LoadScripts(context.Background(), client)
	require.NoError(t, err, "LoadScripts must succeed against miniredis")

	cfg := &config.Config{
		Retry: config.RetryConfig{
			DLQTTLSeconds: 3600, // 1 h
		},
	}
	store := redisadapter.NewRedisIdempotencyStore(client, scripts, cfg)
	return &idempotencyTestEnv{store: store, client: client, mr: mr, cfg: cfg}
}

// seedProcessingRecord directly writes an idempotency record with the given
// started_at Unix timestamp, bypassing the Lua CAS script. This is used to
// set up stale-lock scenarios without having to control time.Now().
func seedProcessingRecord(t *testing.T, env *idempotencyTestEnv, taskID, workerID string, startedAtUnix int64) {
	t.Helper()
	type record struct {
		Status    string `json:"status"`
		WorkerID  string `json:"worker_id"`
		StartedAt string `json:"started_at"`
	}
	b, err := json.Marshal(record{
		Status:    "processing",
		WorkerID:  workerID,
		StartedAt: strconv.FormatInt(startedAtUnix, 10),
	})
	require.NoError(t, err)
	key := "idempotency:" + taskID
	require.NoError(t,
		env.client.Set(context.Background(), key, string(b),
			time.Duration(env.cfg.Retry.DLQTTLSeconds)*time.Second).Err())
}

// ---------------------------------------------------------------------------
// SetProcessing tests
// ---------------------------------------------------------------------------

// TestSetProcessing_AcquiresOnce verifies that the first call acquires the
// lock and the second call for the same taskID is rejected.
func TestSetProcessing_AcquiresOnce(t *testing.T) {
	env := newIdempotencyTestEnv(t)
	ctx := context.Background()

	acquired1, err := env.store.SetProcessing(ctx, "task#0", "worker-A")
	require.NoError(t, err)
	assert.True(t, acquired1, "first SetProcessing must acquire the lock")

	acquired2, err := env.store.SetProcessing(ctx, "task#0", "worker-B")
	require.NoError(t, err)
	assert.False(t, acquired2, "second SetProcessing for the same taskID must be rejected")
}

// TestSetProcessing_SkipsCompleted verifies that SetProcessing returns false
// for a task that has already been marked completed.
func TestSetProcessing_SkipsCompleted(t *testing.T) {
	env := newIdempotencyTestEnv(t)
	ctx := context.Background()

	// Acquire and complete a task.
	acquired, err := env.store.SetProcessing(ctx, "completed-task#0", "worker-A")
	require.NoError(t, err)
	require.True(t, acquired)
	require.NoError(t, env.store.SetCompleted(ctx, "completed-task#0"))

	// A new SetProcessing on the same key must be blocked.
	reacquired, err := env.store.SetProcessing(ctx, "completed-task#0", "worker-B")
	require.NoError(t, err)
	assert.False(t, reacquired, "SetProcessing must not acquire a completed key")
}

// ---------------------------------------------------------------------------
// SetCompleted tests
// ---------------------------------------------------------------------------

// TestSetCompleted_MarksComplete verifies that SetCompleted transitions the
// record status from "processing" to "completed" and that IsCompleted returns
// true afterwards.
func TestSetCompleted_MarksComplete(t *testing.T) {
	env := newIdempotencyTestEnv(t)
	ctx := context.Background()

	acquired, err := env.store.SetProcessing(ctx, "mark-complete#0", "worker-A")
	require.NoError(t, err)
	require.True(t, acquired)

	require.NoError(t, env.store.SetCompleted(ctx, "mark-complete#0"))

	completed, err := env.store.IsCompleted(ctx, "mark-complete#0")
	require.NoError(t, err)
	assert.True(t, completed, "IsCompleted must return true after SetCompleted")
}

// TestSetCompleted_XXNoop verifies that SetCompleted on a non-existent key
// (as if the TTL expired) does not create a ghost entry and returns nil.
func TestSetCompleted_XXNoop(t *testing.T) {
	env := newIdempotencyTestEnv(t)
	ctx := context.Background()

	// No prior SetProcessing — key does not exist.
	err := env.store.SetCompleted(ctx, "ghost-task#0")
	assert.NoError(t, err, "SetCompleted on missing key must return nil (XX no-op)")

	// No key must have been created.
	completed, err := env.store.IsCompleted(ctx, "ghost-task#0")
	require.NoError(t, err)
	assert.False(t, completed, "ghost task must not be present after XX no-op")
}

// ---------------------------------------------------------------------------
// IsCompleted tests
// ---------------------------------------------------------------------------

// TestIsCompleted_TrueAfterComplete is covered by TestSetCompleted_MarksComplete.

// TestIsCompleted_FalseForNew verifies that IsCompleted returns false for a
// task that has never been seen.
func TestIsCompleted_FalseForNew(t *testing.T) {
	env := newIdempotencyTestEnv(t)
	ctx := context.Background()

	completed, err := env.store.IsCompleted(ctx, "new-task#0")
	require.NoError(t, err)
	assert.False(t, completed, "IsCompleted must return false for unknown taskID")
}

// TestIsCompleted_FalseForProcessing verifies that IsCompleted returns false
// for a task that is currently in processing state.
func TestIsCompleted_FalseForProcessing(t *testing.T) {
	env := newIdempotencyTestEnv(t)
	ctx := context.Background()

	acquired, err := env.store.SetProcessing(ctx, "proc-task#0", "worker-A")
	require.NoError(t, err)
	require.True(t, acquired)

	completed, err := env.store.IsCompleted(ctx, "proc-task#0")
	require.NoError(t, err)
	assert.False(t, completed, "IsCompleted must return false for a processing task")
}

// ---------------------------------------------------------------------------
// TTL expiry test
// ---------------------------------------------------------------------------

// TestIdempotency_TTLExpiry verifies that after the TTL elapses the lock
// is gone and SetProcessing can acquire it again.
func TestIdempotency_TTLExpiry(t *testing.T) {
	env := newIdempotencyTestEnv(t)
	ctx := context.Background()

	// Use a 1-second TTL for this test.
	shortCfg := &config.Config{Retry: config.RetryConfig{DLQTTLSeconds: 1}}
	scripts, _ := redisadapter.LoadScripts(ctx, env.client)
	shortStore := redisadapter.NewRedisIdempotencyStore(env.client, scripts, shortCfg)

	acquired, err := shortStore.SetProcessing(ctx, "ttl-task#0", "worker-A")
	require.NoError(t, err)
	require.True(t, acquired)

	// Fast-forward miniredis clock past the 1-second TTL.
	env.mr.FastForward(2 * time.Second)

	// Lock must have expired; re-acquisition must succeed.
	reacquired, err := shortStore.SetProcessing(ctx, "ttl-task#0", "worker-B")
	require.NoError(t, err)
	assert.True(t, reacquired, "SetProcessing must succeed after TTL expiry")
}

// ---------------------------------------------------------------------------
// ClearProcessing tests
// ---------------------------------------------------------------------------

// TestClearProcessing_AllowsReacquisition verifies that ClearProcessing
// deletes the lock so the next SetProcessing succeeds.
func TestClearProcessing_AllowsReacquisition(t *testing.T) {
	env := newIdempotencyTestEnv(t)
	ctx := context.Background()

	acquired, err := env.store.SetProcessing(ctx, "clear-task#0", "worker-A")
	require.NoError(t, err)
	require.True(t, acquired)

	require.NoError(t, env.store.ClearProcessing(ctx, "clear-task#0"))

	reacquired, err := env.store.SetProcessing(ctx, "clear-task#0", "worker-B")
	require.NoError(t, err)
	assert.True(t, reacquired, "SetProcessing must succeed after ClearProcessing")
}

// TestClearProcessing_IdempotentOnMissingKey verifies that ClearProcessing
// on a non-existent key (already expired or never set) returns nil.
func TestClearProcessing_IdempotentOnMissingKey(t *testing.T) {
	env := newIdempotencyTestEnv(t)
	assert.NoError(t, env.store.ClearProcessing(context.Background(), "no-such-task#0"))
}

// ---------------------------------------------------------------------------
// TryReclaimStale tests
// ---------------------------------------------------------------------------

// TestTryReclaimStale_ReclaimesStaleLock seeds a processing record with a
// timestamp in the distant past and verifies TryReclaimStale re-acquires it.
func TestTryReclaimStale_ReclaimesStaleLock(t *testing.T) {
	env := newIdempotencyTestEnv(t)
	ctx := context.Background()

	// Seed a "processing" record whose started_at is Unix timestamp 1000
	// (January 1970) — far older than any reasonable maxAge.
	seedProcessingRecord(t, env, "stale-task#0", "old-worker", 1000)

	reclaimed, err := env.store.TryReclaimStale(ctx, "stale-task#0", "new-worker", time.Second)
	require.NoError(t, err)
	assert.True(t, reclaimed, "TryReclaimStale must reclaim a lock set in the distant past")
}

// TestTryReclaimStale_IgnoresActiveLock verifies that a lock set within maxAge
// is NOT reclaimed (the original worker is considered still alive).
func TestTryReclaimStale_IgnoresActiveLock(t *testing.T) {
	env := newIdempotencyTestEnv(t)
	ctx := context.Background()

	// Seed with the current time — lock is brand new.
	seedProcessingRecord(t, env, "active-task#0", "active-worker", time.Now().Unix())

	reclaimed, err := env.store.TryReclaimStale(ctx, "active-task#0", "new-worker", time.Hour)
	require.NoError(t, err)
	assert.False(t, reclaimed, "TryReclaimStale must not reclaim a recently-set lock")
}

// TestTryReclaimStale_MissingKey verifies that TryReclaimStale returns false
// (not an error) when the key does not exist.
func TestTryReclaimStale_MissingKey(t *testing.T) {
	env := newIdempotencyTestEnv(t)
	reclaimed, err := env.store.TryReclaimStale(context.Background(), "no-such#0", "worker", time.Second)
	require.NoError(t, err)
	assert.False(t, reclaimed)
}

// TestTryReclaimStale_CompletedKey verifies that TryReclaimStale returns false
// for a completed task (must not reopen a completed lock).
func TestTryReclaimStale_CompletedKey(t *testing.T) {
	env := newIdempotencyTestEnv(t)
	ctx := context.Background()

	acquired, err := env.store.SetProcessing(ctx, "done-task#0", "worker-A")
	require.NoError(t, err)
	require.True(t, acquired)
	require.NoError(t, env.store.SetCompleted(ctx, "done-task#0"))

	reclaimed, err := env.store.TryReclaimStale(ctx, "done-task#0", "worker-B", time.Second)
	require.NoError(t, err)
	assert.False(t, reclaimed, "TryReclaimStale must not reclaim a completed key")
}
