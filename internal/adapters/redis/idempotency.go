package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/DarioJejer/go-email-queue/internal/config"
	"github.com/DarioJejer/go-email-queue/internal/observability"
	"github.com/DarioJejer/go-email-queue/internal/ports"
)

// idempotencyKeyPrefix is prepended to every task-scoped idempotency key.
// The full key is "idempotency:{taskID}" where taskID is already
// attempt-scoped by the supervisor: "{uuid}#{attempt}".
const idempotencyKeyPrefix = "idempotency:"

// idempotencyKey formats the Redis key for taskID.
func idempotencyKey(taskID string) string {
	return idempotencyKeyPrefix + taskID
}

// idempotencyRecord is the JSON body stored at every idempotency key.
// started_at is stored as a Unix timestamp string so the reclaimStaleLuaScript
// can call tonumber() on it without ISO-8601 parsing.
type idempotencyRecord struct {
	Status      string `json:"status"`
	WorkerID    string `json:"worker_id,omitempty"`
	StartedAt   string `json:"started_at,omitempty"`   // Unix timestamp (string)
	CompletedAt string `json:"completed_at,omitempty"` // RFC3339
}

// RedisIdempotencyStore implements ports.IdempotencyStore using an atomic
// Lua CAS script (ADR-006) to prevent duplicate email delivery.
type RedisIdempotencyStore struct {
	client  *goredis.Client
	scripts *ScriptRegistry
	cfg     *config.Config
}

// Compile-time interface satisfaction check.
var _ ports.IdempotencyStore = (*RedisIdempotencyStore)(nil)

// NewRedisIdempotencyStore constructs a production-ready RedisIdempotencyStore.
// scripts must be the registry returned by LoadScripts.
func NewRedisIdempotencyStore(
	client *goredis.Client,
	scripts *ScriptRegistry,
	cfg *config.Config,
) *RedisIdempotencyStore {
	return &RedisIdempotencyStore{client: client, scripts: scripts, cfg: cfg}
}

// SetProcessing atomically acquires the processing lock for taskID via the
// idempotency Lua CAS script. Returns acquired=true only if this caller is
// the first to claim the key (or the key has expired).
//
// The lock stores workerID and startedAt (Unix timestamp) for operational
// debugging and stale-lock reclaim.
func (s *RedisIdempotencyStore) SetProcessing(ctx context.Context, taskID, workerID string) (bool, error) {
	key := idempotencyKey(taskID)
	ttl := strconv.Itoa(s.cfg.Retry.DLQTTLSeconds)
	startedAt := strconv.FormatInt(time.Now().Unix(), 10)

	result, err := s.client.EvalSha(ctx, s.scripts.IdempotencySHA,
		[]string{key}, workerID, ttl, startedAt).Int64()
	if err != nil && !errors.Is(err, goredis.Nil) {
		return false, fmt.Errorf("idempotency: SetProcessing %s: %w", taskID, err)
	}

	acquired := result == 1
	if !acquired {
		logger := observability.LoggerFromContext(ctx)
		logger.Debug().
			Str("task_id", taskID).
			Msg("idempotency: lock not acquired — duplicate delivery detected")
	}
	return acquired, nil
}

// SetCompleted transitions the idempotency key from "processing" to
// "completed" using SET XX EX so it only updates an existing key.
// If the key has expired mid-flight (XX no-op), a warning is logged and
// nil is returned — the deduplication window has passed.
func (s *RedisIdempotencyStore) SetCompleted(ctx context.Context, taskID string) error {
	key := idempotencyKey(taskID)
	ttl := time.Duration(s.cfg.Retry.DLQTTLSeconds) * time.Second

	rec := idempotencyRecord{
		Status:      "completed",
		CompletedAt: time.Now().UTC().Format(time.RFC3339),
	}
	value, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("idempotency: SetCompleted %s: marshal: %w", taskID, err)
	}

	err = s.client.SetArgs(ctx, key, string(value), goredis.SetArgs{
		Mode: "XX",
		TTL:  ttl,
	}).Err()
	if errors.Is(err, goredis.Nil) {
		// XX condition not met — key expired while the task was in-flight.
		// A new delivery of the same attempt would have acquired a fresh lock,
		// so we must not overwrite it. Log and return nil.
		logger := observability.LoggerFromContext(ctx)
		logger.Warn().
			Str("task_id", taskID).
			Msg("idempotency: SetCompleted XX no-op — key expired mid-flight")
		return nil
	}
	if err != nil {
		return fmt.Errorf("idempotency: SetCompleted %s: %w", taskID, err)
	}
	return nil
}

// IsCompleted returns true only when the stored status is "completed".
// Returns false for unknown keys (TTL expired) or keys in "processing" state.
func (s *RedisIdempotencyStore) IsCompleted(ctx context.Context, taskID string) (bool, error) {
	raw, err := s.client.Get(ctx, idempotencyKey(taskID)).Bytes()
	if errors.Is(err, goredis.Nil) {
		return false, nil // key expired or never set
	}
	if err != nil {
		return false, fmt.Errorf("idempotency: IsCompleted %s: %w", taskID, err)
	}

	var rec idempotencyRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		return false, fmt.Errorf("idempotency: IsCompleted %s: unmarshal: %w", taskID, err)
	}
	return rec.Status == "completed", nil
}

// ClearProcessing deletes the processing lock for taskID. Called by the
// worker before Nacking when a downstream write (ScheduleRetry/SendToDLQ)
// fails, so the PEL reclaim can re-acquire the lock on the next delivery.
//
// Safe to call on a missing or already-expired key — DEL on a missing key
// is a no-op and returns 0, not an error.
func (s *RedisIdempotencyStore) ClearProcessing(ctx context.Context, taskID string) error {
	if err := s.client.Del(ctx, idempotencyKey(taskID)).Err(); err != nil {
		return fmt.Errorf("idempotency: ClearProcessing %s: %w", taskID, err)
	}
	return nil
}

// TryReclaimStale atomically re-acquires a processing lock that was set
// longer than maxAge ago via the reclaim Lua script. This covers the case
// where the original worker crashed after SetProcessing but before
// ClearProcessing, leaving the lock stranded in the sorted set.
//
// Returns reclaimed=true if the stale lock was successfully re-assigned.
// Returns reclaimed=false if the lock is absent, non-processing, or recent.
func (s *RedisIdempotencyStore) TryReclaimStale(ctx context.Context, taskID, workerID string, maxAge time.Duration) (bool, error) {
	key := idempotencyKey(taskID)
	now := time.Now()
	ttl := strconv.Itoa(s.cfg.Retry.DLQTTLSeconds)
	newStartedAt := strconv.FormatInt(now.Unix(), 10)
	maxAgeSeconds := strconv.FormatInt(int64(maxAge.Seconds()), 10)
	nowUnix := strconv.FormatInt(now.Unix(), 10)

	result, err := s.client.EvalSha(ctx, s.scripts.ReclaimStaleSHA,
		[]string{key}, workerID, ttl, newStartedAt, maxAgeSeconds, nowUnix).Int64()
	if err != nil && !errors.Is(err, goredis.Nil) {
		return false, fmt.Errorf("idempotency: TryReclaimStale %s: %w", taskID, err)
	}

	reclaimed := result == 1
	if reclaimed {
		logger := observability.LoggerFromContext(ctx)
		logger.Warn().
			Str("task_id", taskID).
			Str("worker_id", workerID).
			Msg("idempotency: stale lock reclaimed")
	}
	return reclaimed, nil
}
