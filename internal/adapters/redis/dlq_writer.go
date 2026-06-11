package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/DarioJejer/go-email-queue/internal/config"
	"github.com/DarioJejer/go-email-queue/internal/domain"
	"github.com/DarioJejer/go-email-queue/internal/observability"
	"github.com/DarioJejer/go-email-queue/internal/ports"
)

// dlqKeyPrefix is prepended to every tenant-scoped dead-letter list (ADR-005).
const dlqKeyPrefix = "queue:dlq:"

// dlqKey formats the Redis LIST key for a tenant/task-type pair.
func dlqKey(tenantID, taskType string) string {
	return fmt.Sprintf("%s%s:%s", dlqKeyPrefix, tenantID, taskType)
}

// RedisDLQWriter implements ports.DLQWriter using Redis LISTs (ADR-005).
// Entries are append-only; operators inspect via LRANGE or the admin API.
type RedisDLQWriter struct {
	client  *goredis.Client
	cfg     *config.Config
	metrics ports.MetricsRecorder
}

// Compile-time interface satisfaction check.
var _ ports.DLQWriter = (*RedisDLQWriter)(nil)

// NewRedisDLQWriter constructs a production-ready RedisDLQWriter.
func NewRedisDLQWriter(
	client *goredis.Client,
	cfg *config.Config,
	metrics ports.MetricsRecorder,
) *RedisDLQWriter {
	return &RedisDLQWriter{client: client, cfg: cfg, metrics: metrics}
}

// SendToDLQ marshals entry and appends it to the tenant-scoped DLQ list via
// RPUSH, then resets the list TTL with EXPIRE (ADR-005).
func (w *RedisDLQWriter) SendToDLQ(ctx context.Context, entry *domain.DLQEntry) error {
	if entry == nil || entry.Task == nil {
		return fmt.Errorf("dlq writer: entry or entry.Task is nil")
	}

	payload, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("dlq writer: marshal entry for task %s: %w", entry.Task.ID, err)
	}

	key := dlqKey(entry.Task.TenantID, string(entry.Task.Type))
	ttl := time.Duration(w.cfg.Retry.DLQTTLSeconds) * time.Second

	pipe := w.client.Pipeline()
	pipe.RPush(ctx, key, string(payload))
	pipe.Expire(ctx, key, ttl)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("dlq writer: SendToDLQ task %s: %w", entry.Task.ID, err)
	}

	taskType := string(entry.Task.Type)
	if depth, depthErr := w.client.LLen(ctx, key).Result(); depthErr != nil {
		logger := observability.LoggerFromContext(ctx)
		logger.Warn().Err(depthErr).
			Str("task_id", entry.Task.ID).
			Msg("dlq writer: LLEN failed after RPUSH — depth gauge not updated")
	} else {
		w.metrics.RecordDLQDepth(entry.Task.TenantID, taskType, float64(depth))
	}

	logger := observability.LoggerFromContext(ctx)
	logger.Warn().
		Str("task_id", entry.Task.ID).
		Str("tenant_id", entry.Task.TenantID).
		Str("task_type", taskType).
		Int("attempt", entry.Task.Attempt).
		Str("failure_reason", entry.FailureReason).
		Msg("task.dead_lettered")

	return nil
}

// ListDLQ returns up to limit entries from the DLQ in insertion order (oldest
// first) via LRANGE 0 limit-1.
func (w *RedisDLQWriter) ListDLQ(ctx context.Context, tenantID, taskType string, limit int) ([]*domain.DLQEntry, error) {
	if limit <= 0 {
		return []*domain.DLQEntry{}, nil
	}

	raw, err := w.client.LRange(ctx, dlqKey(tenantID, taskType), 0, int64(limit-1)).Result()
	if err != nil {
		return nil, fmt.Errorf("dlq writer: ListDLQ %s/%s: %w", tenantID, taskType, err)
	}

	entries := make([]*domain.DLQEntry, 0, len(raw))
	for i, item := range raw {
		var entry domain.DLQEntry
		if unmarshalErr := json.Unmarshal([]byte(item), &entry); unmarshalErr != nil {
			return nil, fmt.Errorf("dlq writer: ListDLQ %s/%s: unmarshal index %d: %w", tenantID, taskType, i, unmarshalErr)
		}
		entries = append(entries, &entry)
	}
	return entries, nil
}

// DLQDepth returns the current LLEN of the tenant/task-type DLQ list.
func (w *RedisDLQWriter) DLQDepth(ctx context.Context, tenantID, taskType string) (int64, error) {
	depth, err := w.client.LLen(ctx, dlqKey(tenantID, taskType)).Result()
	if err != nil {
		return 0, fmt.Errorf("dlq writer: DLQDepth %s/%s: %w", tenantID, taskType, err)
	}
	return depth, nil
}
