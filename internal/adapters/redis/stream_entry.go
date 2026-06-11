package redis

import (
	"encoding/json"
	"fmt"

	"github.com/DarioJejer/go-email-queue/internal/domain"
)

// XAddValuesBuilder returns the canonical flat field map for a Redis Streams
// XADD entry (ADR-002, ADR-003). All producers — HTTP enqueue, retry flush,
// and future schedulers — must use this helper so stream entries share the
// same top-level schema for redis-cli inspection and downstream tooling.
//
// payload must be the JSON-serialised EmailTask; use MarshalTaskPayload when
// the caller has not already marshalled the task.
func XAddValuesBuilder(task *domain.EmailTask, payload string) map[string]any {
	return map[string]any{
		"id":          task.ID,
		"payload":     payload,
		"enqueued_at": task.EnqueuedAt.UnixNano(),
		"tenant_id":   task.TenantID,
		"task_type":   string(task.Type),
		"priority":    task.Priority.String(),
		"attempt":     task.Attempt,
		"trace_id":    task.TraceID,
	}
}

// MarshalTaskPayload JSON-encodes task for the stream "payload" field.
func MarshalTaskPayload(task *domain.EmailTask) (string, error) {
	b, err := json.Marshal(task)
	if err != nil {
		return "", fmt.Errorf("redis: marshal task %s: %w", task.ID, err)
	}
	return string(b), nil
}
