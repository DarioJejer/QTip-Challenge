package redis_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	redisadapter "github.com/DarioJejer/go-email-queue/internal/adapters/redis"
	"github.com/DarioJejer/go-email-queue/internal/domain"
)

func TestXAddValuesBuilder_CanonicalFields(t *testing.T) {
	t.Parallel()
	enqueuedAt := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	task := &domain.EmailTask{
		ID:          "task-abc",
		TenantID:    "tenant-1",
		Type:        domain.TaskTypeTransactional,
		Priority:    domain.PriorityHigh,
		Recipient:   "user@example.com",
		TemplateID:  "welcome",
		EnqueuedAt:  enqueuedAt,
		Attempt:     2,
		MaxAttempts: 5,
		TraceID:     "trace-xyz",
		Status:      domain.StatusFailed,
	}

	payload, err := redisadapter.MarshalTaskPayload(task)
	require.NoError(t, err)

	values := redisadapter.XAddValuesBuilder(task, payload)

	assert.Equal(t, task.ID, values["id"])
	assert.Equal(t, payload, values["payload"])
	assert.Equal(t, enqueuedAt.UnixNano(), values["enqueued_at"])
	assert.Equal(t, task.TenantID, values["tenant_id"])
	assert.Equal(t, string(task.Type), values["task_type"])
	assert.Equal(t, task.Priority.String(), values["priority"])
	assert.Equal(t, task.Attempt, values["attempt"])
	assert.Equal(t, task.TraceID, values["trace_id"])
}
