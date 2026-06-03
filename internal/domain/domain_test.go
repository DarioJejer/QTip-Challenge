package domain_test

import (
	"testing"
	"time"

	"github.com/DarioJejer/go-email-queue/internal/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// validTask returns a fully-populated EmailTask that passes Validate().
func validTask() *domain.EmailTask {
	return &domain.EmailTask{
		ID:          "task-001",
		TenantID:    "tenant-abc",
		Type:        domain.TaskTypeTransactional,
		Priority:    domain.PriorityNormal,
		Recipient:   "user@example.com",
		TemplateID:  "welcome-v1",
		EnqueuedAt:  time.Now(),
		Attempt:     0,
		MaxAttempts: 5,
		Status:      domain.StatusPending,
	}
}

// ---------------------------------------------------------------------------
// EmailTask.Validate
// ---------------------------------------------------------------------------

func TestEmailTask_Validate_Valid(t *testing.T) {
	require.NoError(t, validTask().Validate())
}

func TestEmailTask_Validate_Fields(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*domain.EmailTask)
		wantField string
	}{
		{
			name:      "missing TenantID",
			mutate:    func(t *domain.EmailTask) { t.TenantID = "" },
			wantField: "tenant_id",
		},
		{
			name:      "missing Recipient",
			mutate:    func(t *domain.EmailTask) { t.Recipient = "" },
			wantField: "recipient",
		},
		{
			name:      "invalid Type",
			mutate:    func(t *domain.EmailTask) { t.Type = "unknown_type" },
			wantField: "type",
		},
		{
			name:      "invalid Priority (too high)",
			mutate:    func(t *domain.EmailTask) { t.Priority = 99 },
			wantField: "priority",
		},
		{
			name:      "invalid Priority (negative)",
			mutate:    func(t *domain.EmailTask) { t.Priority = -1 },
			wantField: "priority",
		},
		{
			name:      "zero MaxAttempts",
			mutate:    func(t *domain.EmailTask) { t.MaxAttempts = 0 },
			wantField: "max_attempts",
		},
		{
			name:      "negative MaxAttempts",
			mutate:    func(t *domain.EmailTask) { t.MaxAttempts = -3 },
			wantField: "max_attempts",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := validTask()
			tc.mutate(task)
			err := task.Validate()
			require.Error(t, err)
			var ve *domain.ValidationError
			require.ErrorAs(t, err, &ve, "expected *ValidationError, got %T: %v", err, err)
			assert.Equal(t, tc.wantField, ve.Field)
		})
	}
}

// ---------------------------------------------------------------------------
// EmailTask.IsRetryable / IsPoisonMessage
// ---------------------------------------------------------------------------

func TestEmailTask_IsRetryable(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*domain.EmailTask)
		want    bool
	}{
		{
			name:   "attempts remaining",
			mutate: func(t *domain.EmailTask) { t.Attempt = 2; t.MaxAttempts = 5 },
			want:   true,
		},
		{
			name:   "exactly at max",
			mutate: func(t *domain.EmailTask) { t.Attempt = 5; t.MaxAttempts = 5 },
			want:   false,
		},
		{
			name:   "over max",
			mutate: func(t *domain.EmailTask) { t.Attempt = 6; t.MaxAttempts = 5 },
			want:   false,
		},
		{
			name: "poison message overrides remaining attempts",
			mutate: func(t *domain.EmailTask) {
				t.Attempt = 1
				t.MaxAttempts = 5
				t.Metadata = map[string]string{"poison": "true"}
			},
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := validTask()
			tc.mutate(task)
			assert.Equal(t, tc.want, task.IsRetryable())
		})
	}
}

func TestEmailTask_IsPoisonMessage(t *testing.T) {
	task := validTask()
	assert.False(t, task.IsPoisonMessage(), "no metadata → not poison")

	task.Metadata = map[string]string{"other": "value"}
	assert.False(t, task.IsPoisonMessage(), "metadata without poison key → not poison")

	task.Metadata["poison"] = "true"
	assert.True(t, task.IsPoisonMessage(), "metadata poison=true → poison")
}

// ---------------------------------------------------------------------------
// RetryPolicy.ComputeDelay
// ---------------------------------------------------------------------------

func TestRetryPolicy_ComputeDelay_Deterministic(t *testing.T) {
	// Zero jitter makes delays deterministic for assertion.
	policy := domain.RetryPolicy{
		MaxAttempts:  10,
		BaseDelay:    1 * time.Second,
		MaxDelay:     15 * time.Minute,
		JitterFactor: 0,
	}

	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{0, 1 * time.Second},
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
		{4, 16 * time.Second},
		{10, 15 * time.Minute},  // capped at MaxDelay
		{100, 15 * time.Minute}, // far above cap
		{-1, 1 * time.Second},   // negative → treated as 0
	}

	for _, tc := range tests {
		got := policy.ComputeDelay(tc.attempt)
		assert.Equal(t, tc.want, got, "attempt=%d", tc.attempt)
	}
}

func TestRetryPolicy_ComputeDelay_JitterWithinBounds(t *testing.T) {
	policy := domain.RetryPolicy{
		MaxAttempts:  5,
		BaseDelay:    1 * time.Second,
		MaxDelay:     15 * time.Minute,
		JitterFactor: 0.2,
	}

	// At attempt=1, base = 2s. Jitter adds at most 20% → max 2.4s.
	const (
		minExpected = 2 * time.Second
		maxExpected = time.Duration(2.4 * float64(time.Second))
	)

	for i := 0; i < 1000; i++ {
		got := policy.ComputeDelay(1)
		assert.GreaterOrEqual(t, got, minExpected, "jitter undershot lower bound")
		assert.LessOrEqual(t, got, maxExpected, "jitter exceeded upper bound")
	}
}

// ---------------------------------------------------------------------------
// Priority.QueueName — ADR-002 hash-tag convention
// ---------------------------------------------------------------------------

func TestPriority_QueueName(t *testing.T) {
	tests := []struct {
		p    domain.Priority
		want string
	}{
		{domain.PriorityLow, "{queue}:email:low"},
		{domain.PriorityNormal, "{queue}:email:normal"},
		{domain.PriorityHigh, "{queue}:email:high"},
		{domain.PriorityCritical, "{queue}:email:critical"},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.want, tc.p.QueueName(), "Priority(%d).QueueName()", int8(tc.p))
	}
}

// ---------------------------------------------------------------------------
// DeterministicTaskID — ADR-006 producer-side deduplication
// ---------------------------------------------------------------------------

func TestDeterministicTaskID_Stable(t *testing.T) {
	id1 := domain.DeterministicTaskID("tenant-a", "user@example.com", "welcome-v1", "evt-123")
	id2 := domain.DeterministicTaskID("tenant-a", "user@example.com", "welcome-v1", "evt-123")
	assert.Equal(t, id1, id2, "same inputs must produce same ID")
}

func TestDeterministicTaskID_DifferentInputs(t *testing.T) {
	base := domain.DeterministicTaskID("tenant-a", "user@example.com", "welcome-v1", "evt-123")

	tests := []struct {
		name string
		id   string
	}{
		{"different tenant", domain.DeterministicTaskID("tenant-b", "user@example.com", "welcome-v1", "evt-123")},
		{"different recipient", domain.DeterministicTaskID("tenant-a", "other@example.com", "welcome-v1", "evt-123")},
		{"different template", domain.DeterministicTaskID("tenant-a", "user@example.com", "reset-v2", "evt-123")},
		{"different eventID", domain.DeterministicTaskID("tenant-a", "user@example.com", "welcome-v1", "evt-456")},
	}
	for _, tc := range tests {
		assert.NotEqual(t, base, tc.id, tc.name)
	}
}

func TestDeterministicTaskID_Length(t *testing.T) {
	id := domain.DeterministicTaskID("t", "r", "tmpl", "evt")
	assert.Len(t, id, 32, "ID must be 32 hex chars")
}

// ---------------------------------------------------------------------------
// TaskType.DefaultMaxAttempts — ADR-005 per-type retry limits
// ---------------------------------------------------------------------------

func TestTaskType_DefaultMaxAttempts(t *testing.T) {
	assert.Equal(t, 7, domain.TaskTypeSecurity.DefaultMaxAttempts())
	assert.Equal(t, 5, domain.TaskTypeTransactional.DefaultMaxAttempts())
	assert.Equal(t, 3, domain.TaskTypeMarketing.DefaultMaxAttempts())
	assert.Equal(t, 5, domain.TaskTypeRegistration.DefaultMaxAttempts())
	assert.Equal(t, 5, domain.TaskTypeBilling.DefaultMaxAttempts())
}

// ---------------------------------------------------------------------------
// EmailTask.Clone
// ---------------------------------------------------------------------------

func TestEmailTask_Clone_IndependentValue(t *testing.T) {
	original := validTask()
	clone := original.Clone()

	// Mutating the clone's scalar fields must not affect the original.
	clone.Attempt = 99
	clone.Status = domain.StatusDead

	assert.Equal(t, 0, original.Attempt)
	assert.Equal(t, domain.StatusPending, original.Status)
}
