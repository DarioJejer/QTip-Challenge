// Package domain defines the core types for the async email delivery queue.
// This package has zero external dependencies — all types are pure Go stdlib.
// It is the innermost layer of the hexagonal architecture and is imported by
// every other package; it must never import from internal/ports or internal/adapters.
package domain

import "fmt"

// TaskType categorises the kind of email being delivered.
// Each type may have distinct retry limits and priority defaults (ADR-005).
type TaskType string

const (
	// TaskTypeRegistration is sent when a new user account is created.
	TaskTypeRegistration TaskType = "registration"
	// TaskTypePasswordReset is sent when a user requests a password reset.
	TaskTypePasswordReset TaskType = "password_reset"
	// TaskTypeBilling is sent for invoices, payment receipts, and billing alerts.
	TaskTypeBilling TaskType = "billing"
	// TaskTypeMarketing is for bulk promotional and newsletter emails.
	TaskTypeMarketing TaskType = "marketing"
	// TaskTypeSecurity is for security alerts, 2FA codes, and login notifications.
	TaskTypeSecurity TaskType = "security"
	// TaskTypeTransactional covers all other transactional emails (order confirmations, etc.).
	TaskTypeTransactional TaskType = "transactional"
)

// IsValid reports whether t is a recognised TaskType.
func (t TaskType) IsValid() bool {
	switch t {
	case TaskTypeRegistration, TaskTypePasswordReset, TaskTypeBilling,
		TaskTypeMarketing, TaskTypeSecurity, TaskTypeTransactional:
		return true
	}
	return false
}

// DefaultMaxAttempts returns the recommended retry limit for t.
// Values are sourced from ADR-005: security=7, transactional=5, marketing=3.
func (t TaskType) DefaultMaxAttempts() int {
	switch t {
	case TaskTypeSecurity:
		return 7
	case TaskTypeTransactional:
		return 5
	case TaskTypeMarketing:
		return 3
	default:
		return 5
	}
}

// Priority controls which Redis Stream a task is written to.
// Higher-priority tasks are consumed first by the worker pool (ADR-002).
type Priority int8

const (
	// PriorityLow is used for non-time-sensitive emails such as marketing.
	PriorityLow Priority = 0
	// PriorityNormal is the default for most transactional emails.
	PriorityNormal Priority = 1
	// PriorityHigh is used for billing and password-reset emails.
	PriorityHigh Priority = 2
	// PriorityCritical is reserved for security alerts and 2FA codes.
	PriorityCritical Priority = 3
)

// String returns the human-readable name of p.
func (p Priority) String() string {
	switch p {
	case PriorityLow:
		return "low"
	case PriorityNormal:
		return "normal"
	case PriorityHigh:
		return "high"
	case PriorityCritical:
		return "critical"
	default:
		return fmt.Sprintf("unknown(%d)", int8(p))
	}
}

// QueueName returns the Redis Stream key for this priority band.
//
// The {queue} hash tag prefix ensures all four priority streams hash to the
// same Redis Cluster slot, which is required for cross-stream Lua scripts and
// XREADGROUP calls spanning multiple keys (ADR-002).
func (p Priority) QueueName() string {
	return fmt.Sprintf("{queue}:email:%s", p.String())
}

// IsValid reports whether p is a known Priority value (0–3).
func (p Priority) IsValid() bool {
	return p >= PriorityLow && p <= PriorityCritical
}

// TaskStatus represents the lifecycle state of an EmailTask as it moves
// through the queue system (ADR-005 state machine).
type TaskStatus string

const (
	// StatusPending indicates the task has been enqueued but not yet picked up.
	StatusPending TaskStatus = "pending"
	// StatusProcessing indicates a worker has claimed the task from the PEL.
	StatusProcessing TaskStatus = "processing"
	// StatusCompleted indicates the email was delivered successfully.
	StatusCompleted TaskStatus = "completed"
	// StatusFailed indicates the last delivery attempt failed.
	StatusFailed TaskStatus = "failed"
	// StatusRetryScheduled indicates the task is waiting in the delayed retry sorted set.
	StatusRetryScheduled TaskStatus = "retry_scheduled"
	// StatusDead indicates the task has been moved to the DLQ and will not be retried.
	StatusDead TaskStatus = "dead"
)
