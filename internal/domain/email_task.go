package domain

import (
	"crypto/sha256"
	"fmt"
	"time"
)

// EmailTask is the canonical domain object representing a single email delivery
// request. It is serialised as JSON and stored in Redis Streams (ADR-003).
//
// All fields are flat — no nested structs — to keep Redis CLI debugging
// straightforward and serialisation overhead minimal at 50k msgs/min.
type EmailTask struct {
	// --- Identity ---

	// ID is the unique task identifier. When created via DeterministicTaskID it
	// provides idempotent enqueue semantics (ADR-006).
	ID string `json:"id"`

	// TenantID namespaces the task for multi-tenant routing and DLQ key scoping (ADR-002).
	TenantID string `json:"tenant_id"`

	// --- Classification ---

	// Type determines retry limits, priority defaults, and routing rules.
	Type TaskType `json:"type"`

	// Priority maps to one of the four Redis priority Streams (ADR-002).
	Priority Priority `json:"priority"`

	// --- Delivery target ---

	// Recipient is the destination email address.
	Recipient string `json:"recipient"`

	// TemplateID identifies the email template in the rendering service.
	TemplateID string `json:"template_id"`

	// TemplateData is the key/value map merged into the template at render time.
	TemplateData map[string]any `json:"template_data,omitempty"`

	// --- Timing ---

	// EnqueuedAt is the wall-clock time the task was created by the producer.
	EnqueuedAt time.Time `json:"enqueued_at"`

	// ScheduledFor, when set, defers delivery until this wall-clock time.
	// The delayed scheduler moves tasks from the sorted set to the main Stream
	// once time.Now() >= *ScheduledFor (ADR-005).
	ScheduledFor *time.Time `json:"scheduled_for,omitempty"`

	// --- Retry state ---

	// Attempt is the 0-indexed current attempt count. It is incremented by the
	// RetryScheduler before re-enqueuing the task.
	Attempt int `json:"attempt"`

	// MaxAttempts is the total number of delivery attempts before dead-lettering.
	MaxAttempts int `json:"max_attempts"`

	// Status is the current lifecycle state of the task.
	Status TaskStatus `json:"status"`

	// LastError records the error string from the most recent failed attempt.
	LastError string `json:"last_error,omitempty"`

	// --- Observability (ADR-007) ---

	// TraceID is the W3C trace-context trace ID stored by the producer so that
	// the worker can continue the distributed trace end-to-end.
	TraceID string `json:"trace_id,omitempty"`

	// SpanID is the W3C trace-context span ID of the producer's enqueue span.
	SpanID string `json:"span_id,omitempty"`

	// --- Extensibility ---

	// Metadata carries arbitrary string key/value pairs for future extensibility.
	// The worker pool uses the reserved key "poison"="true" to flag tasks that
	// caused a worker panic (ADR-005).
	Metadata map[string]string `json:"metadata,omitempty"`
}

// Validate checks that all required fields are present and values are in range.
// It returns a *ValidationError on the first invalid field. Call this before
// enqueuing or processing a task.
func (t *EmailTask) Validate() error {
	if t.TenantID == "" {
		return &ValidationError{Field: "tenant_id", Message: "tenant_id is required"}
	}
	if t.Recipient == "" {
		return &ValidationError{Field: "recipient", Message: "recipient is required"}
	}
	if !t.Type.IsValid() {
		return &ValidationError{
			Field:   "type",
			Message: fmt.Sprintf("unknown task type %q", string(t.Type)),
		}
	}
	if !t.Priority.IsValid() {
		return &ValidationError{
			Field:   "priority",
			Message: fmt.Sprintf("invalid priority %d (must be 0–3)", int8(t.Priority)),
		}
	}
	if t.MaxAttempts <= 0 {
		return &ValidationError{
			Field:   "max_attempts",
			Message: "max_attempts must be greater than zero",
		}
	}
	return nil
}

// IsRetryable reports whether the task should be re-enqueued after the current
// failure. A task is NOT retryable when:
//   - IsPoisonMessage() is true (worker panic or non-retryable error flagged), or
//   - Attempt >= MaxAttempts (retry budget exhausted).
func (t *EmailTask) IsRetryable() bool {
	if t.IsPoisonMessage() {
		return false
	}
	return t.Attempt < t.MaxAttempts
}

// IsPoisonMessage reports whether the task should bypass the retry queue and
// go directly to the DLQ. A task is poison when its Metadata carries
// "poison"="true", which the worker supervisor sets on panic recovery (ADR-005).
func (t *EmailTask) IsPoisonMessage() bool {
	if t.Metadata == nil {
		return false
	}
	return t.Metadata["poison"] == "true"
}

// NextRetryDelay computes the exponential back-off delay for the next attempt
// using the default RetryPolicy. Callers may use RetryPolicyForType to get a
// task-type-specific policy and call ComputeDelay directly.
func (t *EmailTask) NextRetryDelay() time.Duration {
	return RetryPolicyForType(t.Type).ComputeDelay(t.Attempt)
}

// Clone returns a shallow copy of the task suitable for passing to goroutines.
// TemplateData and Metadata maps are shared — do not mutate them in the clone.
func (t *EmailTask) Clone() *EmailTask {
	clone := *t
	return &clone
}

// ValidationError describes a single field-level validation failure.
// Use errors.As to extract it from a wrapped error chain.
type ValidationError struct {
	// Field is the JSON field name that failed validation.
	Field string
	// Message is a human-readable description of the constraint that was violated.
	Message string
}

// Error implements the error interface.
func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation: %s: %s", e.Field, e.Message)
}

// DeterministicTaskID derives a stable, 32-hex-character task ID from the
// logical identity of the delivery request. Producers should use this to enable
// idempotent enqueue semantics: re-submitting the same logical email within the
// 24-hour deduplication window resolves to the same ID and is silently
// deduplicated by the IdempotencyStore (ADR-006).
//
// The inputs must together uniquely identify a single desired delivery:
//   - tenantID: the tenant namespace
//   - recipientEmail: the destination address
//   - templateID: the template being rendered
//   - eventID: the upstream event that triggered the email (e.g. order-id, user-id)
func DeterministicTaskID(tenantID, recipientEmail, templateID, eventID string) string {
	h := sha256.New()
	// NUL-separated to prevent collisions like ("a", "bc") vs ("ab", "c").
	fmt.Fprintf(h, "%s\x00%s\x00%s\x00%s", tenantID, recipientEmail, templateID, eventID)
	return fmt.Sprintf("%x", h.Sum(nil))[:32]
}
