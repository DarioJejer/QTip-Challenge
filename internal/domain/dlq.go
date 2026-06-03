package domain

import "time"

// DLQEntry is the record written to the Redis LIST dead-letter queue for a task
// that has exhausted all retry attempts or been flagged as a poison message
// (ADR-005). It carries the full task payload plus the complete attempt history
// so that operators can diagnose the root cause and, if appropriate, replay
// the message via the admin API.
//
// Redis key schema: queue:dlq:{tenantID}:{taskType}
// TTL: 7 days (configurable via DLQ_TTL_SECONDS env var).
type DLQEntry struct {
	// Task is the final state of the EmailTask at the time of dead-lettering.
	Task *EmailTask `json:"task"`

	// DeadAt is the wall-clock time at which the task was moved to the DLQ.
	DeadAt time.Time `json:"dead_at"`

	// FailureReason is a human-readable summary of why the task was dead-lettered.
	// Common values: "max attempts exceeded", "poison message", "non-retryable error".
	FailureReason string `json:"failure_reason"`

	// FinalError is the error string from the last failed delivery attempt.
	FinalError string `json:"final_error"`

	// AttemptHistory is the ordered list of all delivery attempts, oldest first.
	// It is used by the DLQ admin UI and for post-mortem analysis.
	AttemptHistory []AttemptRecord `json:"attempt_history"`
}

// AttemptRecord captures the outcome of a single delivery attempt by a worker.
// One record is appended to DLQEntry.AttemptHistory for each failed attempt.
type AttemptRecord struct {
	// Attempt is the 1-indexed attempt number (1 = first attempt).
	Attempt int `json:"attempt"`

	// StartedAt is the wall-clock time when the worker began processing.
	StartedAt time.Time `json:"started_at"`

	// FailedAt is the wall-clock time when the worker recorded the failure.
	// Zero value indicates the attempt succeeded (should not appear in a DLQEntry).
	FailedAt time.Time `json:"failed_at,omitempty"`

	// Error is the error message from this attempt. Empty on success.
	Error string `json:"error,omitempty"`

	// WorkerID is the unique identifier of the worker goroutine that processed
	// this attempt (hostname:pid:goroutine-id).
	WorkerID string `json:"worker_id"`
}
