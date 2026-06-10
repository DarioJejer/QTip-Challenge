package ports

import "context"

// IdempotencyStore provides atomic check-and-set semantics to prevent
// duplicate email delivery on top of Redis Streams' at-least-once guarantee.
//
// The concrete implementation uses a Lua CAS script (ADR-006) to atomically
// check and set the processing lock, eliminating TOCTOU races.
//
// Redis key schema: idempotency:{taskID}
// TTL: cfg.DLQTTLSeconds (24 h default) — tasks older than the window are
// safe to re-execute.
type IdempotencyStore interface {
	// SetProcessing atomically acquires the processing lock for taskID.
	// It returns acquired=true if this caller is the first to claim the task.
	// If the key already exists (status "processing" or "completed"), it
	// returns acquired=false and the caller should XACK and skip the task.
	//
	// workerID is stored in the lock value for operational debugging.
	SetProcessing(ctx context.Context, taskID, workerID string) (acquired bool, err error)

	// SetCompleted transitions the idempotency key from "processing" to
	// "completed". It uses SET XX so that it only updates an existing key —
	// preventing ghost completions where the lock expired mid-flight.
	// The TTL is reset to extend the deduplication window from the time of
	// completion.
	SetCompleted(ctx context.Context, taskID string) error

	// IsCompleted checks whether taskID has already been successfully
	// processed. Returns true only when the stored status is "completed".
	// Returns false for unknown keys (TTL expired) or "processing" status.
	IsCompleted(ctx context.Context, taskID string) (bool, error)

	// ClearProcessing deletes the processing lock for taskID. It is called
	// by the worker before Nacking a task whose downstream write failed
	// (ScheduleRetry or SendToDLQ). Without this, the PEL reclaim would see
	// acquired=false for the same attempt key and silently drop the task.
	//
	// It is safe to call on a key that has already expired or been completed —
	// the implementation must be idempotent (DEL on a missing key is a no-op).
	ClearProcessing(ctx context.Context, taskID string) error
}
