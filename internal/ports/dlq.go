package ports

import (
	"context"

	"github.com/DarioJejer/go-email-queue/internal/domain"
)

// DLQWriter appends dead-lettered tasks to a Redis LIST and exposes depth
// information for monitoring. The DLQ is an append-only inspection log —
// there are no consumers; entries are replayed manually via the admin API.
//
// Redis key schema: queue:dlq:{tenantID}:{taskType}
// TTL: cfg.DLQTTLSeconds (default 7 days), reset on each RPUSH (ADR-005).
type DLQWriter interface {
	// SendToDLQ marshals entry and appends it to the tenant-scoped DLQ list
	// with RPUSH, then resets the list TTL with EXPIRE. It also records the
	// DLQ depth gauge via the metrics recorder.
	//
	// entry.Task must not be nil.
	SendToDLQ(ctx context.Context, entry *domain.DLQEntry) error

	// ListDLQ returns up to limit entries from the DLQ list in insertion order
	// (oldest first) using LRANGE 0 limit-1. It is used by the admin API to
	// surface dead-lettered tasks for inspection.
	ListDLQ(ctx context.Context, tenantID, taskType string, limit int) ([]*domain.DLQEntry, error)

	// DLQDepth returns the current length of the DLQ list for the given tenant
	// and task type via LLEN. Returns 0 when the key does not exist.
	DLQDepth(ctx context.Context, tenantID, taskType string) (int64, error)
}
