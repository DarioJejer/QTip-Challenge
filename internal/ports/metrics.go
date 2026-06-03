package ports

import (
	"context"

	"github.com/DarioJejer/go-email-queue/internal/domain"
)

// MetricsRecorder abstracts the Prometheus instrumentation layer so that
// business logic in the worker and adapters does not depend directly on
// prometheus/client_golang. The concrete implementation is
// observability.PrometheusRecorder; in tests a no-op stub is used.
//
// All metric names and label sets are defined in ADR-007.
type MetricsRecorder interface {
	// RecordEnqueued increments email_queue_enqueued_total{tenant_id, task_type, priority}.
	RecordEnqueued(tenantID, taskType, priority string)

	// RecordProcessed increments email_queue_processed_total{tenant_id, task_type, status}
	// and records the processing duration in email_queue_processing_duration_seconds.
	// status must be one of "success", "failed", or "deduped".
	RecordProcessed(tenantID, taskType, status string, durationSeconds float64)

	// RecordQueueDepth sets the email_queue_depth gauge for the named queue stream.
	RecordQueueDepth(queueName string, depth float64)

	// RecordDLQDepth sets the email_queue_dlq_depth gauge for the tenant/type pair.
	RecordDLQDepth(tenantID, taskType string, depth float64)

	// RecordWorkerStats updates all worker-pool gauges from a WorkerStats snapshot:
	//   email_queue_worker_active, email_queue_worker_pool_size.
	RecordWorkerStats(stats domain.WorkerStats)
}

// QueueDepthChecker reads queue depth metrics directly from Redis for use by
// the DLQ monitor and the HPA custom-metric adapter (ADR-008).
type QueueDepthChecker interface {
	// QueueDepth returns the current XLEN of the given Redis Stream key.
	// Returns 0 when the key does not exist.
	QueueDepth(ctx context.Context, queueName string) (int64, error)

	// AllQueueDepths returns a map of stream key → depth for all four priority
	// Streams. It is called by the metrics scrape goroutine every
	// cfg.MetricsScrapeInterval to update the queue-depth gauges.
	AllQueueDepths(ctx context.Context) (map[string]int64, error)
}
