package observability

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/DarioJejer/go-email-queue/internal/domain"
)

// PrometheusRecorder implements ports.MetricsRecorder.
//
// All metric names and label sets are specified in ADR-007. The recorder is
// always constructed with an isolated *prometheus.Registry (never
// DefaultRegisterer) so that multiple instances can co-exist in tests without
// "already registered" panics. The isolated registry is also the one served
// by the /metrics endpoint, ensuring no accidental Go runtime metrics bleed
// into the application metric namespace.
type PrometheusRecorder struct {
	enqueuedTotal             *prometheus.CounterVec
	processedTotal            *prometheus.CounterVec
	processingDurationSeconds *prometheus.HistogramVec
	queueDepth                *prometheus.GaugeVec
	dlqDepth                  *prometheus.GaugeVec
	workerActive              prometheus.Gauge
	workerPoolSize            prometheus.Gauge
}

// NewPrometheusRecorder creates and registers all ADR-007 application metrics
// against reg. It panics (via MustRegister) if any metric cannot be registered
// — misconfiguration must surface at startup, not be swallowed silently.
func NewPrometheusRecorder(reg *prometheus.Registry) *PrometheusRecorder {
	r := &PrometheusRecorder{
		// email_queue_enqueued_total — incremented by the producer on every
		// successful XADD. Label cardinality: tenants × task_types × priorities.
		enqueuedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "email_queue_enqueued_total",
			Help: "Total number of tasks enqueued, partitioned by tenant, type, and priority.",
		}, []string{"tenant_id", "task_type", "priority"}),

		// email_queue_processed_total — incremented by the worker supervisor
		// after each task reaches a terminal state.
		processedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "email_queue_processed_total",
			Help: "Total tasks processed, partitioned by tenant, type, and outcome (success|failed|deduped).",
		}, []string{"tenant_id", "task_type", "status"}),

		// email_queue_processing_duration_seconds — histogram from dequeue
		// to completion. Buckets cover 100ms (fast path) to 30s (slow send).
		processingDurationSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "email_queue_processing_duration_seconds",
			Help:    "End-to-end processing time from dequeue to completion.",
			Buckets: []float64{0.1, 0.5, 1, 5, 10, 30},
		}, []string{"tenant_id", "task_type"}),

		// email_queue_depth — XLEN of each priority stream, scraped by the
		// DLQ monitor and used as the HPA custom metric (ADR-008).
		queueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "email_queue_depth",
			Help: "Current XLEN of each Redis Stream priority queue.",
		}, []string{"queue_name"}),

		// email_queue_dlq_depth — LLEN of the dead-letter list per tenant/type.
		dlqDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "email_queue_dlq_depth",
			Help: "Current LLEN of the dead-letter queue per tenant and task type.",
		}, []string{"tenant_id", "task_type"}),

		// email_queue_worker_active — current number of goroutines holding a
		// semaphore slot (i.e. actively processing a task).
		workerActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "email_queue_worker_active",
			Help: "Number of worker goroutines currently processing a task.",
		}),

		// email_queue_worker_pool_size — configured ceiling from WORKER_POOL_SIZE.
		workerPoolSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "email_queue_worker_pool_size",
			Help: "Configured maximum concurrency of the worker pool (WORKER_POOL_SIZE).",
		}),
	}

	reg.MustRegister(
		r.enqueuedTotal,
		r.processedTotal,
		r.processingDurationSeconds,
		r.queueDepth,
		r.dlqDepth,
		r.workerActive,
		r.workerPoolSize,
	)

	return r
}

// RecordEnqueued increments email_queue_enqueued_total.
// priority should be domain.Priority.String() ("low", "normal", "high", "critical").
func (r *PrometheusRecorder) RecordEnqueued(tenantID, taskType, priority string) {
	r.enqueuedTotal.WithLabelValues(tenantID, taskType, priority).Inc()
}

// RecordProcessed increments email_queue_processed_total and records the
// processing latency in email_queue_processing_duration_seconds.
// status must be "success", "failed", or "deduped".
func (r *PrometheusRecorder) RecordProcessed(tenantID, taskType, status string, durationSeconds float64) {
	r.processedTotal.WithLabelValues(tenantID, taskType, status).Inc()
	r.processingDurationSeconds.WithLabelValues(tenantID, taskType).Observe(durationSeconds)
}

// RecordQueueDepth sets email_queue_depth for the named Redis Stream.
func (r *PrometheusRecorder) RecordQueueDepth(queueName string, depth float64) {
	r.queueDepth.WithLabelValues(queueName).Set(depth)
}

// RecordDLQDepth sets email_queue_dlq_depth for the tenant/type pair.
func (r *PrometheusRecorder) RecordDLQDepth(tenantID, taskType string, depth float64) {
	r.dlqDepth.WithLabelValues(tenantID, taskType).Set(depth)
}

// RecordWorkerStats updates the worker-pool gauges from a WorkerStats snapshot.
func (r *PrometheusRecorder) RecordWorkerStats(stats domain.WorkerStats) {
	r.workerActive.Set(float64(stats.ActiveWorkers))
	r.workerPoolSize.Set(float64(stats.PoolSize))
}

// ---------------------------------------------------------------------------
// Test accessors
// ---------------------------------------------------------------------------
// The methods below expose the underlying Prometheus collectors so that unit
// tests can use prometheus/testutil.ToFloat64 for precise metric assertions.
// They are NOT part of the ports.MetricsRecorder interface and must not be
// called from production business logic.

// EnqueuedTotal returns the email_queue_enqueued_total CounterVec.
func (r *PrometheusRecorder) EnqueuedTotal() *prometheus.CounterVec { return r.enqueuedTotal }

// ProcessedTotal returns the email_queue_processed_total CounterVec.
func (r *PrometheusRecorder) ProcessedTotal() *prometheus.CounterVec { return r.processedTotal }

// QueueDepthGauge returns the email_queue_depth GaugeVec.
func (r *PrometheusRecorder) QueueDepthGauge() *prometheus.GaugeVec { return r.queueDepth }

// DLQDepthGauge returns the email_queue_dlq_depth GaugeVec.
func (r *PrometheusRecorder) DLQDepthGauge() *prometheus.GaugeVec { return r.dlqDepth }

// WorkerActiveGauge returns the email_queue_worker_active Gauge.
func (r *PrometheusRecorder) WorkerActiveGauge() prometheus.Gauge { return r.workerActive }

// WorkerPoolSizeGauge returns the email_queue_worker_pool_size Gauge.
func (r *PrometheusRecorder) WorkerPoolSizeGauge() prometheus.Gauge { return r.workerPoolSize }
