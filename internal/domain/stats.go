package domain

// WorkerStats is a point-in-time snapshot of the worker pool's operational state.
// It is read atomically by the metrics recorder and the /healthz handler.
type WorkerStats struct {
	// ActiveWorkers is the number of goroutines currently processing tasks.
	// This equals the number of semaphore slots currently acquired (ADR-004).
	ActiveWorkers int64

	// PoolSize is the configured maximum worker concurrency (WORKER_POOL_SIZE env var).
	PoolSize int

	// TotalProcessed is the cumulative count of tasks successfully delivered
	// since the service started.
	TotalProcessed int64

	// TotalFailed is the cumulative count of task delivery failures
	// (including those that were subsequently retried or dead-lettered).
	TotalFailed int64

	// TotalRetried is the cumulative count of tasks re-enqueued to the
	// delayed retry sorted set.
	TotalRetried int64

	// QueueDepth is the current number of messages in the main Redis Streams queue
	// (sum of XLEN across all priority streams).
	QueueDepth int64

	// DLQDepth is the total number of entries across all DLQ lists for all tenants.
	DLQDepth int64
}

// TenantConfig holds per-tenant delivery policy overrides.
// When a task is enqueued without explicit configuration, the system falls back
// to the global defaults defined in RetryPolicy.
//
// TenantConfig is loaded at startup from a ConfigMap or database and cached
// in memory. It is referenced by the WorkerSupervisor when computing retry
// delays and rate limits.
type TenantConfig struct {
	// TenantID is the unique identifier for this tenant (matches EmailTask.TenantID).
	TenantID string `json:"tenant_id"`

	// MaxRetries overrides the global default max retry count for all task types
	// belonging to this tenant. A value of 0 means "use global default".
	MaxRetries int `json:"max_retries"`

	// DefaultPriority is assigned to tasks enqueued without an explicit priority.
	DefaultPriority Priority `json:"default_priority"`

	// RateLimitPerMinute caps the number of tasks this tenant may enqueue per minute.
	// A value of 0 disables rate limiting for this tenant.
	RateLimitPerMinute int `json:"rate_limit_per_minute"`
}
