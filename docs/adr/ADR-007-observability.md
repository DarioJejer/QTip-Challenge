# ADR-007: Observability — Logging, Metrics & Tracing

**Status:** Accepted
**Date:** 2026-05-27
**Deciders:** Engineering Team

---

## Context

A production async system processing 50k msgs/min is a black box without structured observability. We need to answer operational questions like:

- Is the system processing tasks at the expected rate?
- Which tenants are generating the most failures?
- How deep is the queue right now, and is it growing?
- What happened to task `01J2K...` — was it processed, retried, or dead-lettered?
- When did the retry storm start, and which worker pod was affected?
- What is the end-to-end latency from API enqueue to email delivery?

Three pillars of observability address these: **structured logging** (event correlation), **metrics** (aggregate system health), and **distributed tracing** (per-request latency and causality).

---

## Decision

**We will use `zerolog` for structured logging, `prometheus/client_golang` for metrics, and OpenTelemetry (OTLP) for distributed tracing.** All three are integrated at the component level, not as cross-cutting middleware only.

---

## Pillar 1: Structured Logging (zerolog)

### Why zerolog

`zerolog` produces zero-allocation structured JSON logs, making it ideal for high-throughput services. At 50k msgs/min, even a 100ns allocation per log event adds up. `zerolog` avoids heap allocations on the hot path entirely via its builder API.

### Universal log schema

Every log line **must** include these fields:

| Field | Type | Source | Description |
|-------|------|--------|-------------|
| `ts` | RFC3339Nano | zerolog default | Timestamp |
| `level` | string | zerolog | `debug` \| `info` \| `warn` \| `error` \| `fatal` |
| `service` | string | const | `"email-queue"` |
| `version` | string | build var | Git tag / commit SHA |
| `component` | string | per component | `producer` \| `worker` \| `scheduler` \| `dlq_monitor` \| `http` |

Context-dependent fields (added when available):

| Field | Type | Description |
|-------|------|-------------|
| `trace_id` | string | OTel W3C trace ID (32 hex chars) |
| `span_id` | string | OTel span ID (16 hex chars) |
| `tenant_id` | string | Task tenant |
| `task_id` | string | Task UUID |
| `task_type` | string | TaskType enum value |
| `priority` | string | Priority name (`low` \| `normal` \| `high` \| `critical`) |
| `attempt` | int | Current attempt number |
| `duration_ms` | int64 | Processing duration in milliseconds |
| `error` | string | Error message (error-level only) |
| `worker_id` | string | `{hostname}:{pid}:{goroutine_id}` |

### Log event catalogue

Every named event below must be emitted exactly once per occurrence. No ad-hoc log messages on the hot path.

| Event | Level | Component | Required extra fields |
|-------|-------|-----------|----------------------|
| `task.enqueued` | INFO | producer | `task_id`, `tenant_id`, `task_type`, `priority`, `queue` |
| `task.enqueued_delayed` | INFO | producer | `task_id`, `scheduled_for` |
| `task.batch_enqueued` | INFO | producer | `count`, `failed_count` |
| `task.dequeued` | DEBUG | worker | `task_id`, `stream_msg_id` |
| `task.processing` | DEBUG | worker | `task_id`, `attempt`, `worker_id` |
| `task.completed` | INFO | worker | `task_id`, `tenant_id`, `task_type`, `duration_ms`, `attempt` |
| `task.failed` | WARN | worker | `task_id`, `error`, `attempt`, `retryable` |
| `task.retried` | INFO | scheduler | `task_id`, `attempt`, `delay_ms`, `scheduled_for` |
| `task.dead_lettered` | WARN | worker | `task_id`, `tenant_id`, `task_type`, `failure_reason`, `attempt` |
| `task.deduplicated` | INFO | worker | `task_id`, `existing_status` |
| `worker.panic` | ERROR | worker | `task_id`, `panic_value`, `stack_trace` |
| `worker.started` | INFO | supervisor | `worker_id`, `pool_size` |
| `worker.stopped` | INFO | supervisor | `worker_id` |
| `shutdown.initiated` | INFO | supervisor | `in_flight` |
| `shutdown.draining` | WARN | supervisor | `in_flight`, `drain_budget_remaining_ms` |
| `shutdown.drained` | INFO | supervisor | `duration_ms` |
| `shutdown.drain_timeout` | WARN | supervisor | `in_flight_remaining` |
| `shutdown.completed` | INFO | main | `duration_ms` |
| `redis.reconnected` | INFO | redis | — |
| `redis.error` | ERROR | redis | `operation`, `error` |
| `migration.applied` | INFO | migration | `version`, `name` |

### Context propagation for logging

```go
// WithTaskFields adds task-scoped fields to the logger in context
func WithTaskFields(ctx context.Context, task *domain.EmailTask) context.Context {
    logger := zerolog.Ctx(ctx).With().
        Str("task_id", task.ID).
        Str("tenant_id", task.TenantID).
        Str("task_type", string(task.Type)).
        Int("attempt", task.Attempt).
        Logger()
    return logger.WithContext(ctx)
}

// Usage in worker:
ctx = observability.WithTaskFields(ctx, task)
log.Ctx(ctx).Info().Int64("duration_ms", dur.Milliseconds()).Msg("task.completed")
```

---

## Pillar 2: Prometheus Metrics

All metrics are registered on a dedicated `*prometheus.Registry` (not the default global registry) to allow clean testing and multi-instance isolation.

### Metric catalogue

#### Counters

| Metric name | Labels | Description |
|-------------|--------|-------------|
| `email_queue_enqueued_total` | `tenant_id`, `task_type`, `priority` | Tasks enqueued by producer |
| `email_queue_processed_total` | `tenant_id`, `task_type`, `status` | Tasks processed by worker (`status`: `success` \| `failed` \| `deduped` \| `panic`) |
| `email_queue_redis_errors_total` | `operation` | Redis command errors by operation name |
| `email_queue_dlq_entries_total` | `tenant_id`, `task_type`, `reason` | Tasks dead-lettered by reason |

#### Histograms

| Metric name | Labels | Buckets | Description |
|-------------|--------|---------|-------------|
| `email_queue_processing_duration_seconds` | `tenant_id`, `task_type` | 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30 | End-to-end worker processing time |
| `email_queue_attempt_number` | `task_type` | 1, 2, 3, 4, 5, 6, 7, 10 | Distribution of attempt number at completion |
| `email_queue_enqueue_duration_seconds` | — | 0.001, 0.005, 0.01, 0.05, 0.1 | Producer XADD latency |

#### Gauges

| Metric name | Labels | Description | Scrape source |
|-------------|--------|-------------|---------------|
| `email_queue_depth` | `queue_name` | Current stream length per priority queue | Redis `XLEN` |
| `email_queue_dlq_depth` | `tenant_id`, `task_type` | DLQ LIST length | Redis `LLEN` |
| `email_queue_retry_depth` | — | Delayed retry sorted set size | Redis `ZCARD` |
| `email_queue_worker_active` | — | Current in-flight worker goroutines | Semaphore counter |
| `email_queue_worker_pool_size` | — | Configured pool size | Config |
| `email_queue_pел_size` | `queue_name` | Pending Entry List size | Redis `XPENDING` |

### Scrape interval recommendation

| Metric type | Recommended scrape interval |
|-------------|----------------------------|
| Queue depth gauges | 15s |
| Worker saturation gauges | 15s |
| DLQ depth | 30s |
| All counters/histograms | 15s |

### Alert thresholds (Prometheus alerting rules)

```yaml
# Queue depth growing unboundedly
- alert: EmailQueueDepthHigh
  expr: email_queue_depth > 50000
  for: 5m
  labels: { severity: warning }

# DLQ has entries — requires operator attention
- alert: EmailQueueDLQNonEmpty
  expr: email_queue_dlq_depth > 0
  for: 1m
  labels: { severity: warning }

# Workers fully saturated — scale out needed
- alert: EmailQueueWorkerSaturation
  expr: email_queue_worker_active / email_queue_worker_pool_size > 0.9
  for: 2m
  labels: { severity: warning }

# Processing error rate elevated
- alert: EmailQueueErrorRateHigh
  expr: rate(email_queue_processed_total{status="failed"}[5m]) /
        rate(email_queue_processed_total[5m]) > 0.05
  for: 5m
  labels: { severity: critical }
```

---

## Pillar 3: Distributed Tracing (OpenTelemetry)

### Why OTel

OpenTelemetry is the CNCF standard for distributed tracing. It is vendor-neutral (Jaeger, Tempo, Honeycomb, Datadog all accept OTLP). The Go SDK is production-ready and integrates cleanly with `context.Context`.

### Tracer setup

```go
func InitTracer(ctx context.Context, cfg *config.Config) (shutdown func(context.Context) error, err error) {
    if cfg.OTELEndpoint == "" {
        // Noop provider — no traces exported, zero overhead
        otel.SetTracerProvider(trace.NewNoopTracerProvider())
        return func(context.Context) error { return nil }, nil
    }

    exporter, err := otlptracegrpc.New(ctx,
        otlptracegrpc.WithEndpoint(cfg.OTELEndpoint),
        otlptracegrpc.WithInsecure(),  // TLS configurable via OTEL_EXPORTER_OTLP_INSECURE
        otlptracegrpc.WithRetry(otlptracegrpc.RetryConfig{Enabled: true}),
    )

    tp := sdktrace.NewTracerProvider(
        sdktrace.WithBatcher(exporter),
        sdktrace.WithResource(resource.NewWithAttributes(
            semconv.SchemaURL,
            semconv.ServiceNameKey.String(cfg.ServiceName),
            semconv.ServiceVersionKey.String(cfg.Version),
        )),
        sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.TraceSampleRate))),
    )

    otel.SetTracerProvider(tp)
    otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
        propagation.TraceContext{},
        propagation.Baggage{},
    ))

    return tp.Shutdown, nil
}
```

### Span catalogue

| Span name | Component | Attributes | Events |
|-----------|-----------|------------|--------|
| `producer.enqueue` | producer | `task.id`, `task.type`, `task.priority`, `tenant.id` | — |
| `producer.enqueue_batch` | producer | `batch.size`, `batch.failed` | — |
| `worker.process` | worker | `task.id`, `task.type`, `task.attempt`, `tenant.id` | `task.processing_started`, `task.completed` or `task.failed` |
| `email.send` | email adapter | `task.id`, `recipient.email`, `template.id` | `send.success` or `send.failure` |
| `retry.schedule` | scheduler | `task.id`, `task.attempt`, `retry.delay_ms` | — |
| `dlq.write` | dlq writer | `task.id`, `tenant.id`, `failure.reason` | — |
| `idempotency.check` | worker | `task.id` | `idempotency.acquired` or `idempotency.skipped` |

### Cross-process trace propagation

The challenge in async systems is that the trace context lives in the HTTP request but the work happens in a different process (worker pod) at a different time. We bridge this by storing the trace context in the task payload:

```go
// Producer side: capture current span context into task
span := trace.SpanFromContext(ctx)
task.TraceID = span.SpanContext().TraceID().String()
task.SpanID  = span.SpanContext().SpanID().String()

// Worker side: reconstruct parent span context from task fields
spanCtx := trace.NewSpanContext(trace.SpanContextConfig{
    TraceID:    traceID,  // parsed from task.TraceID
    SpanID:     spanID,   // parsed from task.SpanID
    TraceFlags: trace.FlagsSampled,
    Remote:     true,     // marks this as a remote parent
})
ctx = trace.ContextWithRemoteSpanContext(ctx, spanCtx)

// Start child span — this appears as a continuation of the original request trace
ctx, span = tracer.Start(ctx, "worker.process")
defer span.End()
```

This produces an end-to-end trace:

```
[HTTP request: POST /v1/tasks]
  └── [producer.enqueue]          (producer pod)
        └── [worker.process]      (worker pod, async)
              └── [email.send]    (worker pod → SendGrid)
```

### Baggage propagation

Tenant ID and task type are propagated as OTel baggage so they are available in all child spans without repeating the task lookup:

```go
bag, _ := baggage.New(
    baggage.NewMember("tenant.id", task.TenantID),
    baggage.NewMember("task.type", string(task.Type)),
)
ctx = baggage.ContextWithBaggage(ctx, bag)
```

### Sample rate

At 50k msgs/min, 100% sampling would generate ~72M spans/hour — expensive in storage and ingestion cost. Recommended default: **1% tail-based sampling** (sample all traces with errors + 1% of successful traces).

```
OTEL_TRACES_SAMPLER=parentbased_traceidratio
OTEL_TRACES_SAMPLER_ARG=0.01
```

---

## Consequences

**What becomes easier:**
- Task lifecycle is fully traceable from producer HTTP call to email delivery confirmation
- Queue depth alerts enable proactive scaling before SLA breach
- Structured log fields allow `tenant_id`-scoped queries in any log aggregator (Loki, CloudWatch, Datadog)
- `attempt_number` histogram reveals retry distribution — identifies task types that commonly fail

**What becomes harder:**
- `trace_id`/`span_id` must be stored in every task payload — adds ~80 bytes per task
- OTel SDK adds ~10ms to startup time (exporter initialization)
- Alert thresholds need tuning for each deployment's traffic pattern
- zerolog's builder API requires discipline — fmt.Sprintf in log messages defeats zero-allocation purpose

**What we will need to revisit:**
- If log volume exceeds log aggregator capacity, add sampling at the logger level for DEBUG events
- If tracing storage cost is high, reduce sample rate or switch to tail-based sampling
- If multi-tenant log isolation is required, route logs by tenant_id at the collector level

---

## Action Items

1. [ ] Implement `NewLogger(level, format string) zerolog.Logger` in `internal/observability/logger.go`
2. [ ] Implement `WithTaskFields(ctx, task)` context helper
3. [ ] Implement `InitTracer(ctx, cfg)` in `internal/observability/tracing.go`
4. [ ] Implement `PrometheusRecorder` struct with all metrics in `internal/observability/metrics.go`
5. [ ] Instrument all components with log events from the catalogue above
6. [ ] Instrument all components with OTel spans from the span catalogue above
7. [ ] Add `email_queue_depth` gauge scraper to `DLQMonitor` (or dedicated `QueueMonitor`)
8. [ ] Create Prometheus alerting rules file in `deployments/k8s/monitoring/`
9. [ ] Add `OTEL_EXPORTER_OTLP_ENDPOINT` to ConfigMap template
