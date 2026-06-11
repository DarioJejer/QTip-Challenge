# ADR-005: Retry Architecture & Dead-Letter Queue

**Status:** Accepted
**Date:** 2026-05-27
**Deciders:** Engineering Team

---

## Context

Email delivery failures are inevitable: SMTP providers return 5xx errors, templates fail to render, recipient addresses are invalid, and downstream APIs time out. The system must handle these failures gracefully with automatic retries, while preventing retry storms that could overload providers and protecting the queue from poison messages that can never succeed.

Key requirements:
- Retries must not hammer a failing provider (exponential backoff with jitter)
- Tasks that exhaust retries must be preserved for operator inspection (DLQ)
- Non-retryable failures (invalid email, template not found) must bypass retry entirely
- Retry delays must be stored durably so they survive pod restarts
- The retry system must not amplify message count (no retry storms)

---

## Decision

**We will implement exponential backoff with jitter using a Redis Sorted Set as the delayed retry store, a separate scheduler goroutine to flush ready tasks, and a Redis LIST as the DLQ with a 7-day TTL.**

---

## Retry State Machine

```
                    ┌─────────┐
                    │ PENDING │  Task enqueued to Redis Stream
                    └────┬────┘
                         │ XREADGROUP
                         ▼
                  ┌─────────────┐
              ┌──►│ PROCESSING  │  Worker acquired semaphore slot
              │   └──────┬──────┘
              │          │
              │    ┌─────┴──────┐
              │    │            │
              │  success      failure
              │    │            │
              │    ▼            ▼
              │ ┌───────┐  ┌────────────────────┐
              │ │SUCCESS│  │ IsRetryable(err)?   │
              │ │(done) │  └──────────┬──────────┘
              │ └───────┘            │
              │               ┌──────┴──────┐
              │            yes│             │no / poison
              │               ▼             ▼
              │   ┌───────────────────┐  ┌──────┐
              │   │attempt<maxAttempts│  │ DEAD │──► RPUSH DLQ
              │   └────────┬──────────┘  └──────┘
              │        ┌───┴───┐
              │      yes│       │no
              │         ▼       ▼
              │  ┌──────────┐ ┌──────┐
              └──│  RETRY   │ │ DEAD │──► RPUSH DLQ
                 │SCHEDULED │ └──────┘
                 └────┬─────┘
                      │ ZADD delayed set
                      │ (score = now + backoff)
                      ▼
              ┌───────────────┐
              │ Scheduler poll│ ZRANGEBYSCORE every 1s
              └───────┬───────┘
                      │ XADD back to main stream
                      └──────────────────────────► PENDING (attempt+1)
```

**Terminal states:** SUCCESS and DEAD. All other states are transient.

---

## Exponential Backoff Formula

```go
func (r *RetryPolicy) ComputeDelay(attempt int) time.Duration {
    // delay = baseDelay * 2^attempt
    delay := float64(r.BaseDelay) * math.Pow(2, float64(attempt))

    // Cap at maxDelay
    if delay > float64(r.MaxDelay) {
        delay = float64(r.MaxDelay)
    }

    // Add jitter: uniform random in [0, delay * jitterFactor]
    // math/rand is fine here — this is not a security context
    jitter := rand.Float64() * delay * r.JitterFactor
    delay += jitter

    return time.Duration(delay)
}

// Default policy
var DefaultRetryPolicy = RetryPolicy{
    BaseDelay:    1 * time.Second,
    MaxDelay:     15 * time.Minute,
    JitterFactor: 0.2,  // ±20% jitter
}
```

### Retry delay schedule (default policy, no jitter)

| Attempt | Delay (no jitter) | Delay range (with 20% jitter) |
|---------|-------------------|-------------------------------|
| 0 (1st retry) | 1s | 1.0s – 1.2s |
| 1 | 2s | 2.0s – 2.4s |
| 2 | 4s | 4.0s – 4.8s |
| 3 | 8s | 8.0s – 9.6s |
| 4 | 16s | 16.0s – 19.2s |
| 5 | 32s | 32.0s – 38.4s |
| 6 | 64s | 64.0s – 76.8s |
| 7+ | 15m (capped) | 15m – 18m |

### Why jitter is non-negotiable at scale

Without jitter, all tasks that fail simultaneously (e.g. during a provider outage) are scheduled for the same retry time. When the outage resolves, all tasks become ready simultaneously — creating a **retry storm** that overwhelms the provider with a spike equal to the entire backlog.

With ±20% jitter, retries are spread across a 20% time window. At 10k queued retries with a 15-minute cap, jitter spreads the retry load over ~3 minutes instead of delivering it as a single spike.

---

## Delayed Retry Implementation

Implemented in `internal/adapters/redis/retry_scheduler.go` (M3-04) and driven by
`internal/worker/delayed_scheduler.go`.

### Responsibility split: supervisor vs scheduler

| Step | Owner | Behaviour |
|------|-------|-----------|
| `task.Attempt++` | **Worker supervisor** | Incremented before `ScheduleRetry` so the sorted-set member carries the correct next-attempt count |
| `ZADD NX` | **RedisRetryScheduler** | Stores task JSON as-is; does **not** increment `Attempt` again |
| `XACK` from PEL | **Worker supervisor** | Only after `ScheduleRetry` succeeds; on failure calls `clearLockAndNack` (see below) |
| `FlushReady` | **DelayedScheduler** | Ticker at `cfg.Retry.SchedulerInterval` (default 1 s) |

### Enqueue to delayed sorted set

```go
func (s *RedisRetryScheduler) ScheduleRetry(ctx context.Context, task *domain.EmailTask, delay time.Duration) error {
    // task.Attempt was already incremented by the supervisor.
    payload, err := json.Marshal(task)
    scheduledFor := time.Now().Add(delay)

    return s.client.ZAddArgs(ctx, "queue:email:retry:delayed", redis.ZAddArgs{
        NX:      true,
        Members: []redis.Z{{Score: float64(scheduledFor.Unix()), Member: string(payload)}},
    }).Err()
    // NX: skip if already scheduled — prevents double-scheduling on PEL redelivery
}
```

### Scheduler goroutine (flushes ready tasks every second)

```go
func (d *DelayedScheduler) Run(ctx context.Context) error {
    ticker := time.NewTicker(d.cfg.Retry.SchedulerInterval) // 1s
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return nil
        case <-ticker.C:
            if _, err := d.retryScheduler.FlushReady(ctx); err != nil {
                logger.Error().Err(err).Msg("delayed scheduler: FlushReady error")
            }
        }
    }
}
```

### FlushReady (re-enqueue pipeline)

```go
func (s *RedisRetryScheduler) FlushReady(ctx context.Context) ([]*domain.EmailTask, error) {
    // 1. ZRANGEBYSCORE queue:email:retry:delayed -inf now COUNT 100
    // 2. Unmarshal each member; corrupt JSON → quarantine (see below)
    // 3. Pipeline: XADD per valid task + single ZREM of all valid members
    // 4. Per-XADD failures logged; entry stays in set for next tick
    // 5. ZCARD → RecordQueueDepth("retry:delayed", depth)  // full set size, not batch delta

    pipe.XAdd(ctx, &redis.XAddArgs{
        Stream: task.Priority.QueueName(),
        Values: XAddValuesBuilder(task, payload), // canonical schema — ADR-003
    })
}
```

### Canonical stream entry on re-enqueue

All XADD paths (HTTP producer, retry flush, future schedulers) share
`XAddValuesBuilder` in `internal/adapters/redis/stream_entry.go`:

```go
func XAddValuesBuilder(task *domain.EmailTask, payload string) map[string]any {
    return map[string]any{
        "id": task.ID, "payload": payload,
        "enqueued_at": task.EnqueuedAt.UnixNano(),
        "tenant_id": task.TenantID, "task_type": string(task.Type),
        "priority": task.Priority.String(),
        "attempt": task.Attempt, "trace_id": task.TraceID,
    }
}
```

The consumer unmarshals only `payload`; top-level fields exist for `redis-cli XRANGE`
inspection and operational tooling (ADR-002, ADR-003).

### Corrupt retry entry quarantine

Unparseable sorted-set members cannot be flushed and must not remain in the delay
queue (they would be re-read every tick and can block the 100-entry batch limit).

**Poison list:** `queue:retry:poison` (Redis LIST)

On JSON unmarshal failure during `FlushReady`:

1. Build a `retryPoisonEntry` envelope (`raw_member`, `parse_error`, `quarantined_at`, `source_key`)
2. Pipeline: `RPUSH queue:retry:poison` + `ZREM queue:email:retry:delayed`
3. `EXPIRE queue:retry:poison` with `cfg.Retry.DLQTTLSeconds` (7 days, same as DLQ)

Inspect via `redis-cli LRANGE queue:retry:poison 0 -1`. This is **not** the tenant
DLQ — corrupt bytes cannot be routed to `queue:dlq:{tenant}:{type}`.

### Worker ACK ordering and PEL safety

The supervisor only XACKs after durable handoff:

```
Send fails (retryable)
  → task.Attempt++
  → ScheduleRetry OK  → XACK          (retry owned by sorted set)
  → ScheduleRetry FAIL → ClearProcessing + Nack  (stay in PEL)

Send fails (terminal) / max attempts
  → SendToDLQ OK  → XACK
  → SendToDLQ FAIL → ClearProcessing + Nack
```

`ClearProcessing` releases the attempt-scoped idempotency lock (`{taskID}#{attempt}`)
before Nack. Without this, a PEL reclaim would see `acquired=false` and silently
drop the task. If `ClearProcessing` itself fails, `TryReclaimStale` at the
idempotency gate (ADR-006) recovers once the lock age exceeds `ClaimIdleThreshold`.

---

## Max Attempts by Task Type

```go
var DefaultMaxAttempts = map[domain.TaskType]int{
    domain.TaskTypeRegistration:  5,
    domain.TaskTypePasswordReset: 5,
    domain.TaskTypeBilling:       5,
    domain.TaskTypeMarketing:     3,
    domain.TaskTypeSecurity:      7,
    domain.TaskTypeTransactional: 5,
}
```

These defaults are stored in `task.MaxAttempts` at enqueue time. Override via tenant config or per-request metadata.

---

## Poison Message Detection

A task is considered a **poison message** if:

1. **Worker panic:** The task caused a `panic()` in a worker goroutine. The panic recovery sets `task.Metadata["poison"] = "true"`. On the next processing attempt (after XCLAIM), `IsPoisonMessage()` detects this and routes directly to DLQ.

2. **Non-retryable error:** The email sender returns a sentinel error `ErrNonRetryable`. Examples:
   - HTTP 400 from SendGrid: invalid recipient address format
   - HTTP 404: template not found
   - `context.DeadlineExceeded` after exhausting the task deadline (not a provider error)

```go
func (t *EmailTask) IsPoisonMessage() bool {
    return t.Metadata["poison"] == "true"
}

func IsRetryable(err error) bool {
    if errors.Is(err, ErrNonRetryable) { return false }
    if errors.Is(err, ErrCircuitOpen)  { return false }  // circuit open = don't retry now
    if errors.Is(err, context.Canceled) { return false } // shutdown path
    return true
}
```

Poison messages skip the retry state entirely and go directly to DLQ with `FailureReason = "poison_message"` or `"non_retryable_error"`.

---

## DLQ Architecture

### Key schema

```
queue:dlq:{tenantID}:{taskType}

Examples:
  queue:dlq:acme-corp:registration
  queue:dlq:acme-corp:marketing
  queue:dlq:startup-inc:security
```

### DLQ entry structure

```go
type DLQEntry struct {
    Task           *EmailTask      `json:"task"`            // Full task snapshot
    DeadAt         time.Time       `json:"dead_at"`
    FailureReason  string          `json:"failure_reason"`  // "max_attempts_exceeded" | "poison_message" | "non_retryable_error"
    FinalError     string          `json:"final_error"`
    AttemptHistory []AttemptRecord `json:"attempt_history"` // Full history of all attempts
}

type AttemptRecord struct {
    Attempt   int       `json:"attempt"`
    StartedAt time.Time `json:"started_at"`
    FailedAt  time.Time `json:"failed_at"`
    Error     string    `json:"error"`
    WorkerID  string    `json:"worker_id"`
}
```

### Write path

```go
func (w *RedisDLQWriter) SendToDLQ(ctx context.Context, entry *domain.DLQEntry) error {
    payload, _ := json.Marshal(entry)
    key := fmt.Sprintf("queue:dlq:%s:%s", entry.Task.TenantID, entry.Task.Type)

    pipe := w.client.Pipeline()
    pipe.RPush(ctx, key, payload)
    pipe.Expire(ctx, key, 7*24*time.Hour)  // reset TTL on each new entry
    _, err := pipe.Exec(ctx)
    return err
}
```

### DLQ Monitor

```go
// Runs every 30s; scrapes LLEN for all known DLQ keys
func (m *DLQMonitor) Run(ctx context.Context) error {
    ticker := time.NewTicker(30 * time.Second)
    for {
        select {
        case <-ctx.Done(): return nil
        case <-ticker.C:
            for _, key := range m.knownDLQKeys {
                depth, _ := m.client.LLen(ctx, key).Result()
                m.metrics.RecordDLQDepth(ctx, tenantID, taskType, depth)
                if depth > m.alertThreshold {
                    logger.Warn().Str("key", key).Int64("depth", depth).Msg("dlq.depth_threshold_exceeded")
                }
            }
        }
    }
}
```

---

## Retry Storm Mitigation

| Mechanism | Description |
|-----------|-------------|
| Jitter | ±20% random spread prevents synchronized retries |
| Max concurrent retries cap | A separate semaphore limits simultaneous in-flight retries to `WORKER_POOL_SIZE / 2` |
| Circuit breaker | Per email-provider circuit breaker opens after 5 consecutive failures; returns `ErrCircuitOpen` (non-retryable until circuit resets at 30s) |
| Exponential backoff cap | 15-minute maximum delay bounds the retry queue growth |
| DLQ routing | Tasks that exceed `MaxAttempts` leave the retry system entirely |

---

## Consequences

**What becomes easier:**
- All failed tasks are preserved in the DLQ for operator inspection and manual replay
- Retry delays survive pod restarts (stored in Redis Sorted Set, not in-process memory)
- Jitter eliminates retry storm risk at scale
- Poison message detection prevents infinite loop on fundamentally broken tasks

**What becomes harder:**
- The delayed scheduler is a single goroutine — it is a mild single point of contention (mitigated by processing batches of 100 per tick)
- ZADD NX prevents double-scheduling but means a task can only appear once in the delayed set — if a task is reclaimed by XCLAIM and retried while still in the delayed set, the NX ensures only one copy exists
- DLQ key discovery is static (`knownDLQKeys` list) — dynamic tenant onboarding requires updating this list or using Redis SCAN

**What we will need to revisit:**
- If retry queue depth exceeds 100k entries, the ZRANGEBYSCORE batch size should be increased or the scheduler should run more frequently
- If tenants need per-tenant retry policies, `RetryPolicy` should be looked up from `TenantConfig` at scheduling time

---

## Action Items

1. [x] Implement `RetryPolicy.ComputeDelay(attempt int)` in `internal/domain/`
2. [x] Implement `RedisRetryScheduler` in `internal/adapters/redis/retry_scheduler.go`
3. [x] Implement `DelayedScheduler.Run()` goroutine in `internal/worker/delayed_scheduler.go`
4. [x] Implement canonical `XAddValuesBuilder` in `internal/adapters/redis/stream_entry.go`
5. [x] Implement corrupt-entry quarantine to `queue:retry:poison`
6. [x] Scrape `email_queue_depth{queue="retry:delayed"}` via `ZCARD` after each flush
7. [ ] Implement `RedisDLQWriter.SendToDLQ()` in `internal/adapters/redis/dlq_writer.go`
8. [ ] Implement `DLQMonitor.Run()` goroutine in `internal/worker/dlq_monitor.go`
9. [ ] Define `ErrNonRetryable`, `ErrCircuitOpen` sentinel errors in `internal/ports/errors.go`
10. [ ] Wire circuit breaker into `SendGridSender` (5 failures → 30s open)
