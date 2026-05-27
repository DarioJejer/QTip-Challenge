# ADR-004: Worker Pool Concurrency Model

**Status:** Accepted
**Date:** 2026-05-27
**Deciders:** Engineering Team

---

## Context

The worker pool is the heart of the system. It must:
- Consume tasks from Redis Streams at up to 50k msgs/min
- Bound concurrency to prevent resource exhaustion (goroutines, Redis connections, email provider rate limits)
- Recover from individual task failures without affecting other workers
- Drain cleanly on SIGTERM without losing in-flight tasks
- Apply backpressure when downstream systems (email provider, idempotency store) are slow

Go's concurrency primitives — goroutines, channels, contexts, and the `sync` package — are the natural fit. This ADR defines the exact topology and lifecycle contracts.

---

## Decision

**We will use a supervisor goroutine managing a fixed-size semaphore-bounded worker pool.** Each task is processed in its own short-lived goroutine. Concurrency is bounded by a buffered channel semaphore. Backpressure is applied at the Redis XREADGROUP blocking call. Graceful shutdown uses `context.WithTimeout` for the drain budget.

---

## Goroutine Topology

```
┌─────────────────────────────────────────────────────────────────┐
│ main()                                                           │
│                                                                  │
│  signal.NotifyContext ──────────────────────────────────────────┐│
│                                                                  ││
│  errgroup.Go ──► WorkerSupervisor goroutine                     ││
│  errgroup.Go ──► DelayedScheduler goroutine                     ││
│  errgroup.Go ──► DLQMonitor goroutine                           ││
│  errgroup.Go ──► HTTP server goroutine (health + metrics)       ││
│                         │                                        ││
│              ┌──────────▼──────────┐                            ││
│              │  Supervisor loop    │◄── SIGTERM ────────────────┘│
│              │  XREADGROUP(BLOCK)  │                             │
│              └──────────┬──────────┘                             │
│                         │ task received                          │
│              ┌──────────▼──────────┐                             │
│              │  sem <- struct{}{}  │  acquire semaphore slot     │
│              │  (blocks if full)   │  ← backpressure point       │
│              └──────────┬──────────┘                             │
│                         │ slot acquired                          │
│              ┌──────────▼──────────────────────────────────┐    │
│              │  go processTask(ctx, task)                   │    │
│              │  ┌─────────────────────────────────────────┐│    │
│              │  │ defer func() { <-sem }()   release slot ││    │
│              │  │ defer recover()            panic guard  ││    │
│              │  │                                         ││    │
│              │  │ idempotencyStore.SetProcessing()        ││    │
│              │  │ emailSender.Send(ctx, task)             ││    │
│              │  │ consumer.Acknowledge() / Nack()         ││    │
│              │  │ retryScheduler / dlqWriter              ││    │
│              │  └─────────────────────────────────────────┘│    │
│              └─────────────────────────────────────────────┘    │
│                                                                  │
│  N concurrent processTask goroutines (N = WORKER_POOL_SIZE)     │
└─────────────────────────────────────────────────────────────────┘
```

---

## Pool Topology

### Supervisor Goroutine

A single long-running goroutine owns the XREADGROUP polling loop. It:
1. Calls `XREADGROUP GROUP email-workers <consumer-name> COUNT 10 BLOCK 5000 STREAMS queue:email:critical queue:email:high queue:email:normal queue:email:low >`
2. For each received message, acquires a semaphore slot and launches a `processTask` goroutine
3. Runs a stale-claim ticker every `WORKER_CLAIM_IDLE_THRESHOLD` (default 30s) via `XAUTOCLAIM`
4. Returns on context cancellation

### Worker Goroutines

Short-lived goroutines, one per task. Each:
- Receives a task and a child context derived from the supervisor's context
- Processes the task through the full pipeline (idempotency → send → ack/nack/retry/dlq)
- Releases the semaphore slot unconditionally via `defer`
- Has a `defer recover()` block for panic isolation

---

## Bounded Concurrency

### Why not `go func()` per task without a bound?

Unbounded goroutines create:
- **Memory exhaustion:** Each goroutine has an initial ~8KB stack. At 50k msgs/min with slow email providers, hundreds of goroutines would pile up
- **Redis connection saturation:** Each goroutine may hold a Redis connection; pool exhaustion causes cascading failures
- **Email provider overload:** Most providers impose rate limits; unbounded concurrency hammers them and triggers 429s

### Semaphore implementation

```go
// Semaphore using buffered channel — idiomatic Go
sem := make(chan struct{}, cfg.WorkerPoolSize) // e.g. 50 slots

// In supervisor loop:
sem <- struct{}{}  // acquire (blocks when full — this IS the backpressure)
go func(task *domain.EmailTask) {
    defer func() { <-sem }()  // release on completion
    processTask(ctx, task)
}(task)
```

`golang.org/x/sync/semaphore` is an alternative with `TryAcquire` support, but the buffered channel is simpler and sufficient. The blocking acquire is intentional — it is the backpressure mechanism.

### Pool size guidance

```
WORKER_POOL_SIZE = f(email_provider_rate_limit, redis_pool_size, cpu_cores)

Rule of thumb:
  WORKER_POOL_SIZE = min(
    email_provider_rate_limit_per_second * avg_send_latency_seconds,
    redis_pool_size * 0.7,
    cpu_cores * 10   // goroutines are cheap but not free
  )

Default: 50 (conservative; tune up based on load testing)
```

---

## Backpressure

The backpressure chain works as follows:

```
Email provider slow / error rate high
  → processTask goroutines take longer
  → semaphore fills up
  → supervisor blocks on semaphore acquire (sem <- struct{}{})
  → XREADGROUP is not called
  → Redis messages accumulate in the stream (PEL grows)
  → Queue depth Prometheus gauge rises
  → HPA triggers scale-out (new pods consume from the same consumer group)
```

This is a clean, natural backpressure path. No explicit rate limiter is needed at the worker level — the semaphore IS the rate limiter.

---

## Worker Lifecycle

### Context propagation

```go
// Supervisor has rootCtx from signal.NotifyContext
// Each worker gets a child context with task-scoped values
workerCtx := context.WithValue(rootCtx, taskContextKey, task)
// If rootCtx is cancelled (SIGTERM), workerCtx.Done() fires in all workers
```

### On context cancellation

When `rootCtx` is cancelled (SIGTERM received):
1. Supervisor stops calling XREADGROUP (select on `ctx.Done()`)
2. In-flight workers receive `workerCtx.Done()` on their next context-aware call
3. Workers must **NACK** (not XACK) — the message stays in the PEL for redelivery by another pod
4. Workers must NOT drop results that are already complete — check if `emailSender.Send` succeeded before deciding ACK vs NACK

```go
func processTask(ctx context.Context, task *domain.EmailTask) {
    defer func() { <-sem }()

    err := emailSender.Send(ctx, task)
    if err != nil {
        if errors.Is(err, context.Canceled) {
            consumer.Nack(ctx, task, err)  // let another worker retry
            return
        }
        // handle retryable / non-retryable errors
    }
    consumer.Acknowledge(context.Background(), task)  // use Background — ctx may be cancelled
}
```

Note: `Acknowledge` uses `context.Background()` not the worker context — we must ACK even if the root context is cancelled, otherwise a successfully processed task re-enters the queue.

---

## Panic Recovery

Every worker goroutine wraps its execution in a `recover()` block:

```go
defer func() {
    if r := recover(); r != nil {
        // 1. Log the panic with stack trace
        logger.Error().
            Interface("panic", r).
            Str("task_id", task.ID).
            Bytes("stack", debug.Stack()).
            Msg("worker.panic")

        // 2. Mark task as poison in metadata
        task.Metadata["poison"] = "true"
        task.Metadata["panic_reason"] = fmt.Sprintf("%v", r)

        // 3. NACK — message stays in PEL; XAUTOCLAIM will reassign
        // On reassignment, IsPoisonMessage() returns true → routes to DLQ
        consumer.Nack(context.Background(), task, fmt.Errorf("panic: %v", r))

        // 4. Record metric
        metrics.RecordProcessed(ctx, task.TenantID, task.Type, "panic", 0)
    }
}()
```

The panic is isolated to one goroutine. The supervisor continues processing other tasks. The panicked task is NACKed and will be reclaimed by XAUTOCLAIM; on re-processing, `IsPoisonMessage()` detects the metadata flag and routes directly to DLQ.

---

## Graceful Shutdown

### Shutdown sequence on SIGTERM

```
T+0s    signal.NotifyContext fires → rootCtx cancelled
T+0s    router.SetReady(false)     → /readyz returns 503
T+0s    Supervisor stops XREADGROUP polling
T+0s    In-flight workers receive ctx.Done() on next context check
T+0-30s Workers complete or NACK and exit; semaphore slots released
T+30s   Drain timeout reached (DRAIN_TIMEOUT_SECONDS=30)
T+30s   Log warning if slots still held
T+30s   Close Redis connections
T+30s   Flush OTel traces/metrics
T+30s   os.Exit(0)
T+60s   K8s sends SIGKILL (terminationGracePeriodSeconds=60)
```

### Drain implementation

```go
func (s *Supervisor) drain(timeout time.Duration) {
    deadline := time.Now().Add(timeout)
    ticker := time.NewTicker(5 * time.Second)
    defer ticker.Stop()

    for time.Now().Before(deadline) {
        inFlight := s.cfg.WorkerPoolSize - len(s.sem)
        if inFlight == 0 {
            logger.Info().Msg("shutdown.drained")
            return
        }
        select {
        case <-ticker.C:
            logger.Warn().Int("in_flight", inFlight).Msg("shutdown.draining")
        }
    }
    logger.Warn().Msg("shutdown.drain_timeout_exceeded")
}
```

---

## Stale Message Recovery

A ticker runs every `WORKER_CLAIM_IDLE_THRESHOLD` (default 30s) to reclaim messages whose workers crashed:

```go
// XAUTOCLAIM reclaims messages idle > threshold and delivers them to this consumer
result, err := client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
    Stream:   queueName,
    Group:    cfg.ConsumerGroup,
    Consumer: cfg.ConsumerName,
    MinIdle:  cfg.ClaimIdleThreshold, // 30s
    Start:    "0-0",
    Count:    100,
}).Result()
```

Reclaimed messages are re-inserted into the local task channel for processing.

---

## Consequences

**What becomes easier:**
- Memory usage is predictable: bounded goroutines + bounded Redis pool
- Graceful shutdown is deterministic: semaphore drain gives exact in-flight count
- Panic isolation means one bad task cannot crash the supervisor
- Backpressure is automatic: semaphore blocks → XREADGROUP not called → stream accumulates → HPA scales out

**What becomes harder:**
- WORKER_POOL_SIZE must be tuned per deployment — too low under-utilizes the pod, too high overloads the email provider
- The `context.Background()` in Acknowledge calls is intentional but non-obvious; must be documented clearly
- Stale claim threshold must be greater than the maximum expected task processing time to avoid spurious redelivery

**What we will need to revisit:**
- If email provider rate limits require per-tenant throttling, we need per-tenant semaphores or a token bucket rate limiter per tenant
- If task processing time varies wildly, a work-stealing scheduler may be more efficient than a fixed pool

---

## Action Items

1. [ ] Implement `WorkerSupervisor` in `internal/worker/supervisor.go`
2. [ ] Implement semaphore as `make(chan struct{}, cfg.WorkerPoolSize)`
3. [ ] Implement `drain(timeout)` method with 5s progress logging
4. [ ] Add `XAUTOCLAIM` stale-claim ticker in supervisor loop
5. [ ] Ensure `Acknowledge` always uses `context.Background()` after successful send
6. [ ] Add panic recovery with poison-message metadata tagging in every worker goroutine
7. [ ] Export `email_queue_worker_active` and `email_queue_worker_pool_size` Prometheus gauges
