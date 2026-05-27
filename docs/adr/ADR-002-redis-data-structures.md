# ADR-002: Redis Data Structures — LIST vs Streams

**Status:** Accepted
**Date:** 2026-05-27
**Deciders:** Engineering Team

---

## Context

Having decided to use Redis as our message broker (ADR-001), we must choose the specific Redis data structures for each queue role in the system. The choice directly affects delivery guarantees, crash recovery behaviour, operational complexity, and throughput.

The system has four distinct queue roles:

| Role | Description |
|------|-------------|
| **Main work queue** | High-throughput FIFO queue per priority band; tasks consumed by the worker pool |
| **Retry queue** | Stores tasks awaiting delayed re-enqueue after a failed processing attempt |
| **Delayed task queue** | Schedules tasks for future delivery (e.g. `ScheduledFor` field set by producer) |
| **Dead-letter queue (DLQ)** | Permanent storage for tasks that have exhausted all retry attempts |

The two primary candidate data structures are **Redis LIST** and **Redis Streams**, with **Redis Sorted Set** as a natural fit for time-based scheduling.

---

## Decision

| Queue Role | Data Structure | Redis Commands Used |
|------------|---------------|---------------------|
| Main work queue (per priority) | **Redis Streams** | `XADD`, `XREADGROUP`, `XACK`, `XCLAIM`, `XAUTOCLAIM` |
| Retry queue | **Redis Streams** | Same as main queue; retried tasks are re-added via `XADD` |
| Delayed task queue | **Redis Sorted Set** | `ZADD`, `ZRANGEBYSCORE`, `ZREM` |
| Dead-letter queue | **Redis LIST** | `RPUSH`, `LRANGE`, `LLEN`, `EXPIRE` |

**We will use Redis Streams for the main and retry queues, a Redis Sorted Set for delayed scheduling, and a Redis LIST for the DLQ.**

---

## Options Considered

### Option A: Redis LIST (RPUSH / BRPOP)

```
Producer: RPUSH queue:email:normal <payload>
Worker:   BRPOP queue:email:normal <timeout>
```

| Dimension | Assessment |
|-----------|------------|
| Complexity | Low — two commands, simple mental model |
| Throughput | ~100k ops/sec; O(1) push and pop |
| Consumer groups | ❌ None — single consumer per queue |
| Crash recovery | ❌ None — message is lost if worker crashes after BRPOP but before processing |
| Acknowledgement | ❌ None — dequeue is destructive and non-reversible |
| Observability | `LLEN` for depth; no per-message metadata |
| Go client | Excellent |

**Mitigation for lost messages:** A "processing" shadow set (`SADD processing:<id>`) with a visibility timeout heartbeat can simulate ACK semantics, but this requires a separate background loop to reclaim stale entries — adding complexity that Streams provide natively.

**Pros:**
- Extremely simple to reason about
- Lowest latency: single round trip to dequeue
- Well understood by every Redis user

**Cons:**
- No acknowledgement — a crashed worker silently drops the message
- Requires custom visibility timeout implementation for crash recovery
- No consumer group semantics — scaling to multiple workers requires application-level sharding
- No per-message metadata or delivery tracking

---

### Option B: Redis Streams (XADD / XREADGROUP) — Selected for main & retry queues

```
Producer: XADD queue:email:high * id <uuid> payload <json> tenant_id <tid>
Worker:   XREADGROUP GROUP email-workers worker-1 COUNT 10 BLOCK 5000 STREAMS queue:email:high >
          XACK queue:email:high email-workers <message-id>   # on success
          # on crash: message stays in PEL, reclaimed via XCLAIM / XAUTOCLAIM
```

| Dimension | Assessment |
|-----------|------------|
| Complexity | Medium — consumer groups, PEL, XCLAIM |
| Throughput | ~80–100k msgs/sec; negligible overhead vs LIST |
| Consumer groups | ✅ Native — multiple worker pods share a group |
| Crash recovery | ✅ PEL tracks unacknowledged messages; XCLAIM reassigns after idle threshold |
| Acknowledgement | ✅ Explicit XACK required — at-least-once delivery |
| Observability | `XLEN`, `XPENDING` for depth and PEL monitoring |
| Go client | Excellent (`go-redis/v9` has full Streams support) |

**PEL (Pending Entry List):** Every message consumed via XREADGROUP is tracked in the PEL until explicitly XACKed. If a worker crashes, the message remains in the PEL. A background goroutine calls `XAUTOCLAIM` periodically to reassign messages idle longer than a threshold (e.g. 30s) to another worker.

**Pros:**
- Built-in consumer groups — multiple pods share work without application-level coordination
- Crash recovery is a first-class primitive (PEL + XCLAIM/XAUTOCLAIM)
- Per-message metadata stored in the stream entry
- `XPENDING` provides rich visibility into unacknowledged messages
- Stream trimming (`MAXLEN ~`) bounds memory usage

**Cons:**
- Higher conceptual complexity: consumer groups, PEL, XCLAIM, message IDs
- `XAUTOCLAIM` requires Redis 7.0+ (widely available)
- Slightly more verbose Go code vs BRPOP

---

### Option C: Redis Sorted Set (ZADD / ZRANGEBYSCORE) — Selected for delayed queue

```
Enqueue delayed:  ZADD queue:email:delayed NX <unix_timestamp> <json_payload>
Scheduler flush:  ZRANGEBYSCORE queue:email:delayed -inf <now> LIMIT 0 100
                  → pipeline: XADD main-queue + ZREM delayed-queue
```

| Dimension | Assessment |
|-----------|------------|
| Time-based scheduling | ✅ Native — score = unix timestamp of intended delivery |
| Deduplication | ✅ NX flag prevents double-scheduling same task |
| Atomicity | Pipeline XADD + ZREM is not atomic; partial failure handled by idempotent re-enqueue |
| Complexity | Low — simple scheduler goroutine polls every second |

This is the natural fit for delayed/scheduled tasks. No alternative comes close for simplicity at this use case.

---

### Option D: Redis LIST — Selected for DLQ only

```
Dead-letter: RPUSH queue:dlq:tenant-a:registration <json_dlq_entry>
             EXPIRE queue:dlq:tenant-a:registration 604800   # 7 days
Inspection:  LRANGE queue:dlq:tenant-a:registration 0 99
```

The DLQ is an **append-only inspection log**. It has no consumers — ops engineers read it directly. Redis LIST is the correct data structure: RPUSH for append, LRANGE for paginated inspection, LLEN for depth monitoring. No consumer group semantics are needed.

---

## Detailed Tradeoff: LIST vs Streams for the Main Queue

The core question is: **what happens when a worker crashes between dequeue and ACK?**

### With LIST + BRPOP:
```
Worker calls BRPOP → message removed from queue → worker crashes → message GONE
```
Recovery requires an external mechanism (visibility timeout with shadow set), which adds:
- A background heartbeat goroutine per worker
- A stale-entry reaper goroutine
- Coordination between these goroutines under shutdown

### With Streams + XREADGROUP:
```
Worker calls XREADGROUP → message moves to PEL (not removed) → worker crashes
→ XAUTOCLAIM after idle threshold → message reassigned to another worker
```
Crash recovery is built-in. The PEL is the source of truth for in-flight messages.

At 50k msgs/min with a worker pool of 50, the operational burden of a custom visibility timeout implementation is not justified when Streams provide this natively.

---

## Queue Key Naming Conventions

```
Main queues (Streams):
  queue:email:critical    # Priority 3
  queue:email:high        # Priority 2
  queue:email:normal      # Priority 1
  queue:email:low         # Priority 0

Delayed queue (Sorted Set):
  queue:email:delayed

Dead-letter queues (LIST, one per tenant/type):
  queue:dlq:{tenantID}:{taskType}

Consumer group name (same for all streams):
  email-workers

Consumer name (per pod):
  {hostname}:{pid}
```

> **Note on Redis Cluster:** All queue keys should use hash tags to ensure co-location on the same slot when using Redis Cluster: e.g. `{queue}:email:high`. This is required for multi-key Lua scripts.

---

## Consequences

**What becomes easier:**
- Crash recovery is automatic via PEL and XAUTOCLAIM — no custom heartbeat needed
- Queue depth monitoring via `XLEN` and `XPENDING` is straightforward
- Multiple worker pods share the same consumer group without application-level coordination
- DLQ inspection is a simple `LRANGE` command, accessible from any Redis client

**What becomes harder:**
- Worker code must explicitly XACK on success and handle the no-XACK path on failure
- Consumer group must be created before workers start (handled by migration runner)
- Stream memory must be bounded with `MAXLEN ~` trimming to prevent unbounded growth
- XAUTOCLAIM idle threshold must be tuned: too short causes unnecessary redelivery; too long delays recovery

**What we will need to revisit:**
- If we need strict per-tenant ordering, we would need one stream per tenant (increases stream count significantly)
- If stream depth monitoring reveals memory pressure, evaluate aggressive MAXLEN trimming or message compression

---

## Action Items

1. [ ] Create consumer groups for all four priority streams on service startup (idempotent XGROUP CREATE ... MKSTREAM)
2. [ ] Implement XAUTOCLAIM background loop with configurable idle threshold (default 30s)
3. [ ] Set `MAXLEN ~ 100000` on all XADD calls to bound stream memory
4. [ ] Use `{queue}` hash tag prefix on all key names for Redis Cluster compatibility
5. [ ] Expose `XLEN` and `XPENDING` counts as Prometheus gauges (see ADR-007)
6. [ ] Set 7-day EXPIRE on all DLQ keys after each RPUSH
