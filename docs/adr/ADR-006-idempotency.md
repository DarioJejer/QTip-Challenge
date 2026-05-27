# ADR-006: Idempotency & Duplicate Delivery Protection

**Status:** Accepted
**Date:** 2026-05-27
**Deciders:** Engineering Team

---

## Context

Redis Streams with consumer groups provide **at-least-once** delivery. This means a message may be delivered more than once in two scenarios:

1. **Worker crash after processing but before XACK:** The message remains in the PEL and is reclaimed by XAUTOCLAIM. A new worker processes it again.
2. **Network partition or Redis failover:** In rare cases, an XACK may not be durably recorded before a failover, causing redelivery.

Email is an externally visible side effect. Sending the same email twice to a user is a product defect with real consequences (user confusion, duplicate billing notifications, double password reset links). Therefore, **exactly-once email delivery must be enforced at the application layer**, on top of the at-least-once transport guarantee.

---

## Decision

**We will use a Redis-backed idempotency store with a Lua atomic check-and-set script to prevent duplicate email delivery within a 24-hour deduplication window.**

Producers assign deterministic UUIDs to tasks. Workers check the idempotency store before processing and mark tasks as completed after successful delivery. Duplicate tasks are silently acknowledged and skipped.

---

## Idempotency Key Schema

```
Redis key:  idempotency:{taskID}
Value:      JSON blob (see below)
TTL:        86400 seconds (24 hours)
```

### Value schema

```go
type IdempotencyRecord struct {
    Status      string    `json:"status"`       // "processing" | "completed"
    WorkerID    string    `json:"worker_id"`
    StartedAt   time.Time `json:"started_at"`
    CompletedAt *time.Time `json:"completed_at,omitempty"`
}
```

### TTL rationale

- **24 hours** covers the full retry window (max 7 retries × 15-minute max delay = ~1.75 hours worst case)
- After 24 hours, a task is considered safe to re-execute (e.g. manual ops replay)
- 24 hours × (active task IDs at any moment) is a small Redis memory footprint: at 50k msgs/min and 100% dedup rate, ~72M keys × ~100 bytes ≈ ~7GB worst case. In practice, dedup rates are low and TTLs expire continuously, keeping memory usage well under 1GB

---

## Check-and-Set Flow

### Non-atomic two-phase approach (illustrative, not used)

```
Worker A:  GET idempotency:{id}   → nil (not seen)
Worker A:  SET idempotency:{id} processing NX EX 86400  → OK (acquired)
Worker A:  emailSender.Send()
Worker A:  SET idempotency:{id} completed XX EX 86400   → OK (updated)
Worker A:  XACK stream group msgID

Worker B (duplicate redelivery):
Worker B:  GET idempotency:{id}   → "completed"  → XACK + skip
```

**TOCTOU race:** Between `GET` and `SET NX`, two workers could both see `nil` and both attempt to acquire. The `SET NX` is atomic and resolves this — only one succeeds. However, a two-command sequence still has a window where a crash between `GET` and `SET NX` leaves no record. The Lua script below eliminates this entirely.

---

## Lua Atomic Check-and-Set Script

The production implementation uses a single Lua script executed atomically on the Redis server, eliminating all TOCTOU races:

```lua
-- idempotency_cas.lua
-- KEYS[1] = idempotency key
-- ARGV[1] = worker_id
-- ARGV[2] = ttl (seconds)
-- ARGV[3] = started_at (RFC3339)
-- Returns: 1 if lock acquired, 0 if already processing or completed

local key = KEYS[1]
local existing = redis.call('GET', key)

if existing then
    local data = cjson.decode(existing)
    -- Already completed: skip (idempotent)
    if data['status'] == 'completed' then
        return 0
    end
    -- Already processing by another worker: skip
    if data['status'] == 'processing' then
        return 0
    end
end

-- Not seen before: acquire processing lock
local payload = cjson.encode({
    status     = 'processing',
    worker_id  = ARGV[1],
    started_at = ARGV[3]
})
redis.call('SET', key, payload, 'EX', tonumber(ARGV[2]))
return 1
```

### Go usage

```go
// Script is loaded at startup via SCRIPT LOAD; SHA stored in ScriptRegistry
acquired, err := client.EvalSha(ctx, scripts.IdempotencyCAS,
    []string{fmt.Sprintf("idempotency:%s", task.ID)},
    workerID,
    strconv.Itoa(cfg.IdempotencyTTLSeconds),
    time.Now().Format(time.RFC3339Nano),
).Int()

if err != nil {
    return false, fmt.Errorf("idempotency: evalsha: %w", err)
}
return acquired == 1, nil
```

### Mark completed (after successful send)

```go
// SET XX: only update if key exists (prevents ghost completions)
completedAt := time.Now()
payload, _ := json.Marshal(IdempotencyRecord{
    Status:      "completed",
    WorkerID:    workerID,
    CompletedAt: &completedAt,
})
err = client.Set(ctx, fmt.Sprintf("idempotency:%s", task.ID),
    payload, time.Duration(cfg.IdempotencyTTLSeconds)*time.Second,
).Err()
```

Note: `SET` without `XX` here is intentional — we always want to mark completion, even if the processing record expired. `XX` would silently fail if the TTL expired between lock acquisition and completion.

---

## Edge Cases

### Worker crashes after SetProcessing but before SetCompleted

```
Worker A: SetProcessing → acquired=true  ✓
Worker A: emailSender.Send() → success
Worker A: CRASHES before SetCompleted and XACK
---
PEL: message is still pending (no XACK)
XAUTOCLAIM: after idle threshold, message reassigned to Worker B
Worker B: SetProcessing → acquired=false (status="processing") → skip
```

**Problem:** Worker A successfully sent the email but died before marking complete. Worker B sees `status=processing` and skips. The email was sent once — correct. But the task is never ACKed.

**Resolution:** After `XAUTOCLAIM`, if the idempotency record shows `status=processing` for longer than `2 × max_task_duration` (e.g. 2 minutes), treat it as a stale lock and attempt re-acquisition. A second Lua script (`idempotency_reclaim.lua`) handles this:

```lua
-- Reclaims a stale processing lock older than stale_threshold_seconds
local key = KEYS[1]
local stale_threshold = tonumber(ARGV[1])  -- seconds
local existing = redis.call('GET', key)
if not existing then return 1 end  -- expired, safe to reacquire
local data = cjson.decode(existing)
if data['status'] == 'completed' then return 0 end  -- already done, skip
-- Check staleness (requires started_at in the record)
-- If started_at is old enough, overwrite with new processing record
-- ... (implementation details in internal/adapters/redis/idempotency.go)
return 1
```

### TTL expires between attempts

If the idempotency key expires (TTL = 24h) before a retry is processed, the retry worker will see no record and re-acquire, re-sending the email. This is intentional: the 24-hour window is the deduplication contract. Tasks older than 24 hours are re-executed on retry.

---

## Producer-Side Deduplication

Producers can use **deterministic task IDs** to make enqueue itself idempotent:

```go
// Deterministic ID: hash of the event's natural key
func DeterministicTaskID(tenantID, recipientEmail, templateID, eventID string) string {
    h := sha256.New()
    fmt.Fprintf(h, "%s|%s|%s|%s", tenantID, recipientEmail, templateID, eventID)
    return fmt.Sprintf("%x", h.Sum(nil))[:32]  // first 32 hex chars
}
```

If the same event triggers two enqueue calls (e.g. duplicate webhook delivery), both calls produce the same task ID. The first enqueue writes to the stream; the second is deduplicated by the idempotency store at processing time (or can be deduplicated at the producer level with a separate `SET NX` before XADD).

### X-Idempotency-Key HTTP header

The producer HTTP API accepts an `X-Idempotency-Key` header. If present, it is used as the task ID directly, enabling API clients to implement retry-safe enqueue:

```
POST /v1/tasks
X-Idempotency-Key: evt_01J2K...

→ task.ID = "evt_01J2K..."
→ Repeat submission with same key → same task ID → deduplicated at worker
```

---

## Deduplication Window Summary

| Scenario | Outcome |
|----------|---------|
| Same task ID, within 24h, not yet processed | Worker 1 acquires; Worker 2 skips |
| Same task ID, within 24h, already completed | All workers skip; email sent exactly once |
| Same task ID, after 24h TTL | Re-processed; email re-sent (intentional) |
| Different task IDs (same content) | Both processed; email sent twice (producer must use deterministic IDs) |
| Worker crash mid-processing | PEL + XAUTOCLAIM redelivers; idempotency stale-lock reclaim handles |

---

## Consequences

**What becomes easier:**
- Workers are safe to crash at any point — redelivery is handled correctly
- API clients can retry failed HTTP enqueue calls safely using `X-Idempotency-Key`
- Ops engineers can manually replay tasks from the DLQ safely — 24h window provides replay protection

**What becomes harder:**
- The Lua script requires Redis 2.6+ (universally available)
- Idempotency store is an additional Redis key namespace that must be sized in capacity planning
- The stale-lock reclaim logic adds complexity to the consumer adapter

**What we will need to revisit:**
- If the 24-hour deduplication window is too short for some task types (e.g. weekly marketing), the TTL should be configurable per task type
- If Redis memory becomes a concern, the idempotency store could be moved to a separate Redis instance or backed by a fast external store (e.g. DynamoDB with TTL)

---

## Action Items

1. [ ] Implement `idempotency_cas.lua` script in `migrations/`
2. [ ] Implement `idempotency_reclaim.lua` script in `migrations/`
3. [ ] Load scripts at startup via `SCRIPT LOAD`; store SHAs in `ScriptRegistry`
4. [ ] Implement `RedisIdempotencyStore` in `internal/adapters/redis/idempotency.go`
5. [ ] Implement `DeterministicTaskID()` in `internal/domain/`
6. [ ] Add `X-Idempotency-Key` header support to `POST /v1/tasks` handler
7. [ ] Write unit tests: concurrent SetProcessing (only one acquires), TTL expiry, stale lock reclaim
