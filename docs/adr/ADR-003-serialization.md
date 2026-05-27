# ADR-003: Task Serialization Format

**Status:** Accepted
**Date:** 2026-05-27
**Deciders:** Engineering Team

---

## Context

Every email task enqueued to Redis must be serialized to bytes and deserialized by workers. The serialization format affects:

- **Payload size** — directly impacts Redis memory usage and network transfer time
- **Serialization/deserialization throughput** — CPU cost per task at 50k msgs/min
- **Debuggability** — can ops engineers inspect queue contents with `redis-cli`?
- **Schema evolution** — can we add/remove fields without breaking running workers?
- **Build complexity** — does the format require code generation or external tooling?

At 50k msgs/min (~833 tasks/sec), even a 100µs serialization overhead per task would consume ~8% of a single CPU core — negligible. The dominant costs are network I/O and Redis command latency, not serialization. This shapes the decision strongly toward simplicity and debuggability over raw performance.

---

## Decision

**We will use `encoding/json` (Go standard library) for v1.**

The format is human-readable JSON with `omitempty` tags on optional fields. An envelope wrapper carries a `version` field for future evolution.

---

## Options Considered

### Option A: encoding/json — Selected

| Dimension | Assessment |
|-----------|------------|
| Payload size | ~500–800 bytes per task (typical) |
| Serialization speed | ~500ns–2µs per task |
| Deserialization speed | ~1–3µs per task |
| Debuggability | ✅ Excellent — readable in `redis-cli`, `redis-insight`, logs |
| Schema evolution | ✅ additive via optional fields + `omitempty` |
| Build complexity | ✅ None — stdlib only |
| Go client | ✅ Native |

**Pros:**
- Zero dependencies — stdlib only
- Payload is human-readable: `redis-cli XRANGE queue:email:normal - +` shows intelligible JSON
- Additive schema evolution: new optional fields with `omitempty` are ignored by old consumers
- Familiar to every Go developer
- Easy to inspect, copy, and replay tasks manually during incidents

**Cons:**
- Larger payloads than binary formats (~30–50% larger than MessagePack)
- Slightly slower than binary formats (negligible at our throughput)
- No schema enforcement — malformed payloads fail at runtime, not compile time

---

### Option B: MessagePack (vmihailenco/msgpack)

| Dimension | Assessment |
|-----------|------------|
| Payload size | ~300–500 bytes per task (~35% smaller than JSON) |
| Serialization speed | ~200–400ns per task |
| Deserialization speed | ~400–800ns per task |
| Debuggability | ❌ Binary — not readable in redis-cli without a decoder |
| Schema evolution | ✅ additive via optional fields |
| Build complexity | Low — single dependency |
| Go client | Good |

**Pros:**
- Meaningfully smaller payloads (saves ~35% Redis memory)
- Faster than JSON

**Cons:**
- Binary format kills `redis-cli` debuggability — the primary operational advantage of Redis as a broker
- One additional dependency
- At 50k msgs/min the memory saving (~150 bytes/task × queue depth) is modest and does not justify the operational cost

**Migration note:** Switching from JSON to MessagePack is a single-file change (swap the `json.Marshal`/`json.Unmarshal` calls for `msgpack.Marshal`/`msgpack.Unmarshal` in the serialization adapter). This is a clean escape hatch if memory pressure becomes a concern.

---

### Option C: Protocol Buffers

| Dimension | Assessment |
|-----------|------------|
| Payload size | ~200–350 bytes per task |
| Serialization speed | ~100–200ns per task |
| Deserialization speed | ~200–500ns per task |
| Debuggability | ❌ Binary — requires protoc tooling to decode |
| Schema evolution | ✅ Strong — field numbers + reserved keywords |
| Build complexity | High — .proto files, protoc, buf, code generation in CI |
| Go client | Good (google.golang.org/protobuf) |

**Pros:**
- Smallest payloads
- Fastest serialization
- Schema enforcement at compile time

**Cons:**
- Build complexity: `.proto` files, `protoc` binary, `buf` toolchain, generated code checked in
- Binary format with no Redis-CLI debuggability
- Significant over-engineering for a v1 that can be migrated later if justified
- Schema changes require regenerating code in all services that share the proto

---

## Canonical EmailTask Schema

The following is the authoritative definition of the `EmailTask` payload stored in each Redis Stream entry.

### Go Struct

```go
// EmailTask is the unit of work enqueued for async email delivery.
// All fields use json tags; optional fields use omitempty.
type EmailTask struct {
    // --- Identity ---
    ID       string `json:"id"`        // UUIDv4; used as idempotency key
    TenantID string `json:"tenant_id"` // Multi-tenant routing

    // --- Classification ---
    Type     TaskType `json:"type"`     // See TaskType enum
    Priority Priority `json:"priority"` // 0=low, 1=normal, 2=high, 3=critical

    // --- Delivery target ---
    Recipient   string         `json:"recipient"`             // Email address
    TemplateID  string         `json:"template_id"`           // Template identifier
    TemplateData map[string]any `json:"template_data,omitempty"` // Template variables

    // --- Scheduling ---
    EnqueuedAt   time.Time  `json:"enqueued_at"`             // RFC3339Nano
    ScheduledFor *time.Time `json:"scheduled_for,omitempty"` // nil = immediate

    // --- Retry state ---
    Attempt     int    `json:"attempt"`      // Current attempt (0-indexed)
    MaxAttempts int    `json:"max_attempts"` // Retry limit (task-type specific)
    LastError   string `json:"last_error,omitempty"`

    // --- Observability ---
    TraceID  string `json:"trace_id,omitempty"`  // OTel W3C trace-id
    SpanID   string `json:"span_id,omitempty"`   // OTel parent span-id

    // --- Extensibility ---
    Metadata map[string]string `json:"metadata,omitempty"` // Arbitrary key-value pairs
    Status   TaskStatus        `json:"status"`
}
```

### TaskType Enum

```go
type TaskType string

const (
    TaskTypeRegistration   TaskType = "registration"
    TaskTypePasswordReset  TaskType = "password_reset"
    TaskTypeBilling        TaskType = "billing"
    TaskTypeMarketing      TaskType = "marketing"
    TaskTypeSecurity       TaskType = "security"
    TaskTypeTransactional  TaskType = "transactional"
)
```

### Priority Enum

```go
type Priority int8

const (
    PriorityLow      Priority = 0
    PriorityNormal   Priority = 1
    PriorityHigh     Priority = 2
    PriorityCritical Priority = 3
)

func (p Priority) QueueName() string {
    switch p {
    case PriorityCritical: return "queue:email:critical"
    case PriorityHigh:     return "queue:email:high"
    case PriorityNormal:   return "queue:email:normal"
    default:               return "queue:email:low"
    }
}
```

### Default MaxAttempts by TaskType

| TaskType | MaxAttempts | Rationale |
|----------|-------------|-----------|
| registration | 5 | Delivery is important; user expects prompt receipt |
| password_reset | 5 | Security-sensitive; must deliver |
| billing | 5 | Legal/compliance requirement |
| marketing | 3 | Lower urgency; avoid spam provider blocks |
| security | 7 | Highest importance; max retry effort |
| transactional | 5 | Standard transactional guarantee |

---

## Envelope Wrapper

To support future format evolution without a flag day, payloads are wrapped in a thin envelope:

```go
type TaskEnvelope struct {
    Version   int    `json:"v"`             // Schema version; currently always 1
    Compressed bool  `json:"c,omitempty"`   // Reserved: true if payload is gzip-compressed
    Payload   []byte `json:"p"`             // JSON-encoded EmailTask
}
```

In v1 the envelope adds ~20 bytes overhead. The `version` field allows consumers to detect and handle schema migrations. The `compressed` flag is reserved for a future optimisation where large `TemplateData` maps are compressed before storage.

**Wire format example (Redis Stream entry fields):**
```
id:        "01J2K..."
envelope:  {"v":1,"p":"{\"id\":\"01J2K...\",\"tenant_id\":\"acme\",...}"}
enqueued_at: "2026-05-27T14:00:00.000000000Z"
tenant_id: "acme"     ← duplicated at top level for fast routing without deserialization
task_type: "registration"
priority:  "2"
trace_id:  "4bf92f3577b34da6a3ce929d0e0e4736"
```

Top-level stream fields (`tenant_id`, `task_type`, `priority`, `trace_id`) are stored redundantly for fast server-side filtering without deserializing the full envelope.

---

## Schema Evolution Strategy

1. **Adding a field:** Add with `omitempty`. Old consumers ignore it. No migration needed.
2. **Renaming a field:** Add the new name with `omitempty`, deprecate the old name for one release cycle, then remove. Never rename in place.
3. **Removing a field:** Mark as deprecated in a comment, set to `omitempty`, remove in the next major version. Workers must tolerate missing fields.
4. **Breaking change:** Increment envelope `version`. Deploy consumers that handle both `v:1` and `v:2` before deploying producers that emit `v:2`.

---

## Consequences

**What becomes easier:**
- Incident debugging: `redis-cli XRANGE queue:email:normal - + COUNT 5` shows readable task payloads
- Local development: tasks can be manually crafted and injected with `redis-cli XADD`
- Schema evolution: optional fields with `omitempty` are backward-compatible by default

**What becomes harder:**
- Payload size is ~35% larger than MessagePack (acceptable at current scale)
- No compile-time schema validation (mitigated by `Validate()` method on `EmailTask`)

**Migration path to MessagePack:** If Redis memory usage becomes a concern, the entire serialization layer is behind a single interface (`Serializer`). Swapping JSON for MessagePack is a one-line change in the adapter constructor, with a rolling deployment that handles both formats during the transition window via the envelope `version` field.

---

## Action Items

1. [ ] Implement `EmailTask` struct in `internal/domain/task.go` with all fields and JSON tags
2. [ ] Implement `TaskEnvelope` in `internal/domain/envelope.go`
3. [ ] Implement `Validate() error` method on `EmailTask` with field-level validation
4. [ ] Write table-driven unit tests for JSON round-trip fidelity (all field types including `time.Time`)
5. [ ] Document `omitempty` convention in the project CONTRIBUTING guide
6. [ ] Add `redis-cli` inspection examples to the operational runbook
