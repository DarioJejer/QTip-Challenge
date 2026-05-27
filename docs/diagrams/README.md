# Architecture Diagrams

All diagrams are authored in [Mermaid](https://mermaid.js.org/) and render natively in GitHub Markdown previews. To render locally, use the [Mermaid Live Editor](https://mermaid.live) or the VS Code Mermaid Preview extension.

---

## Diagrams

### 1. C4 System Context — [`system-context.mmd`](./system-context.mmd)

**What it shows:** The email queue service in relation to external actors and systems — the SaaS Platform API (producer), the Email Provider (SendGrid/SES), Redis Cluster, Prometheus/Grafana, and Jaeger.

**Related ADR:** [ADR-001 — Redis as the Queue Broker](../adr/ADR-001-redis-broker.md)

---

### 2. C4 Container Diagram — [`container.mmd`](./container.mmd)

**What it shows:** Internal containers of the email queue service — the HTTP Producer API, Worker Pool Supervisor, Delayed Task Scheduler, DLQ Monitor — and the Redis data structures each container interacts with (Streams, Sorted Set, LIST, Strings/Lua).

**Related ADRs:**
- [ADR-002 — Redis Data Structures: LIST vs Streams](../adr/ADR-002-redis-data-structures.md)
- [ADR-004 — Worker Pool Concurrency Model](../adr/ADR-004-worker-pool.md)

---

### 3. Task Lifecycle Sequence — [`task-lifecycle.mmd`](./task-lifecycle.mmd)

**What it shows:** The happy-path lifecycle of a single email task from HTTP enqueue through Redis XADD, worker XREADGROUP consumption, idempotency check, email send, XACK, and idempotency completion mark.

**Related ADRs:**
- [ADR-003 — Task Serialization Format](../adr/ADR-003-serialization.md)
- [ADR-006 — Idempotency & Duplicate Delivery Protection](../adr/ADR-006-idempotency.md)

---

### 4. Retry & DLQ Flow — [`retry-flow.mmd`](./retry-flow.mmd)

**What it shows:** The retry state machine in action — a task failing multiple times, being placed in the delayed sorted set with exponential backoff, re-enqueued by the scheduler, and eventually routed to the DLQ after exhausting all attempts.

**Related ADR:** [ADR-005 — Retry Architecture & Dead-Letter Queue](../adr/ADR-005-retry-dlq.md)

---

### 5. Worker Goroutine Topology — [`worker-goroutines.mmd`](./worker-goroutines.mmd)

**What it shows:** The complete goroutine topology of the running service — `main()`, the errgroup goroutines (supervisor, scheduler, DLQ monitor, HTTP server, metrics server), the semaphore-bounded worker goroutine pool, and the shutdown signal propagation path.

**Related ADRs:**
- [ADR-004 — Worker Pool Concurrency Model](../adr/ADR-004-worker-pool.md)
- [ADR-008 — Kubernetes Operational Contract](../adr/ADR-008-kubernetes.md)

---

## Viewing in GitHub

GitHub renders `.mmd` files as Mermaid diagrams directly in the browser. Click any diagram file above to see it rendered.

## Updating Diagrams

1. Edit the `.mmd` file directly
2. Preview changes at [mermaid.live](https://mermaid.live) before committing
3. Commit with a message referencing the ADR being updated, e.g. `docs(diagrams): update container diagram for ADR-002`
