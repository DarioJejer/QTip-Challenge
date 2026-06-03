# go-email-queue

A production-grade, Redis-backed asynchronous email delivery system for multi-tenant SaaS platforms. Built with Go, designed for 10k–50k email tasks per minute, and Kubernetes-native.

---

## Overview

`go-email-queue` decouples email delivery from your application's request path by enqueuing tasks to Redis Streams and processing them asynchronously via a bounded worker pool. It handles retries with exponential backoff, dead-letter queues, idempotent delivery, priority lanes, and delayed scheduling — all observable via Prometheus metrics and OpenTelemetry traces.

```
SaaS Platform API
      │
      ▼ POST /v1/tasks
┌─────────────────────┐
│  HTTP Producer API  │
└─────────┬───────────┘
          │ XADD
          ▼
    Redis Streams          Redis Sorted Set       Redis LIST
  ┌──────────────┐        ┌──────────────────┐  ┌──────────┐
  │ queue:email: │        │ queue:email:      │  │ queue:   │
  │ critical     │        │ retry:delayed     │  │ dlq:*    │
  │ high         │        └──────────────────┘  └──────────┘
  │ normal       │
  │ low          │
  └──────┬───────┘
         │ XREADGROUP
         ▼
  Worker Pool (N goroutines)
         │
         ▼
   Email Provider (SendGrid / SES)
```

See [`docs/diagrams/`](docs/diagrams/) for full C4 context, container, and sequence diagrams.

---

## Architecture Decision Records

All significant design decisions are documented as ADRs in [`docs/adr/`](docs/adr/):

| ADR | Decision |
|-----|----------|
| [ADR-001](docs/adr/ADR-001-redis-broker.md) | Redis as the queue broker (vs Kafka, SQS, RabbitMQ) |
| [ADR-002](docs/adr/ADR-002-redis-data-structures.md) | Redis Streams for queues, Sorted Set for delays, LIST for DLQ |
| [ADR-003](docs/adr/ADR-003-serialization.md) | `encoding/json` for task serialization |
| [ADR-004](docs/adr/ADR-004-worker-pool.md) | Semaphore-bounded goroutine pool with graceful drain |
| [ADR-005](docs/adr/ADR-005-retry-dlq.md) | Exponential backoff with jitter + 7-day DLQ TTL |
| [ADR-006](docs/adr/ADR-006-idempotency.md) | Lua atomic check-and-set for exactly-once delivery |
| [ADR-007](docs/adr/ADR-007-observability.md) | zerolog + Prometheus + OpenTelemetry (OTLP) |
| [ADR-008](docs/adr/ADR-008-kubernetes.md) | K8s probes, HPA, graceful shutdown, PDB |

---

## Project Structure

```
go-email-queue/
├── cmd/server/           # Application entry point
├── internal/
│   ├── domain/           # Pure domain types — EmailTask, RetryPolicy, DLQEntry, etc.
│   ├── ports/            # Interface definitions (TaskProducer, EmailSender, etc.)
│   ├── adapters/
│   │   ├── redis/        # Redis implementations of ports
│   │   ├── email/        # Email sender implementations (stub + SendGrid)
│   │   ├── http/         # HTTP handlers and middleware
│   │   └── stubs/        # No-op stubs for testing and local dev
│   ├── app/              # Application services / use cases
│   ├── worker/           # WorkerSupervisor, DelayedScheduler, DLQMonitor
│   ├── config/           # Config struct + environment variable loading
│   ├── observability/    # zerolog, Prometheus, OpenTelemetry initialization
│   └── di/               # Manual dependency injection wiring
├── migrations/           # Redis Lua scripts (idempotency CAS, consumer groups)
├── docs/
│   ├── adr/              # Architecture Decision Records
│   └── diagrams/         # Mermaid architecture diagrams
├── deployments/
│   ├── k8s/              # Kubernetes manifests (Deployment, HPA, PDB, etc.)
│   └── docker/           # Docker-related files
├── Makefile
├── Dockerfile            # Multi-stage, distroless final image
└── docker-compose.yml    # Local dev (Redis + app)
```

---

## Quickstart

### Prerequisites

- Go 1.23+
- Docker & Docker Compose
- [golangci-lint](https://golangci-lint.run/usage/install/) (for linting)

### Run locally

```bash
# Start Redis
docker-compose up -d redis

# Run the service
REDIS_URL=redis://localhost:6379 \
API_KEYS=dev-key \
go run ./cmd/server/
```

### Run tests

```bash
go test -race ./...
```

### Build

```bash
make build
# binary at bin/server
```

### Docker

```bash
make docker-build
docker-compose up
```

---

## Configuration

All configuration is loaded from environment variables. See [`internal/config/`](internal/config/) for the full list with defaults.

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_URL` | *(required)* | Redis connection URL |
| `API_KEYS` | *(required)* | Comma-separated valid API keys |
| `WORKER_POOL_SIZE` | `50` | Number of concurrent worker goroutines |
| `WORKER_DRAIN_TIMEOUT` | `30s` | Graceful shutdown drain budget |
| `LOG_LEVEL` | `info` | Log level (`debug`\|`info`\|`warn`\|`error`) |
| `LOG_FORMAT` | `json` | Log format (`json`\|`console`) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | *(optional)* | OTLP gRPC endpoint for traces |
| `METRICS_PORT` | `9090` | Prometheus metrics port |
| `HTTP_PORT` | `8080` | HTTP API port |

---

## Key Design Constraints

- **At-least-once delivery** via Redis Streams PEL + XAUTOCLAIM crash recovery
- **Idempotency** via Lua atomic check-and-set (24h deduplication window)
- **Backpressure** via semaphore: full pool → XREADGROUP blocks → natural queue accumulation
- **Graceful shutdown**: SIGTERM → drain in-flight (30s budget) → flush OTel → exit 0
- **Multi-tenancy**: every task carries `tenant_id`; DLQ keys are namespaced per tenant
- **Observability**: all 18 log events, 13 Prometheus metrics, and 7 OTel span names are catalogued in [ADR-007](docs/adr/ADR-007-observability.md)

---

## License

MIT
