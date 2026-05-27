# ADR-001: Redis as the Queue Broker

**Status:** Accepted
**Date:** 2026-05-27
**Deciders:** Engineering Team

---

## Context

We are building a production-grade async email delivery system for a multi-tenant SaaS platform. The system must handle 10k–50k email tasks per minute with occasional campaign spikes, supporting use cases including user registration, password resets, billing notifications, marketing campaigns, security alerts, and bulk transactional emails.

We need a message broker that:
- Supports asynchronous task queuing with reliable delivery
- Can sustain high throughput (50k msgs/min) with low latency
- Supports priority queues and delayed/scheduled task delivery
- Is horizontally scalable and Kubernetes-friendly
- Has mature Go client support
- Is operationally manageable by a small team

The key forces at play:
- **Throughput:** 50k msgs/min is ~833 msgs/sec — well within a single Redis instance's capability (~100k ops/sec sustained)
- **Operability:** The team needs to move fast; complex infrastructure adds risk
- **Cost:** Managed services at this volume carry non-trivial per-message costs
- **Existing stack:** Redis is likely already deployed for caching/session — reuse reduces operational surface
- **Delivery semantics:** Email is a side-effect; at-least-once with application-layer idempotency is sufficient

---

## Decision

**We will use Redis as the message broker**, leveraging Redis Streams for the main work queue and retry queue, a Redis Sorted Set for delayed task scheduling, and a Redis LIST for the dead-letter queue (DLQ).

---

## Options Considered

### Option A: Redis (Selected)

| Dimension           | Assessment                                                              |
|---------------------|-------------------------------------------------------------------------|
| Throughput          | ~100k ops/sec single node; Redis Cluster scales horizontally            |
| Latency             | Sub-millisecond enqueue/dequeue                                         |
| Delivery semantics  | At-least-once via Streams PEL + XCLAIM                                  |
| Operational cost    | Low — single process, simple config, battle-tested                      |
| Go client           | Excellent (`github.com/redis/go-redis/v9`)                              |
| Priority queues     | Native via multiple Streams (one per priority band)                     |
| Delayed tasks       | Native via Sorted Set (ZADD score=unix_timestamp)                       |
| Team familiarity    | High                                                                    |
| Persistence         | AOF + RDB; AOF everysec recommended for durability/performance balance  |

**Pros:**
- Operationally simple: single binary, no JVM, no ZooKeeper/KRaft
- Already in the stack — no new infrastructure to provision and monitor
- Redis Streams provide consumer groups, PEL-based crash recovery, and XCLAIM for stale message reassignment
- Sub-millisecond latency is ideal for high-throughput queuing
- Excellent local testability with `miniredis`
- Sorted Set provides a natural delayed-task scheduler without a separate service

**Cons:**
- Memory-bound: entire queue lives in RAM (mitigated by MAXMEMORY policy + stream MAXLEN trimming)
- At-least-once delivery only — exactly-once requires application-layer idempotency
- No built-in message replay across consumer groups (unlike Kafka's offset reset)
- AOF/RDB persistence adds fsync latency (mitigated with `appendfsync everysec`)
- Lua scripts complicate Redis Cluster deployments (all keys must map to same slot)

---

### Option B: Apache Kafka

| Dimension           | Assessment                                                              |
|---------------------|-------------------------------------------------------------------------|
| Throughput          | Extremely high (millions/sec); overkill for 50k msgs/min               |
| Latency             | 5–15ms typical; higher than Redis                                       |
| Delivery semantics  | At-least-once and exactly-once (with transactions)                      |
| Operational cost    | High — requires ZooKeeper or KRaft, broker cluster, schema registry     |
| Go client           | Good (`segmentio/kafka-go`, `confluentinc/confluent-kafka-go`)          |
| Priority queues     | Not native — requires separate topics per priority                      |
| Delayed tasks       | Not native — requires custom implementation or Kafka Streams            |
| Team familiarity    | Medium                                                                  |

**Pros:**
- Infinite retention and log replay
- Exactly-once semantics available
- Extremely high throughput ceiling

**Cons:**
- Operationally heavy: Kafka + ZooKeeper/KRaft is a significant infrastructure investment
- 5–15ms latency vs Redis's sub-millisecond
- No native delayed task scheduling
- Substantial over-engineering for 50k msgs/min

---

### Option C: AWS SQS

| Dimension           | Assessment                                                              |
|---------------------|-------------------------------------------------------------------------|
| Throughput          | High (standard queues: unlimited; FIFO: 3k msgs/sec per queue)         |
| Latency             | 1–20ms polling-based                                                    |
| Delivery semantics  | At-least-once (standard) / exactly-once (FIFO)                         |
| Operational cost    | Zero ops overhead; ~$0.40/million messages                              |
| Go client           | Good (AWS SDK v2)                                                       |
| Priority queues     | Not native — requires separate queues                                   |
| Delayed tasks       | Native (up to 15min delay per message)                                  |
| Team familiarity    | Medium                                                                  |

**Pros:**
- Zero operational overhead — fully managed
- FIFO queues offer exactly-once processing

**Cons:**
- Vendor lock-in to AWS
- At 50k msgs/min (~2.6B msgs/month) cost becomes ~$1,040/month for standard queues
- FIFO queues limited to 3k msgs/sec — requires heavy sharding at peak
- No local testability without LocalStack
- 256KB message size limit

---

### Option D: RabbitMQ

| Dimension           | Assessment                                                              |
|---------------------|-------------------------------------------------------------------------|
| Throughput          | ~50k msgs/sec single node; adequate                                     |
| Latency             | Low (1–5ms)                                                             |
| Delivery semantics  | At-least-once with manual ack                                           |
| Operational cost    | Medium — Erlang runtime, clustering complexity                          |
| Go client           | Good (`rabbitmq/amqp091-go`)                                            |
| Priority queues     | Native (x-max-priority)                                                 |
| Delayed tasks       | Plugin required (rabbitmq_delayed_message_exchange)                     |
| Team familiarity    | Low                                                                     |

**Pros:**
- Mature AMQP protocol with rich routing semantics
- Native priority queues

**Cons:**
- Erlang runtime adds operational unfamiliarity
- Clustering and HA setup is non-trivial
- Delayed tasks require an additional plugin
- Go client ecosystem thinner than Redis

---

## Trade-off Analysis

The primary trade-off is **operational simplicity vs. delivery guarantees**.

Redis does not provide exactly-once delivery natively, but this is acceptable because:
1. Email delivery is idempotent at the application layer (each task carries a UUID; idempotency keys prevent duplicate sends)
2. Redis Streams with consumer groups and PEL provide at-least-once delivery with crash recovery via XCLAIM
3. The operational cost savings over Kafka or RabbitMQ are significant for a small team

The secondary trade-off is **memory as a capacity ceiling**. Redis stores all queue data in RAM. At 50k msgs/min with an average payload of ~1KB, the steady-state queue depth (assuming fast processing) is low. Mitigations:
- Stream `MAXLEN ~` trimming to bound memory usage
- `MAXMEMORY` policy configured as `noeviction` (fail loudly, never silently drop messages)
- DLQ entries stored with a 7-day TTL to bound dead-letter accumulation

---

## Consequences

**What becomes easier:**
- Local development and testing (`miniredis` eliminates the need for a real Redis instance in tests)
- Operational runbooks are simpler (Redis is well-understood)
- Delayed task scheduling is a single sorted set — no external scheduler service required
- Existing Redis instances can be reused (separate DB index recommended for isolation)

**What becomes harder:**
- Exactly-once delivery requires careful application-layer idempotency implementation
- Lua scripts used for atomic operations are incompatible with Redis Cluster unless all keys share the same hash slot — use `{tag}` key notation
- Queue replay (re-processing historical messages) is not possible once messages are ACKed and trimmed

**What we will need to revisit:**
- If throughput grows beyond ~500k msgs/min sustained, evaluate Redis Cluster sharding or migration to Kafka
- If the team requires audit-trail replay of all historical tasks, Kafka or an event store should be evaluated
- If multi-region active-active is required, Redis's async replication model introduces split-brain risk

---

## Reliability Guarantees

| Property                  | Guarantee                                                                 |
|---------------------------|---------------------------------------------------------------------------|
| Delivery                  | At-least-once (Streams PEL + XCLAIM)                                      |
| Ordering                  | FIFO within a single stream (per priority band); no cross-band ordering   |
| Durability                | AOF `everysec` — up to 1 second of data loss on hard crash                |
| Crash recovery            | Messages in PEL reclaimed by other workers via XCLAIM after idle TTL      |
| Duplicate suppression     | Application-layer idempotency store (Redis SET NX with 24h TTL)           |

---

## Recommended Production Configuration

```
# Persistence
appendonly yes
appendfsync everysec
no-appendfsync-on-rewrite no

# Memory management
maxmemory 4gb
maxmemory-policy noeviction

# Replication
replica-lazy-flush yes
```

---

## Action Items

1. [ ] Configure Redis with AOF `everysec` persistence in all environments
2. [ ] Set `maxmemory-policy noeviction` to fail loudly on memory pressure
3. [ ] Implement stream `MAXLEN ~` trimming on all producer XADDs
4. [ ] Use `{queue}` hash tag notation in all Redis keys for Cluster compatibility
5. [ ] Provision Redis Sentinel or Redis Cluster for HA in production
6. [ ] Document runbook for Redis failover procedure
7. [ ] Set up Redis memory alerting at 70% and 90% thresholds
