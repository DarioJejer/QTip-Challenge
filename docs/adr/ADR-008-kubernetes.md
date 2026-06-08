# ADR-008: Kubernetes Operational Contract

**Status:** Accepted  
**Date:** 2026-05-27  
**Deciders:** Engineering Team

---

## Context

The email queue service runs as a Kubernetes Deployment. Unlike a stateless HTTP service, this service:

- **Pulls** from Redis Streams rather than receiving pushed traffic — readiness semantics differ
- **Holds in-flight work** — a forced pod kill during task processing risks message redelivery
- **Must scale** based on queue depth, not CPU or memory
- **Must not** lose messages during rolling deployments or node drains

This ADR defines the exact Kubernetes configuration — probes, shutdown sequence, resource limits, HPA, and PDB — to achieve zero-message-loss deployments and stable autoscaling.

---

## Decision

**We will configure a Deployment with a 60-second termination grace period, custom queue-depth HPA metrics, maxUnavailable=0 rolling updates, and a PodDisruptionBudget of minAvailable=1.**

---

## 1. Health Probes

### Liveness Probe — `GET /healthz`

The liveness probe determines whether the pod should be restarted. It returns `200 OK` if:
- The worker supervisor goroutine is running (checked via an atomic heartbeat flag updated every poll cycle)
- Redis PING succeeds within 2 seconds

It returns `503 Service Unavailable` if:
- Redis is unreachable for more than 10 consecutive seconds
- The supervisor goroutine has exited (detected by a closed channel or missed heartbeat)

```yaml
livenessProbe:
  httpGet:
    path: /healthz
    port: 8080
  initialDelaySeconds: 10   # allow JVM-equivalent startup
  periodSeconds: 10
  failureThreshold: 3        # 30s of failure → restart
  timeoutSeconds: 5
```

**Why restart on Redis unavailability?** A worker that cannot reach Redis cannot process tasks, XACK messages, or update the idempotency store. It is in a broken state. Restarting gives the pod a chance to reconnect with a fresh Redis client. The PDB ensures at least one replica stays healthy during the restart.

### Readiness Probe — `GET /readyz`

The readiness probe determines whether the pod should receive traffic. For a pull-based worker, "traffic" means:
- Being scraped by Prometheus on `/metrics` (port 9090)
- Being included in any future admin API load balancer

It returns `200 OK` when:
- The worker pool has successfully created all consumer groups
- The supervisor goroutine has started and is actively polling

It returns `503 Service Unavailable` during:
- Startup (before consumer groups are created and supervisor starts)
- Graceful shutdown drain (to signal to Prometheus to stop scraping mid-shutdown)

```yaml
readinessProbe:
  httpGet:
    path: /readyz
    port: 8080
  initialDelaySeconds: 5
  periodSeconds: 5
  failureThreshold: 2       # 10s not ready → remove from endpoints
  timeoutSeconds: 3         # allow for Redis PING under load
```

**Readiness vs Liveness for pull workers:** Unlike HTTP servers, a pull-based worker doesn't receive Kubernetes-routed traffic. However, readiness still matters because: (1) Prometheus ServiceMonitor uses endpoint selection, and (2) it gates the rolling update — new pods must be ready before old pods are terminated.

> **M2 implementation note:** In M2, `router.SetReady(true)` is called when the HTTP server goroutine starts, which is earlier than the full readiness condition described above (consumer groups not yet created — those are M3 stubs). This is acceptable for the skeleton milestone. **Action item for M3:** move `SetReady(true)` to after `RunMigrations` and consumer group initialisation succeed, so the probe accurately reflects the worker's ability to consume.

---

## 2. Graceful Shutdown Sequence

When Kubernetes sends `SIGTERM` to a pod (during rolling update, scale-down, or node drain), the following sequence executes:

```
T + 0s    SIGTERM received by PID 1
T + 0s    signal.NotifyContext fires → rootCtx cancelled
T + 0s    router.SetReady(false)
          → /readyz returns 503
          → Prometheus scrape endpoint still responds (uses separate context)
T + 0s    WorkerSupervisor: stops XREADGROUP polling
          → No new tasks dequeued
T + 0s    In-flight processTask goroutines: ctx.Done() fires on next context check
          → Workers that have not yet called Send: do NOT call XACK
            The message remains in the PEL and will be reclaimed by
            XAUTOCLAIM after the idle threshold expires on a surviving pod.
          → Workers mid-Send: complete the send, then XACK with context.Background()
T + 0–30s Drain loop: polls semaphore every 5s, logs in-flight count
T + 30s   Drain timeout (DRAIN_TIMEOUT=30s, configurable)
          → Log warning if slots still occupied
T + 30s   Flush OTel spans and metrics
T + 30s   Close Redis connections (logs pool stats)
T + 30s   os.Exit(0)
T + 60s   Kubernetes sends SIGKILL (terminationGracePeriodSeconds=60)
          → 30s safety buffer; pod is gone by T+30s in normal operation
```

> **Redis Streams has no NACK command.** "Not ACKing" is the mechanism — the PEL entry ages in place. When `XAUTOCLAIM` runs on a surviving pod after `cfg.ClaimIdleThreshold`, it reclaims any messages whose PEL idle time has exceeded the threshold and re-dispatches them. This is why the drain timeout is set conservatively at 30s: it covers the worst-case in-flight task, after which any remaining tasks are safely reclaimed by other pods via PEL.

### Why 30s drain + 30s buffer = 60s terminationGracePeriodSeconds

- Average task processing time: ~500ms–2s (email API latency)
- Maximum task processing time: 30s (HTTP timeout to email provider)
- Drain budget: 30s covers the worst-case in-flight task
- K8s buffer: additional 30s before SIGKILL, covering any OTel flush delays
- `terminationGracePeriodSeconds: 60` in the pod spec

### preStop hook

A `preStop` hook adds a 5-second sleep before SIGTERM to allow the load balancer (for the metrics/admin endpoint) to drain connections:

```yaml
lifecycle:
  preStop:
    exec:
      command: ["/bin/sleep", "5"]
```

---

## 3. Autoscaling (HPA)

The worker pool scales based on **queue depth** (the primary signal) and **worker saturation** (the secondary signal). CPU and memory are poor scaling signals for this workload — a saturated worker pool has high CPU not because it needs more CPU, but because it needs more replicas.

### Required Prometheus recording rules

Two recording rules must be deployed (e.g. via a `PrometheusRule` CR) before the HPA custom metrics are available:

```yaml
# deploy alongside the service in the email-queue namespace
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: email-queue-hpa-rules
spec:
  groups:
    - name: email_queue_hpa
      interval: 15s
      rules:
        # Aggregate queue depth across ALL four priority streams.
        # Monitoring only one stream (e.g. "normal") would miss spikes
        # in critical/high queues, delaying scale-out when it matters most.
        - record: email_queue_depth_total
          expr: sum(email_queue_depth) by (job)

        # Worker saturation ratio — used as secondary HPA signal.
        # email_queue_worker_active / email_queue_worker_pool_size
        # Both metrics are Gauges registered in observability.PrometheusRecorder.
        - record: email_queue_worker_saturation
          expr: >
            email_queue_worker_active
            /
            email_queue_worker_pool_size
```

> Without these rules deployed, the HPA will fail to resolve the custom metrics and silently fall back to the CPU utilisation target only.

### Custom metric: queue depth

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: email-queue-worker
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: email-queue-worker
  minReplicas: 2
  maxReplicas: 20
  metrics:
    # Primary: total queue depth across all four priority streams.
    # Uses the email_queue_depth_total recording rule defined above.
    - type: External
      external:
        metric:
          name: email_queue_depth_total
        target:
          type: AverageValue
          averageValue: "1000"   # scale up when total depth > 1000 per replica

    # Secondary: worker saturation ratio (0–1).
    # Uses the email_queue_worker_saturation recording rule defined above.
    - type: External
      external:
        metric:
          name: email_queue_worker_saturation
        target:
          type: AverageValue
          averageValue: "800m"   # 0.8 = 80% saturation threshold

    # Fallback: CPU (prevents idle pods at near-zero load)
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70

  behavior:
    scaleUp:
      stabilizationWindowSeconds: 0      # scale up immediately
      policies:
        - type: Pods
          value: 4
          periodSeconds: 60              # add max 4 pods per minute
    scaleDown:
      stabilizationWindowSeconds: 300    # wait 5 minutes before scaling down
      policies:
        - type: Pods
          value: 2
          periodSeconds: 120             # remove max 2 pods per 2 minutes
```

### Stabilization windows

- **Scale up:** No stabilization — respond immediately to queue depth spikes (campaign bursts)
- **Scale down:** 5-minute stabilization window — prevents oscillation after a burst drains. Burst traffic patterns often have secondary spikes; premature scale-down followed by immediate scale-up wastes pod startup time

---

## 4. Rolling Update Safety

```yaml
strategy:
  type: RollingUpdate
  rollingUpdate:
    maxUnavailable: 0    # never kill an old pod before a new one is ready
    maxSurge: 1          # only one extra pod at a time (controls total pod count)
```

### Why maxUnavailable=0

With `maxUnavailable: 1`, Kubernetes can terminate an old pod before the new pod passes its readiness probe. This creates a window where total capacity is reduced. For a queue processor, reduced capacity means growing queue depth.

With `maxUnavailable: 0` + `maxSurge: 1`, the sequence is:
1. New pod starts and passes readiness probe
2. Old pod receives SIGTERM and drains
3. Repeat

Queue depth is never negatively impacted during deployment.

---

## 5. Pod Disruption Budget

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: email-queue-worker-pdb
spec:
  minAvailable: 1
  selector:
    matchLabels:
      app: email-queue-worker
```

This ensures at least 1 replica remains running during:
- Node drains (`kubectl drain`)
- Cluster upgrades
- Voluntary disruptions (e.g. `kubectl delete pod`)

With `minReplicas: 2` in the HPA, the PDB allows one pod to be disrupted at a time, ensuring continuous processing.

---

## 6. Resource Requests & Limits

```yaml
resources:
  requests:
    cpu: 250m
    memory: 256Mi
  limits:
    cpu: 1000m       # allow CPU burst during high-throughput spikes
    memory: 512Mi    # hard cap — OOM kills are preferable to node memory exhaustion
```

### Rationale

- **CPU request (250m):** Baseline for goroutine scheduling at moderate load. The Go scheduler is efficient; 50 worker goroutines do not require 50 CPUs.
- **CPU limit (1000m):** Allows burst during campaign spikes without affecting neighbours. Golang's runtime scales goroutine parallelism with GOMAXPROCS, which is capped by the CPU limit automatically (via automaxprocs library).
- **Memory request (256Mi):** Covers Go runtime, goroutine stacks (50 × ~8KB = 400KB), in-flight task payloads, and Redis connection pool.
- **Memory limit (512Mi):** Hard cap prevents a runaway retry queue from OOMing the node. An OOM kill is detected by the liveness probe (pod restarts); a node OOM affects all pods on the node.

### automaxprocs

Add `go.uber.org/automaxprocs` to `main.go`:
```go
import _ "go.uber.org/automaxprocs"  // sets GOMAXPROCS = CPU limit (not node CPUs)
```

Without this, `GOMAXPROCS` defaults to the number of node CPUs (e.g. 96), causing the Go scheduler to spin up many OS threads despite the CPU limit of 1000m — degrading performance.

---

## 7. ConfigMap & Secret Strategy

```yaml
# ConfigMap: non-sensitive operational config
apiVersion: v1
kind: ConfigMap
metadata:
  name: email-queue-config
data:
  WORKER_POOL_SIZE: "50"
  DRAIN_TIMEOUT: "30s"              # duration string — matches parseDuration() in config.go
  RETRY_BASE_DELAY: "1s"
  RETRY_MAX_DELAY: "15m"
  LOG_LEVEL: "info"
  LOG_FORMAT: "json"
  METRICS_PORT: "9090"
  HTTP_PORT: "8080"
  OTEL_SERVICE_NAME: "email-queue"

---
# Secret: sensitive credentials (never in ConfigMap, never in image)
apiVersion: v1
kind: Secret
metadata:
  name: email-queue-secrets
type: Opaque
stringData:
  REDIS_URL: "redis://:password@redis-master.redis.svc.cluster.local:6379"
  REDIS_PASSWORD: "..."
  API_KEYS: "key1,key2,key3"
  SENDGRID_API_KEY: "SG...."
```

**Principle:** Config that changes per environment belongs in ConfigMap. Credentials belong in Secret. Neither belongs in the container image. Use `envFrom` to inject both into the pod:

```yaml
envFrom:
  - configMapRef:
      name: email-queue-config
  - secretRef:
      name: email-queue-secrets
```

---

## 8. Security Context

```yaml
securityContext:
  runAsNonRoot: true
  runAsUser: 65532          # matches distroless nonroot user
  readOnlyRootFilesystem: true
  allowPrivilegeEscalation: false
  capabilities:
    drop: ["ALL"]
  seccompProfile:
    type: RuntimeDefault    # enables the node's default seccomp filter;
                            # required by PSA restricted profile and CIS benchmark
```

The distroless base image runs as user 65532 by default. `readOnlyRootFilesystem: true` prevents any runtime file writes (the binary only writes to stdout/stderr). `seccompProfile: RuntimeDefault` applies the containerd/runc default seccomp policy, blocking ~300 rarely-used syscalls without any application changes. This satisfies CIS Kubernetes Benchmark and Pod Security Admission (restricted) requirements.

---

## 9. Topology Spread

```yaml
topologySpreadConstraints:
  - maxSkew: 1
    topologyKey: topology.kubernetes.io/zone
    whenUnsatisfiable: ScheduleAnyway   # prefer spread; never block scheduling
    labelSelector:
      matchLabels:
        app: email-queue-worker
```

Ensures worker pods are spread across availability zones, preventing a full outage if one zone goes down.

`ScheduleAnyway` is used rather than `DoNotSchedule` so that pods are never left `Pending` in single-AZ clusters or during initial rollout when zone spread cannot be satisfied. Once multi-AZ topology is confirmed in production, tighten to `DoNotSchedule` for strict zone isolation.

---

## Failure Scenarios

| Scenario | Detection | Response |
|----------|-----------|----------|
| Redis unreachable | Liveness probe fails (3 × 10s = 30s) | Pod restarted; PDB keeps 1 pod alive |
| Worker pod OOMKilled | K8s event + Prometheus `kube_pod_container_status_last_terminated_reason` | Pod restarted; PEL retains in-flight tasks |
| Rolling deploy | readyz 503 on old pod | New pod must pass readiness before old pod SIGTERM |
| Node drain | PDB blocks drain until min 1 available | Graceful drain before eviction |
| Campaign spike | HPA `email_queue_depth_total` metric exceeds threshold | Scale out across all priority queues; stabilization prevents thrash |
| SIGTERM under load | Drain timeout | Tasks not ACKed → PEL → reclaimed by XAUTOCLAIM on surviving pods |

---

## Consequences

**What becomes easier:**
- Zero-message-loss rolling deployments are guaranteed by `maxUnavailable: 0`
- HPA responds to actual work backlog (total queue depth across all priorities), not proxy signals (CPU)
- Security baseline is strong (non-root, read-only FS, no capabilities, RuntimeDefault seccomp)
- Topology spread prevents single-AZ failures from taking down all workers

**What becomes harder:**
- Custom HPA metrics require `prometheus-adapter` deployed in the cluster **and** the `PrometheusRule` recording rules from §3 applied
- `terminationGracePeriodSeconds: 60` means rolling deployments take ~60s per pod — a 10-pod deployment takes ~10 minutes (acceptable for this workload)
- `readOnlyRootFilesystem: true` requires any temp file writes (e.g. profiling) to use `emptyDir` volumes

**What we will need to revisit:**
- If email provider latency degrades beyond 30s, increase `DRAIN_TIMEOUT` and `terminationGracePeriodSeconds` accordingly
- If the cluster does not support custom metrics, fall back to CPU-based HPA with a conservative target (50%)
- Change `whenUnsatisfiable` from `ScheduleAnyway` to `DoNotSchedule` once multi-AZ topology is verified

---

## Action Items

1. [ ] Implement `GET /healthz` handler with supervisor heartbeat + Redis PING check
2. [ ] Implement `GET /readyz` handler with readiness flag (SetReady/IsReady)
3. [ ] **(M3)** Move `SetReady(true)` to after consumer group initialisation succeeds
4. [ ] Set `terminationGracePeriodSeconds: 60` in Deployment spec
5. [ ] Add `preStop: sleep 5` lifecycle hook
6. [ ] Deploy `PrometheusRule` CR with `email_queue_depth_total` and `email_queue_worker_saturation` recording rules
7. [ ] Configure HPA with `email_queue_depth_total` (all queues) and `email_queue_worker_saturation` custom metrics
8. [ ] Create PodDisruptionBudget with `minAvailable: 1`
9. [ ] Set `maxUnavailable: 0, maxSurge: 1` rolling update strategy
10. [ ] Add `go.uber.org/automaxprocs` import to `cmd/server/main.go`
11. [ ] Set `readOnlyRootFilesystem: true`, drop all capabilities, and add `seccompProfile: RuntimeDefault`
12. [ ] Add topology spread constraints with `ScheduleAnyway`; tighten to `DoNotSchedule` post multi-AZ verification
