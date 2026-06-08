package redis_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"

	redisadapter "github.com/DarioJejer/go-email-queue/internal/adapters/redis"
	"github.com/DarioJejer/go-email-queue/internal/config"
	"github.com/DarioJejer/go-email-queue/internal/domain"
	"github.com/DarioJejer/go-email-queue/internal/ports"
)

// ---------------------------------------------------------------------------
// Test doubles
// ---------------------------------------------------------------------------

// noopMetrics is a test double for ports.MetricsRecorder that silently drops
// every observation. It keeps producer tests focused on Redis behaviour.
type noopMetrics struct{}

func (n *noopMetrics) RecordEnqueued(_, _, _ string)                    {}
func (n *noopMetrics) RecordProcessed(_, _, _ string, _ float64)        {}
func (n *noopMetrics) RecordQueueDepth(_ string, _ float64)             {}
func (n *noopMetrics) RecordDLQDepth(_, _ string, _ float64)            {}
func (n *noopMetrics) RecordWorkerStats(_ domain.WorkerStats)           {}

var _ ports.MetricsRecorder = (*noopMetrics)(nil)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// testEnv groups the objects needed across multiple test assertions.
type testEnv struct {
	producer *redisadapter.RedisProducer
	client   *redis.Client
	mr       *miniredis.Miniredis
}

// newTestEnv starts a miniredis server, connects a go-redis client, and builds
// a RedisProducer with a noop tracer and noop metrics.
func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	mr := miniredis.RunT(t)

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	tracer := noop.NewTracerProvider().Tracer("test")
	p := redisadapter.NewRedisProducer(client, &config.Config{}, &noopMetrics{}, tracer)

	return &testEnv{producer: p, client: client, mr: mr}
}

// validTask returns a minimal EmailTask that passes domain.Validate().
func validTask(id string) *domain.EmailTask {
	return &domain.EmailTask{
		ID:          id,
		TenantID:    "tenant-1",
		Type:        domain.TaskTypeTransactional,
		Priority:    domain.PriorityNormal,
		Recipient:   "user@example.com",
		TemplateID:  "welcome",
		EnqueuedAt:  time.Now(),
		MaxAttempts: 5,
		Status:      domain.StatusPending,
	}
}

// xlen returns the length of a Redis Stream via the test client.
func xlen(t *testing.T, client *redis.Client, stream string) int64 {
	t.Helper()
	n, err := client.XLen(context.Background(), stream).Result()
	require.NoError(t, err)
	return n
}

// zcard returns the cardinality of a Redis sorted set via the test client.
func zcard(t *testing.T, client *redis.Client, key string) int64 {
	t.Helper()
	n, err := client.ZCard(context.Background(), key).Result()
	require.NoError(t, err)
	return n
}

// ---------------------------------------------------------------------------
// Enqueue tests
// ---------------------------------------------------------------------------

func TestEnqueue_Success(t *testing.T) {
	env := newTestEnv(t)
	task := validTask("task-001")

	err := env.producer.Enqueue(context.Background(), task)
	require.NoError(t, err)

	assert.Equal(t, int64(1), xlen(t, env.client, task.Priority.QueueName()),
		"stream should contain exactly one message after enqueue")
}

func TestEnqueue_ValidationFailure(t *testing.T) {
	env := newTestEnv(t)

	task := &domain.EmailTask{
		ID:          "task-002",
		Type:        domain.TaskTypeTransactional,
		Priority:    domain.PriorityNormal,
		Recipient:   "user@example.com",
		MaxAttempts: 5,
		// TenantID intentionally missing — should fail Validate().
	}

	err := env.producer.Enqueue(context.Background(), task)
	require.Error(t, err)

	var valErr *domain.ValidationError
	require.ErrorAs(t, err, &valErr, "should return a ValidationError")
	assert.Equal(t, "tenant_id", valErr.Field)

	// Nothing written to Redis — validate before touching the stream.
	assert.Equal(t, int64(0), xlen(t, env.client, task.Priority.QueueName()))
}

func TestEnqueue_RedisError(t *testing.T) {
	// Use a client pointing at an unreachable address to simulate a Redis failure.
	client := redis.NewClient(&redis.Options{
		Addr:        "localhost:1",
		DialTimeout: 50 * time.Millisecond,
	})
	t.Cleanup(func() { _ = client.Close() })

	tracer := noop.NewTracerProvider().Tracer("test")
	p := redisadapter.NewRedisProducer(client, &config.Config{}, &noopMetrics{}, tracer)

	err := p.Enqueue(context.Background(), validTask("task-003"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "task-003", "error should identify the failing task ID")
}

// ---------------------------------------------------------------------------
// EnqueueBatch tests
// ---------------------------------------------------------------------------

func TestEnqueueBatch_AllSuccess(t *testing.T) {
	env := newTestEnv(t)

	tasks := []*domain.EmailTask{
		validTask("batch-001"),
		validTask("batch-002"),
		validTask("batch-003"),
	}
	// Route across all three non-low priority streams.
	tasks[0].Priority = domain.PriorityNormal
	tasks[1].Priority = domain.PriorityHigh
	tasks[2].Priority = domain.PriorityCritical

	err := env.producer.EnqueueBatch(context.Background(), tasks)
	require.NoError(t, err)

	assert.Equal(t, int64(1), xlen(t, env.client, domain.PriorityNormal.QueueName()), "normal stream")
	assert.Equal(t, int64(1), xlen(t, env.client, domain.PriorityHigh.QueueName()), "high stream")
	assert.Equal(t, int64(1), xlen(t, env.client, domain.PriorityCritical.QueueName()), "critical stream")
}

func TestEnqueueBatch_PartialFailure(t *testing.T) {
	env := newTestEnv(t)

	tasks := []*domain.EmailTask{
		validTask("ok-001"),
		{
			// Missing TenantID.
			ID: "bad-002", Type: domain.TaskTypeTransactional,
			Priority: domain.PriorityNormal, Recipient: "a@b.com", MaxAttempts: 5,
		},
		{
			// Missing Recipient.
			ID: "bad-003", TenantID: "t", Type: domain.TaskTypeTransactional,
			Priority: domain.PriorityNormal, MaxAttempts: 5,
		},
	}

	err := env.producer.EnqueueBatch(context.Background(), tasks)
	require.Error(t, err)

	var multiErr *domain.MultiError
	require.ErrorAs(t, err, &multiErr, "should return a MultiError")
	assert.Len(t, multiErr.Errors, 2, "both invalid tasks should be reported")

	// Validation failure aborts before writing — no messages in Redis.
	assert.Equal(t, int64(0), xlen(t, env.client, domain.PriorityNormal.QueueName()))
}

func TestEnqueueBatch_Empty(t *testing.T) {
	env := newTestEnv(t)

	err := env.producer.EnqueueBatch(context.Background(), nil)
	require.NoError(t, err, "empty batch should be a no-op")
}

// ---------------------------------------------------------------------------
// EnqueueDelayed tests
// ---------------------------------------------------------------------------

func TestEnqueueDelayed_Success(t *testing.T) {
	env := newTestEnv(t)
	task := validTask("delayed-001")
	delay := 5 * time.Minute

	before := time.Now()
	err := env.producer.EnqueueDelayed(context.Background(), task, delay)
	require.NoError(t, err)

	// Verify exactly one entry landed in the delayed sorted set.
	assert.Equal(t, int64(1), zcard(t, env.client, "queue:email:delayed"),
		"delayed set should contain exactly one entry")

	// Retrieve the member and deserialise to confirm ScheduledFor is set.
	members, err := env.client.ZRangeByScore(context.Background(), "queue:email:delayed",
		&redis.ZRangeBy{Min: "-inf", Max: "+inf"}).Result()
	require.NoError(t, err)
	require.Len(t, members, 1)

	var stored domain.EmailTask
	require.NoError(t, json.Unmarshal([]byte(members[0]), &stored))
	require.NotNil(t, stored.ScheduledFor, "ScheduledFor must be set")
	assert.WithinDuration(t, before.Add(delay), *stored.ScheduledFor, 5*time.Second)
}
