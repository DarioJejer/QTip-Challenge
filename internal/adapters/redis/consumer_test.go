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
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// consumerTestEnv groups all objects needed for consumer tests.
type consumerTestEnv struct {
	consumer *redisadapter.RedisConsumer
	client   *redis.Client
	mr       *miniredis.Miniredis
	cfg      *config.Config
}

const (
	testGroup    = "test-group"
	testConsumer = "test-consumer"
)

// newConsumerTestEnv starts a miniredis server and builds a RedisConsumer with
// a short BlockTimeout so tests complete promptly.
func newConsumerTestEnv(t *testing.T) *consumerTestEnv {
	t.Helper()
	mr := miniredis.RunT(t)

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	cfg := &config.Config{
		Worker: config.WorkerConfig{
			PoolSize:      10,
			ConsumerGroup: testGroup,
			ConsumerName:  testConsumer,
			BlockTimeout:  100 * time.Millisecond,
		},
	}

	tracer := noop.NewTracerProvider().Tracer("test")
	c := redisadapter.NewRedisConsumer(client, cfg, &noopMetrics{}, tracer)

	return &consumerTestEnv{consumer: c, client: client, mr: mr, cfg: cfg}
}

// seedStream pre-creates a consumer group at position "0" (so it reads already
// added messages), XADDs one task payload, and returns the message ID.
//
// Using position "0" for the group allows tests to seed data before calling
// Consume without racing against the XREADGROUP goroutine.
func seedStream(t *testing.T, env *consumerTestEnv, task *domain.EmailTask) (streamKey, msgID string) {
	t.Helper()
	ctx := context.Background()

	streamKey = task.Priority.QueueName()

	// Create group at 0 so pre-seeded messages are visible to XREADGROUP.
	err := env.client.XGroupCreateMkStream(ctx, streamKey, testGroup, "0").Err()
	require.NoError(t, err)

	payload, err := json.Marshal(task)
	require.NoError(t, err)

	msgID, err = env.client.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey,
		ID:     "*",
		Values: map[string]any{
			"id":       task.ID,
			"payload":  string(payload),
			"tenant_id": task.TenantID,
		},
	}).Result()
	require.NoError(t, err)

	return streamKey, msgID
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestConsume_ReceivesMessages verifies that a task added to a priority stream
// is delivered to the channel returned by Consume.
func TestConsume_ReceivesMessages(t *testing.T) {
	env := newConsumerTestEnv(t)
	task := validTask("consume-001")
	task.Priority = domain.PriorityNormal

	seedStream(t, env, task)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := env.consumer.Consume(ctx)
	require.NoError(t, err)

	select {
	case received, ok := <-ch:
		require.True(t, ok, "channel must be open")
		assert.Equal(t, task.ID, received.ID)
		assert.Equal(t, task.TenantID, received.TenantID)
		assert.NotEmpty(t, received.Metadata["_stream_id"], "stream message ID must be stamped")
		assert.NotEmpty(t, received.Metadata["_stream_key"], "stream key must be stamped")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting to receive task from channel")
	}
}

// TestAcknowledge_RemovesFromPEL verifies that calling Acknowledge after
// consuming a task removes the message from the PEL (pending entry list).
func TestAcknowledge_RemovesFromPEL(t *testing.T) {
	env := newConsumerTestEnv(t)
	task := validTask("ack-001")
	task.Priority = domain.PriorityHigh

	streamKey, _ := seedStream(t, env, task)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := env.consumer.Consume(ctx)
	require.NoError(t, err)

	var received *domain.EmailTask
	select {
	case received = <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for task")
	}

	// PEL should have one entry before ACK.
	pending, err := env.client.XPending(context.Background(), streamKey, testGroup).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), pending.Count, "one message should be in PEL before ACK")

	// ACK removes it from PEL.
	require.NoError(t, env.consumer.Acknowledge(context.Background(), received))

	pending, err = env.client.XPending(context.Background(), streamKey, testGroup).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), pending.Count, "PEL should be empty after ACK")
}

// TestNack_LeavesMessageInPEL verifies that calling Nack does NOT remove the
// message from the PEL — it remains for redelivery via ClaimStale (ADR-004).
func TestNack_LeavesMessageInPEL(t *testing.T) {
	env := newConsumerTestEnv(t)
	task := validTask("nack-001")
	task.Priority = domain.PriorityCritical

	streamKey, _ := seedStream(t, env, task)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := env.consumer.Consume(ctx)
	require.NoError(t, err)

	var received *domain.EmailTask
	select {
	case received = <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for task")
	}

	// Nack should return nil and leave the message in PEL.
	require.NoError(t, env.consumer.Nack(context.Background(), received, fmt.Errorf("simulated worker error")))

	pending, err := env.client.XPending(context.Background(), streamKey, testGroup).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), pending.Count, "PEL must still contain the message after NACK")
}

// TestClaimStale_ReclaimsIdleMessages verifies that ClaimStale returns tasks
// whose PEL entries have been idle longer than idleThreshold.
func TestClaimStale_ReclaimsIdleMessages(t *testing.T) {
	env := newConsumerTestEnv(t)
	task := validTask("stale-001")
	task.Priority = domain.PriorityNormal

	seedStream(t, env, task)

	// Consume the task so it lands in the PEL (without acknowledging).
	consumeCtx, consumeCancel := context.WithCancel(context.Background())
	ch, err := env.consumer.Consume(consumeCtx)
	require.NoError(t, err)

	select {
	case <-ch: // task received and sitting in PEL
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for task consumption")
	}
	consumeCancel() // stop the poll goroutine

	// Advance miniredis clock past the idle threshold.
	idleThreshold := 30 * time.Second
	env.mr.FastForward(idleThreshold + time.Second)

	// ClaimStale should reclaim the unacknowledged message.
	claimed, err := env.consumer.ClaimStale(context.Background(), idleThreshold)
	require.NoError(t, err)
	require.Len(t, claimed, 1, "one stale message should be reclaimed")
	assert.Equal(t, task.ID, claimed[0].ID)
	assert.NotEmpty(t, claimed[0].Metadata["_stream_id"])
}

// TestConsumerGroupAutoCreate_Idempotent verifies that calling Consume twice
// does not fail even though the consumer groups already exist (BUSYGROUP is
// silently ignored on the second call).
func TestConsumerGroupAutoCreate_Idempotent(t *testing.T) {
	env := newConsumerTestEnv(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// First call — creates all four consumer groups.
	_, err := env.consumer.Consume(ctx)
	require.NoError(t, err, "first Consume should succeed")

	cancel() // stop the first goroutine

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	// Second call — groups already exist; BUSYGROUP must be silently ignored.
	_, err = env.consumer.Consume(ctx2)
	require.NoError(t, err, "second Consume must not fail on BUSYGROUP")
}
