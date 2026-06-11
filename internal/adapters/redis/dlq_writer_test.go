package redis_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	redisadapter "github.com/DarioJejer/go-email-queue/internal/adapters/redis"
	"github.com/DarioJejer/go-email-queue/internal/config"
	"github.com/DarioJejer/go-email-queue/internal/domain"
)

const testDLQKey = "queue:dlq:tenant-1:transactional"

type dlqTestEnv struct {
	writer *redisadapter.RedisDLQWriter
	client *redis.Client
	mr     *miniredis.Miniredis
	cfg    *config.Config
}

func newDLQTestEnv(t *testing.T) *dlqTestEnv {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	cfg := &config.Config{
		Retry: config.RetryConfig{
			DLQTTLSeconds: 3600,
		},
	}
	return &dlqTestEnv{
		writer: redisadapter.NewRedisDLQWriter(client, cfg, &noopMetrics{}),
		client: client,
		mr:     mr,
		cfg:    cfg,
	}
}

func sampleDLQEntry(taskID, reason string) *domain.DLQEntry {
	return &domain.DLQEntry{
		Task: &domain.EmailTask{
			ID:       taskID,
			TenantID: "tenant-1",
			Type:     domain.TaskTypeTransactional,
			Attempt:  3,
		},
		DeadAt:        time.Now().UTC(),
		FailureReason: reason,
		FinalError:    "smtp timeout",
	}
}

func TestSendToDLQ_AppendsToList(t *testing.T) {
	env := newDLQTestEnv(t)
	ctx := context.Background()

	require.NoError(t, env.writer.SendToDLQ(ctx, sampleDLQEntry("dlq-01", "max attempts exceeded")))

	depth, err := env.client.LLen(ctx, testDLQKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), depth)
}

func TestSendToDLQ_SetsTTL(t *testing.T) {
	env := newDLQTestEnv(t)
	ctx := context.Background()

	require.NoError(t, env.writer.SendToDLQ(ctx, sampleDLQEntry("dlq-ttl-01", "non-retryable error")))

	ttl := env.mr.TTL(testDLQKey)
	assert.Greater(t, ttl, time.Duration(0))
	assert.LessOrEqual(t, ttl, time.Duration(env.cfg.Retry.DLQTTLSeconds)*time.Second)
}

func TestListDLQ_ReturnsInOrder(t *testing.T) {
	env := newDLQTestEnv(t)
	ctx := context.Background()

	for _, id := range []string{"first", "second", "third"} {
		require.NoError(t, env.writer.SendToDLQ(ctx, sampleDLQEntry(id, "max attempts exceeded")))
	}

	entries, err := env.writer.ListDLQ(ctx, "tenant-1", string(domain.TaskTypeTransactional), 10)
	require.NoError(t, err)
	require.Len(t, entries, 3)
	assert.Equal(t, "first", entries[0].Task.ID)
	assert.Equal(t, "second", entries[1].Task.ID)
	assert.Equal(t, "third", entries[2].Task.ID)
}

func TestDLQDepth_ReturnsCorrectCount(t *testing.T) {
	env := newDLQTestEnv(t)
	ctx := context.Background()

	depth, err := env.writer.DLQDepth(ctx, "tenant-1", string(domain.TaskTypeTransactional))
	require.NoError(t, err)
	assert.Equal(t, int64(0), depth)

	require.NoError(t, env.writer.SendToDLQ(ctx, sampleDLQEntry("depth-01", "max attempts exceeded")))
	require.NoError(t, env.writer.SendToDLQ(ctx, sampleDLQEntry("depth-02", "max attempts exceeded")))

	depth, err = env.writer.DLQDepth(ctx, "tenant-1", string(domain.TaskTypeTransactional))
	require.NoError(t, err)
	assert.Equal(t, int64(2), depth)
}
