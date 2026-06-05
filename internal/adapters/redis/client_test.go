package redis_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	redisadapter "github.com/DarioJejer/go-email-queue/internal/adapters/redis"
	"github.com/DarioJejer/go-email-queue/internal/config"
)

// newTestConfig returns a config.Config pointing at the given miniredis addr.
func newTestConfig(addr string) *config.Config {
	return &config.Config{
		Redis: config.RedisConfig{
			URL:          addr,
			Password:     "",
			DB:           0,
			PoolSize:     5,
			MinIdleConns: 1,
			DialTimeout:  2 * time.Second,
			ReadTimeout:  2 * time.Second,
			WriteTimeout: 2 * time.Second,
		},
	}
}

// testRegistry returns a fresh isolated Prometheus registry for each test.
// Using prometheus.NewRegistry() instead of prometheus.DefaultRegisterer
// ensures no global state leaks between tests — each test owns its own
// collector set and there are no "already registered" conflicts.
func testRegistry() prometheus.Registerer {
	return prometheus.NewRegistry()
}

// ---------------------------------------------------------------------------
// NewRedisClient
// ---------------------------------------------------------------------------

func TestNewRedisClient_Success(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := newTestConfig(mr.Addr())

	client, err := redisadapter.NewRedisClient(cfg, testRegistry())
	require.NoError(t, err)
	require.NotNil(t, client)
	t.Cleanup(func() { _ = client.Close() })

	// Verify the client is actually connected by running a SET/GET.
	ctx := context.Background()
	require.NoError(t, client.Set(ctx, "ping-test", "pong", 0).Err())
	val, err := client.Get(ctx, "ping-test").Result()
	require.NoError(t, err)
	assert.Equal(t, "pong", val)
}

func TestNewRedisClient_Unreachable(t *testing.T) {
	cfg := newTestConfig("localhost:19999") // nothing listening here
	cfg.Redis.DialTimeout = 300 * time.Millisecond

	client, err := redisadapter.NewRedisClient(cfg, testRegistry())
	assert.Error(t, err, "should fail when Redis is unreachable")
	assert.Nil(t, client)
	assert.Contains(t, err.Error(), "ping failed")
}

// ---------------------------------------------------------------------------
// RedisClientLifecycle.HealthCheck
// ---------------------------------------------------------------------------

func TestHealthCheck_Success(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := newTestConfig(mr.Addr())

	client, err := redisadapter.NewRedisClient(cfg, testRegistry())
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	lc := redisadapter.NewRedisClientLifecycle(client)
	assert.NoError(t, lc.HealthCheck(context.Background()))
}

func TestHealthCheck_FailsWhenServerDown(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := newTestConfig(mr.Addr())

	client, err := redisadapter.NewRedisClient(cfg, testRegistry())
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	lc := redisadapter.NewRedisClientLifecycle(client)

	// Stop miniredis to simulate Redis going away.
	mr.Close()

	err = lc.HealthCheck(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "health check failed")
}

// ---------------------------------------------------------------------------
// LoadScripts / ScriptRegistry
// ---------------------------------------------------------------------------

func TestLoadScripts_Success(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := newTestConfig(mr.Addr())

	client, err := redisadapter.NewRedisClient(cfg, testRegistry())
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	registry, err := redisadapter.LoadScripts(context.Background(), client)
	require.NoError(t, err)
	require.NotNil(t, registry)
	assert.NotEmpty(t, registry.IdempotencySHA, "SHA must be populated after SCRIPT LOAD")
	assert.Len(t, registry.IdempotencySHA, 40, "Redis script SHAs are 40 hex characters")
}

func TestLoadScripts_Idempotent(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := newTestConfig(mr.Addr())

	client, err := redisadapter.NewRedisClient(cfg, testRegistry())
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	r1, err := redisadapter.LoadScripts(ctx, client)
	require.NoError(t, err)

	r2, err := redisadapter.LoadScripts(ctx, client)
	require.NoError(t, err)

	// SCRIPT LOAD is idempotent — same script always produces same SHA.
	assert.Equal(t, r1.IdempotencySHA, r2.IdempotencySHA)
}

// ---------------------------------------------------------------------------
// RedisClientLifecycle.Close
// ---------------------------------------------------------------------------

func TestClose_Success(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := newTestConfig(mr.Addr())

	client, err := redisadapter.NewRedisClient(cfg, testRegistry())
	require.NoError(t, err)

	lc := redisadapter.NewRedisClientLifecycle(client)
	assert.NoError(t, lc.Close())
}

// ---------------------------------------------------------------------------
// Pool metrics — collector registers against the injected registry
// ---------------------------------------------------------------------------

func TestNewRedisClient_RegistersPoolMetrics(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := newTestConfig(mr.Addr())

	reg := prometheus.NewRegistry()
	client, err := redisadapter.NewRedisClient(cfg, reg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	// Gathering from the isolated registry must return the three pool metrics
	// without error. The global DefaultRegisterer must be untouched.
	mfs, err := reg.Gather()
	require.NoError(t, err)

	names := make(map[string]bool)
	for _, mf := range mfs {
		names[mf.GetName()] = true
	}
	assert.True(t, names["redis_pool_total_conns"], "redis_pool_total_conns must be registered")
	assert.True(t, names["redis_pool_idle_conns"], "redis_pool_idle_conns must be registered")
	assert.True(t, names["redis_pool_stale_conns"], "redis_pool_stale_conns must be registered")
}
