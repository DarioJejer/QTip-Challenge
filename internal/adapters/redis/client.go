// Package redis provides the Redis client factory and lifecycle management for
// the email queue service. All Redis adapter implementations (producer,
// consumer, idempotency store, etc.) receive a *redis.Client constructed here.
package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"

	"github.com/DarioJejer/go-email-queue/internal/config"
)

// NewRedisClient constructs a go-redis Client from the application config,
// verifies connectivity with a PING, and registers Prometheus pool-stat gauges
// against the provided registry.
//
// The reg parameter must not be nil. Pass prometheus.DefaultRegisterer in
// production (main.go) and prometheus.NewRegistry() in tests so each test
// gets an isolated registry with no global state pollution.
//
// Connection pool sizing (ADR-006):
//
//	PoolSize = WORKER_POOL_SIZE * 1.5
//
// This ensures workers never contend for connections even under full
// saturation. The default config sets PoolSize=75 for PoolSize=50 workers.
//
// Returns an error if the PING fails (Redis unreachable or auth rejected).
func NewRedisClient(cfg *config.Config, reg prometheus.Registerer) (*redis.Client, error) {
	opts := &redis.Options{
		Addr:         addrFromURL(cfg.Redis.URL),
		Password:     cfg.Redis.Password,
		DB:           cfg.Redis.DB,
		PoolSize:     cfg.Redis.PoolSize,
		MinIdleConns: cfg.Redis.MinIdleConns,
		DialTimeout:  cfg.Redis.DialTimeout,
		ReadTimeout:  cfg.Redis.ReadTimeout,
		WriteTimeout: cfg.Redis.WriteTimeout,
		OnConnect: func(ctx context.Context, cn *redis.Conn) error {
			log.Info().
				Str("component", "redis").
				Msg("redis: new connection established")
			return nil
		},
	}

	client := redis.NewClient(opts)

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Redis.DialTimeout)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redis: ping failed (addr=%s): %w", opts.Addr, err)
	}

	log.Info().
		Str("component", "redis").
		Str("addr", opts.Addr).
		Int("pool_size", opts.PoolSize).
		Int("min_idle_conns", opts.MinIdleConns).
		Msg("redis: client connected")

	registerPoolMetrics(client, reg)

	return client, nil
}

// addrFromURL converts a Redis URL (redis://host:port) or plain host:port
// string into the addr format expected by go-redis Options.Addr.
// go-redis also accepts full URLs via ParseURL, but keeping this simple
// avoids requiring a URL scheme in all environments.
func addrFromURL(url string) string {
	// Strip common scheme prefixes so callers can pass either form.
	for _, prefix := range []string{"redis://", "rediss://"} {
		if len(url) > len(prefix) && url[:len(prefix)] == prefix {
			return url[len(prefix):]
		}
	}
	return url
}

// ---------------------------------------------------------------------------
// RedisClientLifecycle — shutdown and health
// ---------------------------------------------------------------------------

// RedisClientLifecycle wraps a *redis.Client with graceful-close and
// health-check behaviour. The DI wiring in main.go stores this alongside the
// raw client so the errgroup shutdown sequence can call Close().
type RedisClientLifecycle struct {
	client *redis.Client
}

// NewRedisClientLifecycle wraps client with lifecycle management.
func NewRedisClientLifecycle(client *redis.Client) *RedisClientLifecycle {
	return &RedisClientLifecycle{client: client}
}

// HealthCheck pings Redis with a 2-second timeout. Used by the /healthz probe.
func (l *RedisClientLifecycle) HealthCheck(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := l.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis: health check failed: %w", err)
	}
	return nil
}

// Close logs the current pool statistics then closes all connections.
// Call this during the graceful shutdown sequence after all workers have
// drained (ADR-008).
func (l *RedisClientLifecycle) Close() error {
	stats := l.client.PoolStats()
	log.Info().
		Str("component", "redis").
		Uint32("hits", stats.Hits).
		Uint32("misses", stats.Misses).
		Uint32("timeouts", stats.Timeouts).
		Uint32("total_conns", stats.TotalConns).
		Uint32("idle_conns", stats.IdleConns).
		Uint32("stale_conns", stats.StaleConns).
		Msg("redis: closing client — final pool stats")

	if err := l.client.Close(); err != nil {
		return fmt.Errorf("redis: close: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Prometheus pool metrics (ADR-007)
// ---------------------------------------------------------------------------

// registerPoolMetrics registers a poolCollector against reg.
//
// Why prometheus.Registerer rather than ports.MetricsRecorder?
// Pool stats are infrastructure-level pull metrics: Prometheus calls Collect()
// on each scrape. ports.MetricsRecorder is designed for application-level push
// events (enqueues, failures, retries). Mixing these two models would make
// MetricsRecorder responsible for managing scrape-time state, which is not its
// contract. The correct separation is:
//   - ports.MetricsRecorder  →  application events recorded imperatively
//   - prometheus.Registerer  →  infrastructure Collectors registered once at startup
//
// Both are injected from main.go; neither reaches for a global.
func registerPoolMetrics(client *redis.Client, reg prometheus.Registerer) {
	// Ignore "already registered" errors — safe when the same binary registers
	// multiple clients (e.g. read replica) against the same registry.
	_ = reg.Register(newPoolCollector(client))
}

// poolCollector implements prometheus.Collector for Redis pool statistics.
type poolCollector struct {
	client     *redis.Client
	totalConns *prometheus.Desc
	idleConns  *prometheus.Desc
	staleConns *prometheus.Desc
}

func newPoolCollector(client *redis.Client) *poolCollector {
	return &poolCollector{
		client: client,
		totalConns: prometheus.NewDesc(
			"redis_pool_total_conns",
			"Total number of connections in the Redis pool (active + idle).",
			nil, nil,
		),
		idleConns: prometheus.NewDesc(
			"redis_pool_idle_conns",
			"Number of idle connections in the Redis pool.",
			nil, nil,
		),
		staleConns: prometheus.NewDesc(
			"redis_pool_stale_conns",
			"Number of stale connections removed from the Redis pool.",
			nil, nil,
		),
	}
}

func (c *poolCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.totalConns
	ch <- c.idleConns
	ch <- c.staleConns
}

func (c *poolCollector) Collect(ch chan<- prometheus.Metric) {
	stats := c.client.PoolStats()
	ch <- prometheus.MustNewConstMetric(c.totalConns, prometheus.GaugeValue, float64(stats.TotalConns))
	ch <- prometheus.MustNewConstMetric(c.idleConns, prometheus.GaugeValue, float64(stats.IdleConns))
	ch <- prometheus.MustNewConstMetric(c.staleConns, prometheus.GaugeValue, float64(stats.StaleConns))
}
