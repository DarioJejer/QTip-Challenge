package config_test

import (
	"strings"
	"testing"
	"time"

	"github.com/DarioJejer/go-email-queue/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setEnv sets env vars from a map and returns a cleanup function that restores
// original values. Call t.Cleanup(cleanup) immediately after.
func setEnv(t *testing.T, vars map[string]string) {
	t.Helper()
	for k, v := range vars {
		t.Setenv(k, v)
	}
}

// ---------------------------------------------------------------------------
// Load — defaults
// ---------------------------------------------------------------------------

func TestLoad_Defaults(t *testing.T) {
	// Do not set any env vars; everything must come from defaults.
	cfg, err := config.Load()
	require.NoError(t, err)

	// Redis
	assert.Equal(t, "", cfg.Redis.URL, "REDIS_URL should default to empty")
	assert.Equal(t, 75, cfg.Redis.PoolSize)
	assert.Equal(t, 10, cfg.Redis.MinIdleConns)
	assert.Equal(t, 5*time.Second, cfg.Redis.DialTimeout)
	assert.Equal(t, 3*time.Second, cfg.Redis.ReadTimeout)
	assert.Equal(t, 3*time.Second, cfg.Redis.WriteTimeout)
	assert.Equal(t, 0, cfg.Redis.DB)

	// Worker
	assert.Equal(t, 50, cfg.Worker.PoolSize)
	assert.Equal(t, "email-workers", cfg.Worker.ConsumerGroup)
	assert.NotEmpty(t, cfg.Worker.ConsumerName, "ConsumerName must be hostname:pid")
	assert.Contains(t, cfg.Worker.ConsumerName, ":")
	assert.Equal(t, 5*time.Second, cfg.Worker.BlockTimeout)
	assert.Equal(t, 30*time.Second, cfg.Worker.ClaimIdleThreshold)
	assert.Equal(t, 30*time.Second, cfg.Worker.DrainTimeout)

	// Retry
	assert.Equal(t, 1*time.Second, cfg.Retry.BaseDelay)
	assert.Equal(t, 15*time.Minute, cfg.Retry.MaxDelay)
	assert.InDelta(t, 0.2, cfg.Retry.JitterFactor, 0.0001)
	assert.Equal(t, 1*time.Second, cfg.Retry.SchedulerInterval)
	assert.Equal(t, 604800, cfg.Retry.DLQTTLSeconds)

	// Observability
	assert.Equal(t, "info", cfg.Observability.LogLevel)
	assert.Equal(t, "json", cfg.Observability.LogFormat)
	assert.Equal(t, "", cfg.Observability.OTELEndpoint)
	assert.Equal(t, "email-queue", cfg.Observability.ServiceName)
	assert.Equal(t, 9090, cfg.Observability.MetricsPort)

	// Server
	assert.Equal(t, 8080, cfg.Server.HTTPPort)
	assert.Nil(t, cfg.Server.APIKeys, "API_KEYS unset should yield nil slice")
	assert.Equal(t, 10*time.Second, cfg.Server.ReadTimeout)
	assert.Equal(t, 30*time.Second, cfg.Server.WriteTimeout)
}

// ---------------------------------------------------------------------------
// Load — env override
// ---------------------------------------------------------------------------

func TestLoad_EnvOverrides(t *testing.T) {
	setEnv(t, map[string]string{
		"REDIS_URL":                   "redis://redis.prod:6379",
		"REDIS_PASSWORD":              "s3cr3t",
		"REDIS_DB":                    "2",
		"REDIS_POOL_SIZE":             "100",
		"REDIS_MIN_IDLE_CONNS":        "20",
		"REDIS_DIAL_TIMEOUT":          "10s",
		"WORKER_POOL_SIZE":            "200",
		"WORKER_CONSUMER_GROUP":       "my-group",
		"WORKER_CONSUMER_NAME":        "worker-01",
		"WORKER_BLOCK_TIMEOUT":        "8s",
		"WORKER_CLAIM_IDLE_THRESHOLD": "1m",
		"DRAIN_TIMEOUT":               "45s",
		"RETRY_BASE_DELAY":            "2s",
		"RETRY_MAX_DELAY":             "10m",
		"RETRY_JITTER_FACTOR":         "0.3",
		"RETRY_SCHEDULER_INTERVAL":    "500ms",
		"DLQ_TTL_SECONDS":             "86400",
		"LOG_LEVEL":                   "debug",
		"LOG_FORMAT":                  "console",
		"OTEL_EXPORTER_OTLP_ENDPOINT": "grpc://collector:4317",
		"OTEL_SERVICE_NAME":           "email-queue-staging",
		"METRICS_PORT":                "9091",
		"HTTP_PORT":                   "8081",
		"API_KEYS":                    "key-alpha,key-beta",
		"HTTP_READ_TIMEOUT":           "15s",
		"HTTP_WRITE_TIMEOUT":          "60s",
	})

	cfg, err := config.Load()
	require.NoError(t, err)

	assert.Equal(t, "redis://redis.prod:6379", cfg.Redis.URL)
	assert.Equal(t, "s3cr3t", cfg.Redis.Password)
	assert.Equal(t, 2, cfg.Redis.DB)
	assert.Equal(t, 100, cfg.Redis.PoolSize)
	assert.Equal(t, 20, cfg.Redis.MinIdleConns)
	assert.Equal(t, 10*time.Second, cfg.Redis.DialTimeout)
	assert.Equal(t, 200, cfg.Worker.PoolSize)
	assert.Equal(t, "my-group", cfg.Worker.ConsumerGroup)
	assert.Equal(t, "worker-01", cfg.Worker.ConsumerName)
	assert.Equal(t, 8*time.Second, cfg.Worker.BlockTimeout)
	assert.Equal(t, 1*time.Minute, cfg.Worker.ClaimIdleThreshold)
	assert.Equal(t, 45*time.Second, cfg.Worker.DrainTimeout)
	assert.Equal(t, 2*time.Second, cfg.Retry.BaseDelay)
	assert.Equal(t, 10*time.Minute, cfg.Retry.MaxDelay)
	assert.InDelta(t, 0.3, cfg.Retry.JitterFactor, 0.0001)
	assert.Equal(t, 500*time.Millisecond, cfg.Retry.SchedulerInterval)
	assert.Equal(t, 86400, cfg.Retry.DLQTTLSeconds)
	assert.Equal(t, "debug", cfg.Observability.LogLevel)
	assert.Equal(t, "console", cfg.Observability.LogFormat)
	assert.Equal(t, "grpc://collector:4317", cfg.Observability.OTELEndpoint)
	assert.Equal(t, "email-queue-staging", cfg.Observability.ServiceName)
	assert.Equal(t, 9091, cfg.Observability.MetricsPort)
	assert.Equal(t, 8081, cfg.Server.HTTPPort)
	assert.Equal(t, []string{"key-alpha", "key-beta"}, cfg.Server.APIKeys)
	assert.Equal(t, 15*time.Second, cfg.Server.ReadTimeout)
	assert.Equal(t, 60*time.Second, cfg.Server.WriteTimeout)
}

// ---------------------------------------------------------------------------
// Validate — required fields and constraint violations
// ---------------------------------------------------------------------------

func TestValidate_ValidConfig(t *testing.T) {
	setEnv(t, map[string]string{
		"REDIS_URL": "redis://localhost:6379",
		"API_KEYS":  "my-secret-key",
	})
	cfg, err := config.Load()
	require.NoError(t, err)
	assert.NoError(t, cfg.Validate())
}

func TestValidate_MissingRedisURL(t *testing.T) {
	setEnv(t, map[string]string{"API_KEYS": "key1"})
	cfg, err := config.Load()
	require.NoError(t, err)

	err = cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "REDIS_URL")
}

func TestValidate_MissingAPIKeys(t *testing.T) {
	setEnv(t, map[string]string{"REDIS_URL": "redis://localhost:6379"})
	cfg, err := config.Load()
	require.NoError(t, err)

	err = cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "API_KEYS")
}

func TestValidate_MultipleErrors(t *testing.T) {
	// Neither REDIS_URL nor API_KEYS set — both should appear in the error.
	cfg, err := config.Load()
	require.NoError(t, err)

	err = cfg.Validate()
	require.Error(t, err)

	var ve *config.ValidationErrors
	require.ErrorAs(t, err, &ve)
	assert.GreaterOrEqual(t, len(ve.Errors), 2)

	combined := err.Error()
	assert.Contains(t, combined, "REDIS_URL")
	assert.Contains(t, combined, "API_KEYS")
}

func TestValidate_InvalidLogLevel(t *testing.T) {
	setEnv(t, map[string]string{
		"REDIS_URL": "redis://localhost:6379",
		"API_KEYS":  "key1",
		"LOG_LEVEL": "verbose",
	})
	cfg, err := config.Load()
	require.NoError(t, err)

	err = cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "LOG_LEVEL")
}

func TestValidate_InvalidJitterFactor(t *testing.T) {
	setEnv(t, map[string]string{
		"REDIS_URL":           "redis://localhost:6379",
		"API_KEYS":            "key1",
		"RETRY_JITTER_FACTOR": "1.5",
	})
	cfg, err := config.Load()
	require.NoError(t, err)

	err = cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RETRY_JITTER_FACTOR")
}

func TestValidate_ZeroWorkerPoolSize(t *testing.T) {
	setEnv(t, map[string]string{
		"REDIS_URL":        "redis://localhost:6379",
		"API_KEYS":         "key1",
		"WORKER_POOL_SIZE": "0",
	})
	cfg, err := config.Load()
	require.NoError(t, err)

	err = cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "WORKER_POOL_SIZE")
}

// ---------------------------------------------------------------------------
// parseAPIKeys edge cases
// ---------------------------------------------------------------------------

func TestLoad_APIKeys_CommaSeparated(t *testing.T) {
	setEnv(t, map[string]string{"API_KEYS": "  key1 , key2,key3  "})
	cfg, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, []string{"key1", "key2", "key3"}, cfg.Server.APIKeys)
}

func TestLoad_APIKeys_SingleKey(t *testing.T) {
	setEnv(t, map[string]string{"API_KEYS": "only-key"})
	cfg, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, []string{"only-key"}, cfg.Server.APIKeys)
}

// ---------------------------------------------------------------------------
// parseDuration — malformed value falls back to default
// ---------------------------------------------------------------------------

func TestLoad_MalformedDuration_FallsBackToDefault(t *testing.T) {
	setEnv(t, map[string]string{"REDIS_DIAL_TIMEOUT": "not-a-duration"})
	cfg, err := config.Load()
	require.NoError(t, err)
	// Should silently fall back to the 5s default.
	assert.Equal(t, 5*time.Second, cfg.Redis.DialTimeout)
}

// ---------------------------------------------------------------------------
// ValidationErrors.Error() formatting
// ---------------------------------------------------------------------------

func TestValidationErrors_ErrorFormatting(t *testing.T) {
	cfg, err := config.Load()
	require.NoError(t, err)

	err = cfg.Validate()
	require.Error(t, err)

	msg := err.Error()
	assert.True(t, strings.HasPrefix(msg, "config validation failed"), "should start with summary")
	assert.Contains(t, msg, "\n  - ", "each error should be on its own line")
}
