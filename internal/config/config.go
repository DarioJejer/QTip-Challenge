// Package config loads and validates application configuration from environment
// variables. It has zero external dependencies — only the Go standard library
// is used. Every field has a sensible default so the service starts correctly
// against a local docker-compose stack with no additional env configuration.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds the complete runtime configuration of the email queue service.
// Fields are grouped by subsystem. All durations are stored as time.Duration
// so callers never need to multiply by time.Second.
type Config struct {
	Redis         RedisConfig
	Worker        WorkerConfig
	Retry         RetryConfig
	Email         EmailConfig
	Observability ObservabilityConfig
	Server        ServerConfig
}

// RedisConfig controls the go-redis client connection pool (ADR-001, ADR-006).
type RedisConfig struct {
	// URL is the Redis server address, e.g. "redis://localhost:6379".
	// Env: REDIS_URL — required.
	URL string

	// Password is the Redis AUTH password. Empty string disables AUTH.
	// Env: REDIS_PASSWORD — optional.
	Password string

	// DB is the logical Redis database index (0–15).
	// Env: REDIS_DB — default 0.
	DB int

	// PoolSize is the maximum number of socket connections in the pool.
	// Rule of thumb: WORKER_POOL_SIZE * 1.5 (ADR-006).
	// Env: REDIS_POOL_SIZE — default 75.
	PoolSize int

	// MinIdleConns is the minimum number of idle connections kept open.
	// Env: REDIS_MIN_IDLE_CONNS — default 10.
	MinIdleConns int

	// DialTimeout is the timeout for establishing new connections.
	// Env: REDIS_DIAL_TIMEOUT — default 5s.
	DialTimeout time.Duration

	// ReadTimeout is the per-command read deadline.
	// Env: REDIS_READ_TIMEOUT — default 3s.
	ReadTimeout time.Duration

	// WriteTimeout is the per-command write deadline.
	// Env: REDIS_WRITE_TIMEOUT — default 3s.
	WriteTimeout time.Duration
}

// WorkerConfig controls the worker pool supervisor (ADR-004).
type WorkerConfig struct {
	// PoolSize is the maximum number of concurrent worker goroutines.
	// Semaphore size = PoolSize (ADR-004).
	// Env: WORKER_POOL_SIZE — default 50.
	PoolSize int

	// ConsumerGroup is the Redis Streams consumer group name.
	// Env: WORKER_CONSUMER_GROUP — default "email-workers".
	ConsumerGroup string

	// ConsumerName uniquely identifies this pod/process within the group.
	// Env: WORKER_CONSUMER_NAME — default "<hostname>:<pid>".
	ConsumerName string

	// BlockTimeout is the XREADGROUP BLOCK duration. A value of 0 blocks
	// indefinitely (not recommended). Default 5s allows context cancellation
	// to be detected promptly.
	// Env: WORKER_BLOCK_TIMEOUT — default 5s.
	BlockTimeout time.Duration

	// ClaimIdleThreshold is the minimum PEL idle time before a message is
	// reclaimed by ClaimStale (XAUTOCLAIM min-idle-time — ADR-004).
	// Env: WORKER_CLAIM_IDLE_THRESHOLD — default 30s.
	ClaimIdleThreshold time.Duration

	// DrainTimeout is the maximum time the supervisor waits for in-flight
	// tasks to complete after receiving SIGTERM (ADR-008).
	// Env: DRAIN_TIMEOUT — default 30s.
	DrainTimeout time.Duration
}

// RetryConfig controls the delayed-retry scheduler (ADR-005).
type RetryConfig struct {
	// BaseDelay is the initial back-off delay before the first retry.
	// Env: RETRY_BASE_DELAY — default 1s.
	BaseDelay time.Duration

	// MaxDelay caps the exponential back-off growth.
	// Env: RETRY_MAX_DELAY — default 15m.
	MaxDelay time.Duration

	// JitterFactor (0–1) is the fraction of the computed delay added as random
	// noise to prevent retry storms (ADR-005).
	// Env: RETRY_JITTER_FACTOR — default 0.2.
	JitterFactor float64

	// SchedulerInterval is how often the DelayedScheduler polls the sorted set.
	// Env: RETRY_SCHEDULER_INTERVAL — default 1s.
	SchedulerInterval time.Duration

	// DLQTTLSeconds is the Redis EXPIRE TTL set on every DLQ list and
	// idempotency key (in seconds).
	// Env: DLQ_TTL_SECONDS — default 604800 (7 days).
	DLQTTLSeconds int

	// DLQMonitorInterval is how often the DLQMonitor scrapes known DLQ depths.
	// Env: DLQ_MONITOR_INTERVAL — default 30s.
	DLQMonitorInterval time.Duration

	// DLQAlertThreshold is the LLEN above which the DLQMonitor logs a warning.
	// Env: DLQ_ALERT_THRESHOLD — default 100.
	DLQAlertThreshold int
}

// EmailConfig controls the email delivery adapter (M3-07).
// When SendGridAPIKey is empty the service uses RealisticStubSender for local dev.
type EmailConfig struct {
	// SendGridAPIKey is the SendGrid API key. Empty selects the stub sender.
	// Env: SENDGRID_API_KEY — default "".
	SendGridAPIKey string

	// FromEmail is the sender address for SendGrid mail/send requests.
	// Required when SendGridAPIKey is set.
	// Env: EMAIL_FROM — default "noreply@example.com".
	FromEmail string

	// FromName is the optional display name paired with FromEmail.
	// Env: EMAIL_FROM_NAME — default "Email Queue".
	FromName string

	// StubFailRate is the simulated failure probability [0,1] for the stub sender.
	// Env: STUB_EMAIL_FAIL_RATE — default 0.
	StubFailRate float64

	// StubLatency is the base simulated send latency for the stub sender.
	// Env: STUB_EMAIL_LATENCY — default 0.
	StubLatency time.Duration
}

// ObservabilityConfig controls logging, metrics, and distributed tracing
// (ADR-007).
type ObservabilityConfig struct {
	// LogLevel controls the minimum log level: debug, info, warn, error.
	// Env: LOG_LEVEL — default "info".
	LogLevel string

	// LogFormat is either "json" (production) or "console" (local dev).
	// Env: LOG_FORMAT — default "json".
	LogFormat string

	// OTELEndpoint is the OTLP gRPC endpoint for the OTel collector/Jaeger.
	// Empty string disables tracing (noop TracerProvider).
	// Env: OTEL_EXPORTER_OTLP_ENDPOINT — default "".
	OTELEndpoint string

	// ServiceName is the OTel service.name resource attribute.
	// Env: OTEL_SERVICE_NAME — default "email-queue".
	ServiceName string

	// MetricsPort is the port the Prometheus /metrics server listens on.
	// Env: METRICS_PORT — default 9090.
	MetricsPort int
}

// ServerConfig controls the public HTTP API server (ADR-008).
type ServerConfig struct {
	// HTTPPort is the port the chi router listens on.
	// Env: HTTP_PORT — default 8080.
	HTTPPort int

	// APIKeys is the list of valid X-API-Key header values. At least one key
	// is required. Loaded from a comma-separated env var.
	// Env: API_KEYS — required (e.g. "key1,key2").
	APIKeys []string

	// ReadTimeout is the net/http server ReadTimeout.
	// Env: HTTP_READ_TIMEOUT — default 10s.
	ReadTimeout time.Duration

	// WriteTimeout is the net/http server WriteTimeout.
	// Env: HTTP_WRITE_TIMEOUT — default 30s.
	WriteTimeout time.Duration
}

// Load reads every configuration value from the environment and applies
// defaults for values that are not set. It returns a populated *Config.
// Call Validate() on the result to check for missing required values.
func Load() (*Config, error) {
	consumerName, err := defaultConsumerName()
	if err != nil {
		return nil, fmt.Errorf("config: resolve consumer name: %w", err)
	}

	cfg := &Config{
		Redis: RedisConfig{
			URL:          getEnv("REDIS_URL", ""),
			Password:     getEnv("REDIS_PASSWORD", ""),
			DB:           getEnvInt("REDIS_DB", 0),
			PoolSize:     getEnvInt("REDIS_POOL_SIZE", 75),
			MinIdleConns: getEnvInt("REDIS_MIN_IDLE_CONNS", 10),
			DialTimeout:  parseDuration("REDIS_DIAL_TIMEOUT", 5*time.Second),
			ReadTimeout:  parseDuration("REDIS_READ_TIMEOUT", 3*time.Second),
			WriteTimeout: parseDuration("REDIS_WRITE_TIMEOUT", 3*time.Second),
		},
		Worker: WorkerConfig{
			PoolSize:           getEnvInt("WORKER_POOL_SIZE", 50),
			ConsumerGroup:      getEnv("WORKER_CONSUMER_GROUP", "email-workers"),
			ConsumerName:       getEnv("WORKER_CONSUMER_NAME", consumerName),
			BlockTimeout:       parseDuration("WORKER_BLOCK_TIMEOUT", 5*time.Second),
			ClaimIdleThreshold: parseDuration("WORKER_CLAIM_IDLE_THRESHOLD", 30*time.Second),
			DrainTimeout:       parseDuration("DRAIN_TIMEOUT", 30*time.Second),
		},
		Retry: RetryConfig{
			BaseDelay:         parseDuration("RETRY_BASE_DELAY", 1*time.Second),
			MaxDelay:          parseDuration("RETRY_MAX_DELAY", 15*time.Minute),
			JitterFactor:      getEnvFloat("RETRY_JITTER_FACTOR", 0.2),
			SchedulerInterval: parseDuration("RETRY_SCHEDULER_INTERVAL", 1*time.Second),
			DLQTTLSeconds:       getEnvInt("DLQ_TTL_SECONDS", 604800),
			DLQMonitorInterval:  parseDuration("DLQ_MONITOR_INTERVAL", 30*time.Second),
			DLQAlertThreshold:   getEnvInt("DLQ_ALERT_THRESHOLD", 100),
		},
		Email: EmailConfig{
			SendGridAPIKey: getEnv("SENDGRID_API_KEY", ""),
			FromEmail:      getEnv("EMAIL_FROM", "noreply@example.com"),
			FromName:       getEnv("EMAIL_FROM_NAME", "Email Queue"),
			StubFailRate:   getEnvFloat("STUB_EMAIL_FAIL_RATE", 0),
			StubLatency:    parseDuration("STUB_EMAIL_LATENCY", 0),
		},
		Observability: ObservabilityConfig{
			LogLevel:     getEnv("LOG_LEVEL", "info"),
			LogFormat:    getEnv("LOG_FORMAT", "json"),
			OTELEndpoint: getEnv("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
			ServiceName:  getEnv("OTEL_SERVICE_NAME", "email-queue"),
			MetricsPort:  getEnvInt("METRICS_PORT", 9090),
		},
		Server: ServerConfig{
			HTTPPort:     getEnvInt("HTTP_PORT", 8080),
			APIKeys:      parseAPIKeys(getEnv("API_KEYS", "")),
			ReadTimeout:  parseDuration("HTTP_READ_TIMEOUT", 10*time.Second),
			WriteTimeout: parseDuration("HTTP_WRITE_TIMEOUT", 30*time.Second),
		},
	}
	return cfg, nil
}

// Validate checks all required fields and value constraints. It collects every
// violation and returns them together as a single error so operators can fix
// all problems in one restart cycle rather than discovering them one by one.
func (c *Config) Validate() error {
	var errs []string

	if c.Redis.URL == "" {
		errs = append(errs, "REDIS_URL is required")
	}
	if len(c.Server.APIKeys) == 0 {
		errs = append(errs, "API_KEYS is required (comma-separated list of valid API keys)")
	}
	if c.Worker.PoolSize <= 0 {
		errs = append(errs, fmt.Sprintf("WORKER_POOL_SIZE must be > 0, got %d", c.Worker.PoolSize))
	}
	if c.Redis.PoolSize <= 0 {
		errs = append(errs, fmt.Sprintf("REDIS_POOL_SIZE must be > 0, got %d", c.Redis.PoolSize))
	}
	if c.Retry.JitterFactor < 0 || c.Retry.JitterFactor > 1 {
		errs = append(errs, fmt.Sprintf("RETRY_JITTER_FACTOR must be in [0, 1], got %.2f", c.Retry.JitterFactor))
	}
	if c.Retry.BaseDelay <= 0 {
		errs = append(errs, "RETRY_BASE_DELAY must be > 0")
	}
	if c.Retry.MaxDelay < c.Retry.BaseDelay {
		errs = append(errs, "RETRY_MAX_DELAY must be >= RETRY_BASE_DELAY")
	}
	if c.Retry.DLQTTLSeconds <= 0 {
		errs = append(errs, "DLQ_TTL_SECONDS must be > 0")
	}
	if c.Worker.DrainTimeout <= 0 {
		errs = append(errs, "DRAIN_TIMEOUT must be > 0")
	}
	if c.Observability.MetricsPort <= 0 || c.Observability.MetricsPort > 65535 {
		errs = append(errs, fmt.Sprintf("METRICS_PORT must be 1–65535, got %d", c.Observability.MetricsPort))
	}
	if c.Server.HTTPPort <= 0 || c.Server.HTTPPort > 65535 {
		errs = append(errs, fmt.Sprintf("HTTP_PORT must be 1–65535, got %d", c.Server.HTTPPort))
	}
	if c.Email.SendGridAPIKey != "" && c.Email.FromEmail == "" {
		errs = append(errs, "EMAIL_FROM is required when SENDGRID_API_KEY is set")
	}
	if c.Email.StubFailRate < 0 || c.Email.StubFailRate > 1 {
		errs = append(errs, fmt.Sprintf("STUB_EMAIL_FAIL_RATE must be in [0, 1], got %.2f", c.Email.StubFailRate))
	}

	logLevel := strings.ToLower(c.Observability.LogLevel)
	switch logLevel {
	case "debug", "info", "warn", "error":
		// valid
	default:
		errs = append(errs, fmt.Sprintf("LOG_LEVEL must be one of debug|info|warn|error, got %q", c.Observability.LogLevel))
	}

	if len(errs) == 0 {
		return nil
	}
	return &ValidationErrors{Errors: errs}
}

// ValidationErrors is returned by Validate when one or more constraints are
// violated. It implements the error interface and prints all violations.
type ValidationErrors struct {
	Errors []string
}

func (e *ValidationErrors) Error() string {
	return fmt.Sprintf("config validation failed (%d error(s)):\n  - %s",
		len(e.Errors), strings.Join(e.Errors, "\n  - "))
}

// Unwrap returns nil; use errors.As to inspect the ValidationErrors directly.
func (e *ValidationErrors) Unwrap() error { return nil }

// Is satisfies errors.Is for target matching.
func (e *ValidationErrors) Is(target error) bool {
	var t *ValidationErrors
	return errors.As(target, &t)
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	s := os.Getenv(key)
	if s == "" {
		return defaultVal
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return defaultVal
	}
	return v
}

func getEnvFloat(key string, defaultVal float64) float64 {
	s := os.Getenv(key)
	if s == "" {
		return defaultVal
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return defaultVal
	}
	return v
}

// parseDuration reads a duration string from the environment (e.g. "30s",
// "5m"). If the variable is unset or cannot be parsed, defaultVal is returned.
func parseDuration(envKey string, defaultVal time.Duration) time.Duration {
	s := os.Getenv(envKey)
	if s == "" {
		return defaultVal
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		// Return default on parse failure; the caller validates the range.
		return defaultVal
	}
	return d
}

// parseAPIKeys splits a comma-separated string into a slice of non-empty keys.
func parseAPIKeys(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if k := strings.TrimSpace(p); k != "" {
			out = append(out, k)
		}
	}
	return out
}

// defaultConsumerName builds a unique consumer identifier from the OS hostname
// and current process ID. This ensures each pod in a Kubernetes Deployment
// uses a distinct consumer name within the Redis Streams consumer group
// (ADR-008).
func defaultConsumerName() (string, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", hostname, os.Getpid()), nil
}
