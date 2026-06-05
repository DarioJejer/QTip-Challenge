package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
	_ "go.uber.org/automaxprocs" // ADR-008: GOMAXPROCS = container CPU limit

	httpadapter "github.com/DarioJejer/go-email-queue/internal/adapters/http"
	redisadapter "github.com/DarioJejer/go-email-queue/internal/adapters/redis"
	"github.com/DarioJejer/go-email-queue/internal/adapters/stubs"
	"github.com/DarioJejer/go-email-queue/internal/config"
)

// version is injected at build time:
//
//	go build -ldflags "-X main.version=$(git describe --tags --always)"
var version = "dev"

func main() {
	// -------------------------------------------------------------------------
	// Step 1: Load and validate configuration.
	// All required env vars are checked here; missing values cause a fatal exit
	// so misconfigurations are caught at startup, not at runtime (ADR-008).
	// -------------------------------------------------------------------------
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, `{"level":"fatal","msg":"config load failed","error":%q}`+"\n", err.Error())
		os.Exit(1)
	}
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, `{"level":"fatal","msg":"config validation failed","error":%q}`+"\n", err.Error())
		os.Exit(1)
	}

	// -------------------------------------------------------------------------
	// Step 2: Initialise structured logging.
	// TODO(M2-08): replace initLogger with observability.Init(ctx, cfg) which
	//              additionally wires the OTel TracerProvider + MeterProvider
	//              and returns a shutdown flush function.
	// -------------------------------------------------------------------------
	initLogger(cfg.Observability.LogLevel, cfg.Observability.LogFormat)

	log.Info().
		Str("version", version).
		Str("service", cfg.Observability.ServiceName).
		Int("http_port", cfg.Server.HTTPPort).
		Int("metrics_port", cfg.Observability.MetricsPort).
		Int("worker_pool_size", cfg.Worker.PoolSize).
		Msg("starting email-queue service")

	// Prometheus registry — injected explicitly into every component that
	// registers metrics. Never use prometheus.DefaultRegisterer in application
	// code; the default registry is reserved for Go runtime / process metrics
	// only (see registerPoolMetrics for the full rationale).
	// TODO(M2-08): move registry construction into observability.Init.
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		prometheus.NewGoCollector(),
		prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}),
	)

	// Root context: cancelled on SIGTERM or SIGINT. stop() must be deferred so
	// the signal handler goroutine is cleaned up on exit (ADR-008).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// -------------------------------------------------------------------------
	// Step 3: Connect to Redis and verify connectivity.
	// Fatal on failure — the service has no meaningful behaviour without Redis.
	// -------------------------------------------------------------------------
	redisClient, err := redisadapter.NewRedisClient(cfg, reg)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to connect to Redis")
	}
	redisLifecycle := redisadapter.NewRedisClientLifecycle(redisClient)
	log.Info().Str("addr", cfg.Redis.URL).Msg("Redis connection established")

	// -------------------------------------------------------------------------
	// Step 4: Load Lua scripts into the Redis script cache.
	// Scripts must be loaded before workers start so EVALSHA calls don't fail
	// with NOSCRIPT errors on the first task (ADR-006).
	// -------------------------------------------------------------------------
	scripts, err := redisadapter.LoadScripts(ctx, redisClient)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load Redis Lua scripts")
	}
	log.Info().Str("idempotency_sha", scripts.IdempotencySHA).Msg("Lua scripts loaded")

	// -------------------------------------------------------------------------
	// Step 5: Construct port implementations.
	// All adapters are stubs in M2. Each TODO comment names the M3 issue that
	// replaces it with the real implementation. The variable names match what
	// the worker supervisor and HTTP handlers expect so that swapping in real
	// adapters in M3 is a one-line change per component.
	// -------------------------------------------------------------------------
	producer := stubs.NewStubProducer()
	// TODO(M3-01): producer = redisadapter.NewRedisProducer(redisClient, cfg, metricsRecorder, tracer)

	consumer := stubs.NewStubConsumer()
	// TODO(M3-02): consumer = redisadapter.NewRedisConsumer(redisClient, cfg, metricsRecorder, tracer)

	retryScheduler := stubs.NewStubRetryScheduler()
	// TODO(M3-04): retryScheduler = redisadapter.NewRedisRetryScheduler(redisClient, producer)

	dlqWriter := stubs.NewStubDLQWriter()
	// TODO(M3-06): dlqWriter = redisadapter.NewRedisDLQWriter(redisClient, cfg, metricsRecorder)

	idempotencyStore := stubs.NewStubIdempotencyStore()
	// TODO(M3-05): idempotencyStore = redisadapter.NewRedisIdempotencyStore(redisClient, scripts, cfg)

	emailSender := stubs.NewStubEmailSender()
	// TODO(M3-07): emailSender = email.NewStubSender(0, 10*time.Millisecond)  // local dev
	//              emailSender = email.NewSendGridSender(cfg.SendGridAPIKey, tracer, metricsRecorder) // prod

	metricsRecorder := stubs.NewStubMetricsRecorder()
	// TODO(M2-08): metricsRecorder = observability.NewPrometheusRecorder(reg)

	// Suppress "declared and not used" errors for variables consumed in M3.
	_ = consumer
	_ = retryScheduler
	_ = dlqWriter
	_ = idempotencyStore
	_ = emailSender
	_ = metricsRecorder

	// -------------------------------------------------------------------------
	// Step 6: Construct application-layer components.
	// Worker, scheduler, and DLQ monitor are implemented in M3. Their errgroup
	// goroutines below are no-op stubs that honour context cancellation, keeping
	// the shutdown sequence correct even before real logic is wired.
	// TODO(M3-03): supervisor = worker.NewSupervisor(cfg, consumer, emailSender,
	//                               idempotencyStore, retryScheduler, dlqWriter,
	//                               metricsRecorder, tracer)
	// TODO(M3-04): delayedSched = worker.NewDelayedScheduler(cfg, retryScheduler, metricsRecorder)
	// TODO(M3-06): dlqMonitor   = worker.NewDLQMonitor(cfg, dlqWriter, metricsRecorder, []string{})
	// -------------------------------------------------------------------------

	// -------------------------------------------------------------------------
	// Step 7: Construct the HTTP router.
	// -------------------------------------------------------------------------
	router := httpadapter.NewRouter(cfg, producer)

	// -------------------------------------------------------------------------
	// Step 8: Launch all goroutines via errgroup.
	// No raw go func() calls anywhere in main — every goroutine is supervised
	// by errgroup so panics and errors are propagated and handled uniformly
	// (ADR-004 structured concurrency principle applied to the bootstrap layer).
	// -------------------------------------------------------------------------
	g, gCtx := errgroup.WithContext(ctx)

	// --- HTTP API server ---
	httpServer := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Server.HTTPPort),
		Handler:      router.Handler(),
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
	}
	g.Go(func() error {
		log.Info().Int("port", cfg.Server.HTTPPort).Msg("HTTP server listening")
		router.SetReady(true)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("http server: %w", err)
		}
		return nil
	})

	// --- Prometheus metrics server ---
	// Served on a separate port so network policy can restrict /metrics to
	// the Prometheus scraper without exposing it to public traffic (ADR-007).
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	}))
	metricsServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Observability.MetricsPort),
		Handler: metricsMux,
	}
	g.Go(func() error {
		log.Info().Int("port", cfg.Observability.MetricsPort).Msg("metrics server listening")
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("metrics server: %w", err)
		}
		return nil
	})

	// --- Worker supervisor (stub) ---
	// TODO(M3-03): replace body with supervisor.Run(gCtx)
	g.Go(func() error {
		log.Info().Int("pool_size", cfg.Worker.PoolSize).Msg("worker supervisor started (stub)")
		<-gCtx.Done()
		log.Info().Msg("worker supervisor stopped")
		return nil
	})

	// --- Delayed retry scheduler (stub) ---
	// TODO(M3-04): replace body with delayedSched.Run(gCtx)
	g.Go(func() error {
		log.Info().Dur("interval", cfg.Retry.SchedulerInterval).Msg("delayed scheduler started (stub)")
		<-gCtx.Done()
		log.Info().Msg("delayed scheduler stopped")
		return nil
	})

	// --- DLQ monitor (stub) ---
	// TODO(M3-06): replace body with dlqMonitor.Run(gCtx)
	g.Go(func() error {
		log.Info().Msg("DLQ monitor started (stub)")
		<-gCtx.Done()
		log.Info().Msg("DLQ monitor stopped")
		return nil
	})

	// --- Shutdown coordinator ---
	// Follows the ADR-008 SIGTERM drain sequence step-by-step:
	//
	//   T+ 0s  SIGTERM received → SetReady(false) → /readyz returns 503
	//          → load balancer drains existing connections
	//   T+ 0s  httpServer.Shutdown(drainCtx) — stops accepting new requests,
	//          waits for in-flight requests to complete
	//   T+30s  drain timeout → force-close remaining connections
	//   T+30s  redisLifecycle.Close() — log pool stats, close all connections
	//   T+30s  OTel flush (TODO M2-08)
	//
	// terminationGracePeriodSeconds=60 in the pod spec gives a 30s buffer
	// between drain completion and SIGKILL (ADR-008).
	g.Go(func() error {
		<-gCtx.Done()
		log.Info().Msg("shutdown signal received — beginning graceful drain")

		// Step 1: flip readiness probe so the load balancer stops routing traffic.
		router.SetReady(false)

		// Step 2: drain HTTP servers within the configured budget.
		drainCtx, drainCancel := context.WithTimeout(context.Background(), cfg.Worker.DrainTimeout)
		defer drainCancel()

		if err := httpServer.Shutdown(drainCtx); err != nil {
			log.Warn().Err(err).Msg("HTTP server did not drain cleanly")
		} else {
			log.Info().Msg("HTTP server drained")
		}

		if err := metricsServer.Shutdown(drainCtx); err != nil {
			log.Warn().Err(err).Msg("metrics server did not drain cleanly")
		} else {
			log.Info().Msg("metrics server drained")
		}

		// Step 3: close Redis after all workers have stopped.
		if err := redisLifecycle.Close(); err != nil {
			log.Warn().Err(err).Msg("Redis close error")
		}

		// TODO(M2-08): shutdownObs(drainCtx) — flush OTel spans and metrics

		log.Info().Msg("graceful shutdown complete")
		return nil
	})

	// Block until all goroutines exit.
	if err := g.Wait(); err != nil {
		log.Error().Err(err).Msg("service exited with error")
		os.Exit(1)
	}
	log.Info().Msg("service stopped cleanly")
}

// initLogger configures the global zerolog logger.
// JSON is the default (zerolog zero-allocation path, production-ready).
// Console mode enables coloured human-readable output for local development.
//
// TODO(M2-08): replace with observability.NewLogger(cfg) which also attaches
// the OTel trace/span IDs to every log line via a zerolog hook.
func initLogger(level, format string) {
	var lvl zerolog.Level
	switch level {
	case "debug":
		lvl = zerolog.DebugLevel
	case "warn":
		lvl = zerolog.WarnLevel
	case "error":
		lvl = zerolog.ErrorLevel
	default:
		lvl = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(lvl)

	if format == "console" {
		log.Logger = log.Output(zerolog.NewConsoleWriter())
	}
	// JSON is zerolog's default output format — no-op needed for production.
}
