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
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"golang.org/x/sync/errgroup"
	_ "go.uber.org/automaxprocs" // ADR-008: GOMAXPROCS = container CPU limit

	emailadapter "github.com/DarioJejer/go-email-queue/internal/adapters/email"
	httpadapter "github.com/DarioJejer/go-email-queue/internal/adapters/http"
	redisadapter "github.com/DarioJejer/go-email-queue/internal/adapters/redis"
	"github.com/DarioJejer/go-email-queue/internal/ports"
	"github.com/DarioJejer/go-email-queue/internal/config"
	"github.com/DarioJejer/go-email-queue/internal/observability"
	"github.com/DarioJejer/go-email-queue/internal/worker"
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
	// -------------------------------------------------------------------------
	observability.Version = version
	log.Logger = observability.NewLogger(cfg.Observability.LogLevel, cfg.Observability.LogFormat)

	log.Info().
		Str("version", version).
		Str("service", cfg.Observability.ServiceName).
		Int("http_port", cfg.Server.HTTPPort).
		Int("metrics_port", cfg.Observability.MetricsPort).
		Int("worker_pool_size", cfg.Worker.PoolSize).
		Msg("starting email-queue service")

	reg := prometheus.NewRegistry()
	reg.MustRegister(
		prometheus.NewGoCollector(),
		prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}),
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// -------------------------------------------------------------------------
	// Step 2b: Initialise OpenTelemetry TracerProvider.
	// -------------------------------------------------------------------------
	shutdownObs, err := observability.InitTracer(ctx, cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to initialise OpenTelemetry tracer")
	}
	log.Info().
		Bool("tracing_enabled", cfg.Observability.OTELEndpoint != "").
		Msg("OpenTelemetry tracer initialised")

	tracer := otel.GetTracerProvider().Tracer("email-queue")

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
	// Step 4: Load Lua scripts.
	// -------------------------------------------------------------------------
	scripts, err := redisadapter.LoadScripts(ctx, redisClient)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load Redis Lua scripts")
	}
	log.Info().
		Str("idempotency_sha", scripts.IdempotencySHA).
		Str("reclaim_stale_sha", scripts.ReclaimStaleSHA).
		Msg("Lua scripts loaded")

	// -------------------------------------------------------------------------
	// Step 5: Construct port implementations.
	// -------------------------------------------------------------------------

	metricsRecorder := observability.NewPrometheusRecorder(reg)
	log.Info().Msg("Prometheus metrics recorder initialised")

	// M3-01: Real Redis Streams producer.
	producer := redisadapter.NewRedisProducer(redisClient, cfg, metricsRecorder, tracer)

	// M3-02: Real Redis Streams consumer.
	consumer := redisadapter.NewRedisConsumer(redisClient, cfg, metricsRecorder, tracer)

	// M3-04: Real retry scheduler backed by the retry sorted set.
	retryScheduler := redisadapter.NewRedisRetryScheduler(redisClient, cfg, metricsRecorder)

	// M3-06: Real Redis LIST dead-letter writer.
	dlqWriter := redisadapter.NewRedisDLQWriter(redisClient, cfg, metricsRecorder)

	// M3-05: Real Redis idempotency store backed by Lua CAS scripts.
	idempotencyStore := redisadapter.NewRedisIdempotencyStore(redisClient, scripts, cfg)

	var emailSender ports.EmailSender
	if cfg.Email.SendGridAPIKey != "" {
		emailSender = emailadapter.NewSendGridSender(
			cfg.Email.SendGridAPIKey,
			cfg.Email.FromEmail,
			cfg.Email.FromName,
			tracer,
			metricsRecorder,
		)
		log.Info().Str("from", cfg.Email.FromEmail).Msg("using SendGrid email sender")
	} else {
		emailSender = emailadapter.NewStubSender(cfg.Email.StubFailRate, cfg.Email.StubLatency)
		log.Info().
			Float64("stub_fail_rate", cfg.Email.StubFailRate).
			Dur("stub_latency", cfg.Email.StubLatency).
			Msg("using realistic stub email sender (no SENDGRID_API_KEY)")
	}

	// -------------------------------------------------------------------------
	// Step 6: Construct application-layer components.
	// -------------------------------------------------------------------------

	// M3-03: Real worker supervisor.
	supervisor := worker.NewSupervisor(
		cfg, consumer, emailSender,
		idempotencyStore, retryScheduler, dlqWriter,
		metricsRecorder, tracer,
	)

	// M3-04: Real delayed scheduler flushes the retry sorted set on each tick.
	delayedScheduler := worker.NewDelayedScheduler(cfg, retryScheduler, metricsRecorder)

	// M3-06: DLQ depth monitor — scrapes known DLQ keys every 30s (configurable).
	dlqMonitor := worker.NewDLQMonitor(cfg, dlqWriter, metricsRecorder, []string{})

	// -------------------------------------------------------------------------
	// Step 7: Construct the HTTP router.
	// -------------------------------------------------------------------------
	router := httpadapter.NewRouter(cfg, producer)

	// -------------------------------------------------------------------------
	// Step 8: Launch all goroutines via errgroup.
	// No raw go func() calls anywhere in main — every goroutine is supervised
	// by errgroup so panics and errors are propagated uniformly (ADR-004).
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
	// the Prometheus scraper without exposing it on the public API port.
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

	// --- Worker supervisor (M3-03) ---
	// Run blocks until gCtx is cancelled, then drains in-flight tasks.
	g.Go(func() error {
		return supervisor.Run(gCtx)
	})

	// --- Delayed retry scheduler (M3-04) ---
	// Ticks at cfg.Retry.SchedulerInterval; flushes ready tasks from the retry
	// sorted set into their priority streams.
	g.Go(func() error {
		return delayedScheduler.Run(gCtx)
	})

	// --- DLQ monitor (M3-06) ---
	g.Go(func() error {
		return dlqMonitor.Run(gCtx)
	})

	// --- Shutdown coordinator (ADR-008 SIGTERM drain sequence) ---
	//
	//   T+0s   SIGTERM -> SetReady(false) -> /readyz 503
	//   T+0s   httpServer.Shutdown -> drain in-flight HTTP requests
	//   T+0s   supervisor.Drain -> wait for in-flight email tasks to finish
	//          (workers may still call Redis ACK/NACK during this window)
	//   T+30s  redisLifecycle.Close -> safe now that workers are idle
	//   T+30s  shutdownObs -> flush OTel spans
	g.Go(func() error {
		<-gCtx.Done()
		log.Info().Msg("shutdown signal received -- beginning graceful drain")

		router.SetReady(false)

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

		// Wait for in-flight workers before closing Redis (workers still need it).
		if err := supervisor.Drain(cfg.Worker.DrainTimeout); err != nil {
			log.Warn().Err(err).Msg("supervisor drain timeout")
		} else {
			log.Info().Msg("worker supervisor drained")
		}

		if err := redisLifecycle.Close(); err != nil {
			log.Warn().Err(err).Msg("Redis close error")
		}

		if err := shutdownObs(drainCtx); err != nil {
			log.Warn().Err(err).Msg("OTel shutdown error")
		} else {
			log.Info().Msg("OTel flushed")
		}

		log.Info().Msg("graceful shutdown complete")
		return nil
	})

	if err := g.Wait(); err != nil {
		log.Error().Err(err).Msg("service exited with error")
		os.Exit(1)
	}
	log.Info().Msg("service stopped cleanly")
}
