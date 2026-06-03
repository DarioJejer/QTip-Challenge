package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	_ "go.uber.org/automaxprocs" // ADR-008: sets GOMAXPROCS = container CPU limit
)

// version is injected at build time:
//
//	go build -ldflags "-X main.version=$(git describe --tags --always)"
var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logf("info", "starting", "version", version)

	// -------------------------------------------------------------------------
	// Bootstrap sequence — each step is implemented in a subsequent M2 issue.
	// Wire order follows the DI diagram: docs/diagrams/worker-goroutines.mmd
	// -------------------------------------------------------------------------

	// TODO(M2-04): cfg, err := config.Load()
	//              if err != nil { logf("fatal", err.Error()); os.Exit(1) }

	// TODO(M2-08): shutdownObs, err := observability.Init(ctx, cfg)
	//              (zerolog global logger, OTel TracerProvider, Prometheus registry)
	//              if err != nil { logf("fatal", err.Error()); os.Exit(1) }
	//              defer shutdownObs(ctx)

	// TODO(M2-06): redisClient, err := redisadapter.NewClient(cfg)
	//              if err != nil { logf("fatal", err.Error()); os.Exit(1) }
	//              defer redisClient.Close()

	// TODO(M2-06): if err := redisadapter.LoadScripts(ctx, redisClient); err != nil {
	//                  logf("fatal", err.Error()); os.Exit(1)
	//              }

	// TODO(M2-03): Construct port implementations:
	//              idempotencyStore = redis.NewIdempotencyStore(redisClient, scripts, cfg)
	//              taskProducer     = stubs.NewStubProducer()          // replaced in M3-01
	//              taskConsumer     = stubs.NewStubConsumer()          // replaced in M3-02
	//              retryScheduler   = stubs.NewStubRetryScheduler()    // replaced in M3-04
	//              dlqWriter        = stubs.NewStubDLQWriter()         // replaced in M3-06
	//              emailSender      = email.NewStubSender(0, 0)        // replaced in M3-07
	//              metricsRecorder  = observability.NewPrometheusRecorder(registry)

	// TODO(M2-07): Construct application components:
	//              workerPool       = worker.NewSupervisor(cfg, taskConsumer, emailSender, ...)
	//              delayedScheduler = worker.NewDelayedScheduler(cfg, retryScheduler, ...)
	//              dlqMonitor       = worker.NewDLQMonitor(cfg, dlqWriter, metricsRecorder, ...)
	//              router           = httpAdapter.NewRouter(cfg, taskProducer, ...)

	// TODO(M2-07): Start all goroutines via errgroup:
	//              g, gCtx := errgroup.WithContext(ctx)
	//              g.Go(func() error { return httpServer.ListenAndServe() })
	//              g.Go(func() error { return metricsServer.ListenAndServe() })
	//              g.Go(func() error { return workerPool.Run(gCtx) })
	//              g.Go(func() error { return delayedScheduler.Run(gCtx) })
	//              g.Go(func() error { return dlqMonitor.Run(gCtx) })
	//              if err := g.Wait(); err != nil { logf("error", err.Error()) }

	// Wait for shutdown signal
	<-ctx.Done()
	logf("info", "shutdown signal received")
}

// logf writes a minimal JSON log line to stdout.
// Replaced by zerolog in M2-08 once observability is wired.
func logf(level, msg string, kvs ...string) {
	line := fmt.Sprintf(`{"level":%q,"service":"email-queue","version":%q,"msg":%q`, level, version, msg)
	for i := 0; i+1 < len(kvs); i += 2 {
		line += fmt.Sprintf(`,%q:%q`, kvs[i], kvs[i+1])
	}
	line += "}\n"
	fmt.Fprint(os.Stdout, line)
}
