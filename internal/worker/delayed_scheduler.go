package worker

import (
	"context"
	"time"

	"github.com/DarioJejer/go-email-queue/internal/config"
	"github.com/DarioJejer/go-email-queue/internal/observability"
	"github.com/DarioJejer/go-email-queue/internal/ports"
)

// DelayedScheduler ticks at cfg.Retry.SchedulerInterval and calls
// retryScheduler.FlushReady on each tick to move retry-ready tasks from the
// sorted set back into their priority Redis Streams (ADR-005).
type DelayedScheduler struct {
	cfg            *config.Config
	retryScheduler ports.RetryScheduler
	metrics        ports.MetricsRecorder
}

// NewDelayedScheduler constructs a DelayedScheduler.
func NewDelayedScheduler(
	cfg *config.Config,
	retryScheduler ports.RetryScheduler,
	metrics ports.MetricsRecorder,
) *DelayedScheduler {
	return &DelayedScheduler{
		cfg:            cfg,
		retryScheduler: retryScheduler,
		metrics:        metrics,
	}
}

// Run starts the flush ticker and blocks until ctx is cancelled, then returns
// nil. Wire it into an errgroup goroutine alongside the HTTP server and worker
// supervisor (ADR-008).
func (d *DelayedScheduler) Run(ctx context.Context) error {
	logger := observability.LoggerFromContext(ctx)
	logger.Info().
		Dur("interval", d.cfg.Retry.SchedulerInterval).
		Msg("delayed scheduler starting")

	ticker := time.NewTicker(d.cfg.Retry.SchedulerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			flushed, err := d.retryScheduler.FlushReady(ctx)
			if err != nil {
				logger.Error().Err(err).Msg("delayed scheduler: FlushReady error")
				continue
			}
			if len(flushed) > 0 {
				logger.Debug().Int("count", len(flushed)).Msg("delayed scheduler: flushed retry tasks")
			}
		case <-ctx.Done():
			logger.Info().Msg("delayed scheduler stopping")
			return nil
		}
	}
}
