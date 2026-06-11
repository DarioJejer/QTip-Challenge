package worker

import (
	"context"
	"strings"
	"time"

	"github.com/DarioJejer/go-email-queue/internal/config"
	"github.com/DarioJejer/go-email-queue/internal/observability"
	"github.com/DarioJejer/go-email-queue/internal/ports"
)

// DLQMonitor periodically scrapes LLEN for configured DLQ keys and updates
// the email_queue_dlq_depth Prometheus gauge (ADR-005, ADR-007).
type DLQMonitor struct {
	cfg          *config.Config
	dlqWriter    ports.DLQWriter
	metrics      ports.MetricsRecorder
	knownDLQKeys []string
}

// NewDLQMonitor constructs a DLQMonitor. knownDLQKeys must be full Redis keys
// in the form queue:dlq:{tenantID}:{taskType}. An empty slice is valid — the
// monitor ticks but performs no scrapes until keys are configured.
func NewDLQMonitor(
	cfg *config.Config,
	dlqWriter ports.DLQWriter,
	metrics ports.MetricsRecorder,
	knownDLQKeys []string,
) *DLQMonitor {
	return &DLQMonitor{
		cfg:          cfg,
		dlqWriter:    dlqWriter,
		metrics:      metrics,
		knownDLQKeys: knownDLQKeys,
	}
}

// Run starts the scrape ticker and blocks until ctx is cancelled, then returns
// nil. Wire it into an errgroup goroutine alongside the HTTP server (ADR-008).
func (m *DLQMonitor) Run(ctx context.Context) error {
	logger := observability.LoggerFromContext(ctx)
	interval := m.cfg.Retry.DLQMonitorInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	threshold := int64(m.cfg.Retry.DLQAlertThreshold)
	if threshold <= 0 {
		threshold = 100
	}

	logger.Info().
		Dur("interval", interval).
		Int64("alert_threshold", threshold).
		Int("known_keys", len(m.knownDLQKeys)).
		Msg("dlq monitor starting")

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.scrape(ctx, threshold)
		case <-ctx.Done():
			logger.Info().Msg("dlq monitor stopping")
			return nil
		}
	}
}

func (m *DLQMonitor) scrape(ctx context.Context, threshold int64) {
	logger := observability.LoggerFromContext(ctx)
	for _, key := range m.knownDLQKeys {
		tenantID, taskType, ok := parseDLQKey(key)
		if !ok {
			logger.Warn().Str("key", key).Msg("dlq monitor: skipping malformed DLQ key")
			continue
		}

		depth, err := m.dlqWriter.DLQDepth(ctx, tenantID, taskType)
		if err != nil {
			logger.Error().Err(err).Str("key", key).Msg("dlq monitor: DLQDepth error")
			continue
		}

		m.metrics.RecordDLQDepth(tenantID, taskType, float64(depth))
		if depth > threshold {
			logger.Warn().
				Str("key", key).
				Str("tenant_id", tenantID).
				Str("task_type", taskType).
				Int64("depth", depth).
				Int64("threshold", threshold).
				Msg("dlq.depth_threshold_exceeded")
		}
	}
}

// parseDLQKey extracts tenantID and taskType from queue:dlq:{tenantID}:{taskType}.
func parseDLQKey(key string) (tenantID, taskType string, ok bool) {
	const prefix = "queue:dlq:"
	if !strings.HasPrefix(key, prefix) {
		return "", "", false
	}
	rest := key[len(prefix):]
	idx := strings.Index(rest, ":")
	if idx <= 0 || idx >= len(rest)-1 {
		return "", "", false
	}
	return rest[:idx], rest[idx+1:], true
}
