package email

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/DarioJejer/go-email-queue/internal/domain"
	"github.com/DarioJejer/go-email-queue/internal/observability"
	"github.com/DarioJejer/go-email-queue/internal/ports"
)

// RealisticStubSender simulates email delivery latency and random failures for
// local development and integration tests (M3-07).
type RealisticStubSender struct {
	failRate float64
	latency  time.Duration
	rng      *rand.Rand
}

// Compile-time interface satisfaction check.
var _ ports.EmailSender = (*RealisticStubSender)(nil)

// NewStubSender returns a stub sender with the given simulated failure rate
// [0,1] and base latency. Jitter up to 50% of latency is added per send.
func NewStubSender(failRate float64, latency time.Duration) *RealisticStubSender {
	return newStubSender(failRate, latency, rand.New(rand.NewSource(time.Now().UnixNano())))
}

// newStubSender constructs a stub with an explicit RNG (tests).
func newStubSender(failRate float64, latency time.Duration, rng *rand.Rand) *RealisticStubSender {
	return &RealisticStubSender{
		failRate: failRate,
		latency:  latency,
		rng:      rng,
	}
}

// Send simulates provider latency and optionally fails at failRate.
func (s *RealisticStubSender) Send(ctx context.Context, task *domain.EmailTask) error {
	start := time.Now()

	jitter := time.Duration(0)
	if s.latency > 0 {
		jitter = time.Duration(s.rng.Int63n(int64(s.latency / 2)))
	}
	if err := sleepWithContext(ctx, s.latency+jitter); err != nil {
		return err
	}

	if s.failRate > 0 && s.rng.Float64() < s.failRate {
		return fmt.Errorf("stub: simulated send failure for %s", task.ID)
	}

	logger := observability.LoggerFromContext(ctx)
	logger.Info().
		Str("event", "email.sent").
		Str("mode", "stub").
		Str("recipient", task.Recipient).
		Str("task_type", string(task.Type)).
		Str("template_id", task.TemplateID).
		Int64("latency_ms", time.Since(start).Milliseconds()).
		Msg("email.sent (stub)")

	return nil
}

// SendBatch delivers each task individually and returns a MultiError on partial failure.
func (s *RealisticStubSender) SendBatch(ctx context.Context, tasks []*domain.EmailTask) error {
	var errs []error
	for _, task := range tasks {
		if err := s.Send(ctx, task); err != nil {
			errs = append(errs, fmt.Errorf("task %s: %w", task.ID, err))
		}
	}
	if len(errs) > 0 {
		return &domain.MultiError{Errors: errs}
	}
	return nil
}

// HealthCheck always succeeds for the stub sender.
func (s *RealisticStubSender) HealthCheck(_ context.Context) error {
	return nil
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
