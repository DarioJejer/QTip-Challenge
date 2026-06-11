package email

import (
	"context"
	"sync"
	"time"

	"github.com/DarioJejer/go-email-queue/internal/observability"
	"github.com/DarioJejer/go-email-queue/internal/ports"
)

// circuitBreaker is a minimal in-process breaker (ADR-005).
// After maxFailures consecutive RecordFailure calls it rejects with
// ports.ErrCircuitOpen until the open window elapses.
type circuitBreaker struct {
	mu            sync.Mutex
	maxFailures   int
	openDuration  time.Duration
	consecutive   int
	openUntil     time.Time
}

func newCircuitBreaker(maxFailures int, openDuration time.Duration) *circuitBreaker {
	return &circuitBreaker{
		maxFailures:  maxFailures,
		openDuration: openDuration,
	}
}

func (b *circuitBreaker) Allow() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.openUntil.IsZero() || time.Now().After(b.openUntil) {
		return nil
	}
	return ports.ErrCircuitOpen
}

func (b *circuitBreaker) RecordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.consecutive = 0
	b.openUntil = time.Time{}
}

func (b *circuitBreaker) RecordFailure(ctx context.Context) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.consecutive++
	if b.consecutive < b.maxFailures {
		return
	}

	b.openUntil = time.Now().Add(b.openDuration)
	b.consecutive = 0
	logger := observability.LoggerFromContext(ctx)
	logger.Error().
		Int("max_failures", b.maxFailures).
		Dur("open_duration", b.openDuration).
		Msg("email: circuit breaker opened")
}
