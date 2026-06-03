package domain

import (
	"math"
	"math/rand"
	"time"
)

const (
	defaultBaseDelay    = 1 * time.Second
	defaultMaxDelay     = 15 * time.Minute
	defaultJitterFactor = 0.2
)

// RetryPolicy encodes the exponential back-off configuration for a task type.
// All durations and the jitter factor are configurable so that per-tenant or
// per-task-type overrides can be applied without changing core logic.
type RetryPolicy struct {
	// MaxAttempts is the total number of delivery attempts (including the first)
	// before the task is dead-lettered.
	MaxAttempts int

	// BaseDelay is the delay applied before the first retry (attempt 0).
	BaseDelay time.Duration

	// MaxDelay caps the computed delay, preventing indefinite back-off growth.
	MaxDelay time.Duration

	// JitterFactor (0–1) is the fraction of the computed base delay added as
	// random noise. A value of 0.2 adds up to 20% random jitter, which
	// prevents synchronised retry storms across workers (ADR-005).
	JitterFactor float64
}

// DefaultRetryPolicy returns the baseline retry configuration (ADR-005):
//   - 5 max attempts
//   - 1 s base delay
//   - 15 min max delay
//   - 20% jitter
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:  5,
		BaseDelay:    defaultBaseDelay,
		MaxDelay:     defaultMaxDelay,
		JitterFactor: defaultJitterFactor,
	}
}

// RetryPolicyForType returns the recommended RetryPolicy for a given TaskType.
// The MaxAttempts field is overridden per ADR-005:
//   - security: 7
//   - transactional: 5
//   - marketing: 3
//   - all others: 5
func RetryPolicyForType(tt TaskType) RetryPolicy {
	p := DefaultRetryPolicy()
	p.MaxAttempts = tt.DefaultMaxAttempts()
	return p
}

// ComputeDelay returns the back-off duration for the given attempt number.
//
// Formula (ADR-005):
//
//	base  = BaseDelay * 2^attempt          (exponential growth)
//	base  = min(base, MaxDelay)            (cap)
//	delay = base + rand(0, base * JitterFactor)  (add jitter)
//	delay = min(delay, MaxDelay)           (re-cap after jitter)
//
// Negative attempt values are treated as 0.
func (p RetryPolicy) ComputeDelay(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}

	base := float64(p.BaseDelay) * math.Pow(2, float64(attempt))
	maxF := float64(p.MaxDelay)
	if base > maxF {
		base = maxF
	}

	// rand.Float64 returns [0, 1); non-crypto usage is fine here.
	jitter := rand.Float64() * base * p.JitterFactor //nolint:gosec
	delay := time.Duration(base + jitter)
	if delay > p.MaxDelay {
		delay = p.MaxDelay
	}
	return delay
}
