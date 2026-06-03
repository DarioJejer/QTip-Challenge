package ports

import (
	"context"

	"github.com/DarioJejer/go-email-queue/internal/domain"
)

// EmailSender abstracts the external email delivery provider (SendGrid, SES,
// etc.). The worker calls Send for each task; the concrete implementation
// translates the domain task into the provider's API request.
//
// Error classification (ADR-005, M3-10):
//   - Retryable errors: transient network/5xx failures — worker will retry.
//   - Non-retryable errors (wrapping ErrNonRetryable): permanent failures
//     (invalid address, template not found, 4xx) — worker routes to DLQ.
//   - ErrCircuitOpen: circuit breaker is open — treated as non-retryable to
//     avoid accumulating retries during a provider outage.
type EmailSender interface {
	// Send delivers a single email described by task. The implementation must
	// respect ctx cancellation and return ctx.Err() promptly on cancellation
	// so that graceful shutdown drain completes within the budget.
	//
	// On permanent provider failure the implementation wraps the underlying
	// error with ErrNonRetryable so the worker can route the task to the DLQ
	// without consuming retry attempts.
	Send(ctx context.Context, task *domain.EmailTask) error

	// SendBatch delivers multiple tasks in a single provider API call where
	// supported. It returns a *domain.MultiError listing every failed task ID.
	// Successfully delivered tasks within the batch are not rolled back.
	SendBatch(ctx context.Context, tasks []*domain.EmailTask) error

	// HealthCheck performs a lightweight connectivity check to the email
	// provider and returns nil if the provider is reachable. This is called
	// by the /healthz handler (ADR-008).
	HealthCheck(ctx context.Context) error
}
