package ports

import "errors"

// ErrNonRetryable is a sentinel that email adapter implementations wrap
// around permanent delivery failures (e.g. invalid address, 4xx provider
// response, template not found). When the worker detects this error via
// errors.Is, it skips the retry queue and routes the task directly to the DLQ
// regardless of remaining attempt budget (ADR-005, M3-10).
var ErrNonRetryable = errors.New("non-retryable delivery failure")

// ErrCircuitOpen is returned by an EmailSender implementation when its
// circuit breaker is open due to a sustained provider outage. The worker
// treats this the same as ErrNonRetryable — dead-letter immediately — to
// avoid accumulating retry pressure during an outage (ADR-005).
var ErrCircuitOpen = errors.New("circuit breaker open")
