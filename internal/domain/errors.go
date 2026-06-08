package domain

import (
	"fmt"
	"strings"
)

// MultiError collects multiple errors from a batch operation.
// It is returned by TaskProducer.EnqueueBatch when one or more tasks fail,
// either at validation time or during Redis pipeline execution.
type MultiError struct {
	// Errors is the ordered list of per-task errors. Each entry is wrapped
	// with the failing task ID for easy identification.
	Errors []error
}

// Error implements the error interface. A single error is returned verbatim;
// multiple errors are numbered and joined with "; ".
func (e *MultiError) Error() string {
	if len(e.Errors) == 1 {
		return e.Errors[0].Error()
	}
	msgs := make([]string, len(e.Errors))
	for i, err := range e.Errors {
		msgs[i] = fmt.Sprintf("[%d] %s", i+1, err.Error())
	}
	return fmt.Sprintf("%d errors: %s", len(e.Errors), strings.Join(msgs, "; "))
}

// Unwrap returns the list of wrapped errors to support errors.Is and errors.As
// traversal across the full error set (Go 1.20+ multi-error unwrap).
func (e *MultiError) Unwrap() []error {
	return e.Errors
}
