package http

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/DarioJejer/go-email-queue/internal/domain"
)

// ---------------------------------------------------------------------------
// Health probes (ADR-008)
// ---------------------------------------------------------------------------

// handleHealthz responds with 200 if the service is alive. In M3 this will
// also verify a Redis PING succeeds; in M2 it always returns healthy.
func (r *Router) handleHealthz(w http.ResponseWriter, req *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
		"time":   time.Now().UTC().Format(time.RFC3339),
	})
}

// handleReadyz responds with 200 when the worker pool is active and the
// service is accepting traffic, or 503 during startup and graceful shutdown.
// The supervisor calls SetReady(false) as the first shutdown step (ADR-008).
func (r *Router) handleReadyz(w http.ResponseWriter, req *http.Request) {
	if r.IsReady() {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
		return
	}
	writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready"})
}

// ---------------------------------------------------------------------------
// Task handlers — stubbed in M2, replaced in M3-08
// ---------------------------------------------------------------------------

// handleEnqueueTask stubs POST /v1/tasks.
// TODO(M3-08): replace with full implementation using r.producer.
func (r *Router) handleEnqueueTask(w http.ResponseWriter, req *http.Request) {
	writeError(w, req, http.StatusNotImplemented, "not_implemented",
		"task enqueue is not yet implemented")
}

// handleGetTask stubs GET /v1/tasks/{taskID}.
// TODO(M3): replace with task status lookup.
func (r *Router) handleGetTask(w http.ResponseWriter, req *http.Request) {
	writeError(w, req, http.StatusNotImplemented, "not_implemented",
		"task lookup is not yet implemented")
}

// handleDeleteTask stubs DELETE /v1/tasks/{taskID}.
// TODO(M3): replace with task cancellation logic.
func (r *Router) handleDeleteTask(w http.ResponseWriter, req *http.Request) {
	writeError(w, req, http.StatusNotImplemented, "not_implemented",
		"task deletion is not yet implemented")
}

// handleListDLQ stubs GET /v1/dlq.
// TODO(M3): replace with DLQWriter.ListDLQ call.
func (r *Router) handleListDLQ(w http.ResponseWriter, req *http.Request) {
	writeError(w, req, http.StatusNotImplemented, "not_implemented",
		"DLQ listing is not yet implemented")
}

// ---------------------------------------------------------------------------
// Request / response DTOs
// ---------------------------------------------------------------------------

// EnqueueTaskRequest is the JSON body accepted by POST /v1/tasks.
type EnqueueTaskRequest struct {
	// Recipient is the destination email address. Required.
	Recipient string `json:"recipient"`
	// Type is the task category (registration, password_reset, etc.). Required.
	Type domain.TaskType `json:"type"`
	// Priority is 0 (low) to 3 (critical). Optional; defaults to PriorityNormal.
	Priority domain.Priority `json:"priority"`
	// TemplateID identifies the template to render. Required.
	TemplateID string `json:"template_id"`
	// TemplateData is the key/value map merged into the template. Optional.
	TemplateData map[string]any `json:"template_data,omitempty"`
	// ScheduledFor defers delivery until this time. Optional.
	ScheduledFor *time.Time `json:"scheduled_for,omitempty"`
	// Metadata is arbitrary string key/value for extensibility. Optional.
	Metadata map[string]string `json:"metadata,omitempty"`
}

// Validate checks required fields and value constraints.
func (req *EnqueueTaskRequest) Validate() error {
	if req.Recipient == "" {
		return &domain.ValidationError{Field: "recipient", Message: "recipient is required"}
	}
	if !req.Type.IsValid() {
		return &domain.ValidationError{Field: "type", Message: "type must be a valid TaskType"}
	}
	if req.TemplateID == "" {
		return &domain.ValidationError{Field: "template_id", Message: "template_id is required"}
	}
	if !req.Priority.IsValid() {
		return &domain.ValidationError{Field: "priority", Message: "priority must be 0–3"}
	}
	return nil
}

// errorResponse is the standard JSON error body returned by all error paths.
type errorResponse struct {
	Error     string `json:"error"`
	Code      string `json:"code"`
	RequestID string `json:"request_id"`
}

// ---------------------------------------------------------------------------
// Shared response helpers
// ---------------------------------------------------------------------------

// writeJSON serialises v as JSON and writes it with the given status code.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeError writes a consistent JSON error body.
func writeError(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	writeJSON(w, status, errorResponse{
		Error:     message,
		Code:      code,
		RequestID: RequestIDFromContext(r),
	})
}
