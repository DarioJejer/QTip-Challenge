// Package http wires together the chi router, all middleware, and HTTP handlers
// for the email queue service. Business-logic handlers are stubbed in M2 and
// replaced with real implementations in M3.
package http

import (
	"net/http"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/DarioJejer/go-email-queue/internal/config"
	"github.com/DarioJejer/go-email-queue/internal/ports"
)

// Router wraps a chi.Mux and holds the application dependencies injected at
// construction time. The readiness flag is used by the /readyz probe and is
// toggled by the supervisor during graceful shutdown (ADR-008).
type Router struct {
	mux      *chi.Mux
	cfg      *config.Config
	producer ports.TaskProducer
	ready    atomic.Bool
}

// NewRouter constructs a fully configured chi.Mux with all middleware and
// route registrations. The router is not started here — pass r.Handler() to
// an http.Server in main.go.
func NewRouter(
	cfg *config.Config,
	producer ports.TaskProducer,
) *Router {
	r := &Router{
		mux:      chi.NewMux(),
		cfg:      cfg,
		producer: producer,
	}
	r.ready.Store(false)
	r.registerMiddleware()
	r.registerRoutes()
	return r
}

// Handler returns the underlying http.Handler for use with http.Server.
func (r *Router) Handler() http.Handler { return r.mux }

// SetReady marks the service as ready or not-ready. The supervisor calls
// SetReady(false) as the first step of graceful shutdown so that the readiness
// probe starts failing before in-flight requests are interrupted (ADR-008).
func (r *Router) SetReady(v bool) { r.ready.Store(v) }

// IsReady reports the current readiness state.
func (r *Router) IsReady() bool { return r.ready.Load() }

// ---------------------------------------------------------------------------
// Middleware registration
// ---------------------------------------------------------------------------

func (r *Router) registerMiddleware() {
	r.mux.Use(requestIDMiddleware)
	r.mux.Use(chimiddleware.RealIP)
	r.mux.Use(requestLoggerMiddleware)
	r.mux.Use(recovererMiddleware)
}

// ---------------------------------------------------------------------------
// Route registration
// ---------------------------------------------------------------------------

func (r *Router) registerRoutes() {
	// Health / readiness probes — no auth required (ADR-008).
	r.mux.Get("/healthz", r.handleHealthz)
	r.mux.Get("/readyz", r.handleReadyz)

	// Prometheus metrics — no auth required, typically access-controlled by
	// network policy in Kubernetes (ADR-007).
	r.mux.Handle("/metrics", promhttp.Handler())

	// Authenticated API routes.
	r.mux.Group(func(m chi.Router) {
		m.Use(apiKeyAuthMiddleware(r.cfg.Server.APIKeys))
		m.Use(tenantExtractorMiddleware)
		m.Use(chimiddleware.Timeout(30 * time.Second))
		m.Use(contentTypeEnforcer)

		m.Post("/v1/tasks", r.handleEnqueueTask)
		m.Get("/v1/tasks/{taskID}", r.handleGetTask)
		m.Delete("/v1/tasks/{taskID}", r.handleDeleteTask)
		m.Get("/v1/dlq", r.handleListDLQ)
	})
}
