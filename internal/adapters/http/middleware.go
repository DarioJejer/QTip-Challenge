package http

import (
	"context"
	"net/http"
	"runtime/debug"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// ---------------------------------------------------------------------------
// Context keys — unexported to prevent collisions across packages.
// ---------------------------------------------------------------------------

type contextKey string

const (
	contextKeyRequestID contextKey = "request_id"
	contextKeyTenantID  contextKey = "tenant_id"
)

// RequestIDFromContext extracts the request ID set by requestIDMiddleware.
// Returns an empty string if the value is not present.
func RequestIDFromContext(r *http.Request) string {
	if v, ok := r.Context().Value(contextKeyRequestID).(string); ok {
		return v
	}
	return ""
}

// TenantIDFromContext extracts the tenant ID set by tenantExtractorMiddleware.
// Returns an empty string if the value is not present.
func TenantIDFromContext(r *http.Request) string {
	if v, ok := r.Context().Value(contextKeyTenantID).(string); ok {
		return v
	}
	return ""
}

// ---------------------------------------------------------------------------
// requestIDMiddleware — generates a UUID request ID and echoes it back.
// ---------------------------------------------------------------------------

func requestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" {
			id = uuid.New().String()
		}
		ctx := context.WithValue(r.Context(), contextKeyRequestID, id)
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// ---------------------------------------------------------------------------
// requestLoggerMiddleware — structured zerolog access log entry per request.
// ---------------------------------------------------------------------------

func requestLoggerMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := newResponseWriter(w)
		next.ServeHTTP(ww, r)

		log.Logger.WithLevel(levelForStatus(ww.status)).
			Str("method", r.Method).
			Str("path", r.URL.Path).
			Int("status", ww.status).
			Dur("duration_ms", time.Since(start)).
			Str("request_id", RequestIDFromContext(r)).
			Str("tenant_id", TenantIDFromContext(r)).
			Str("remote_addr", r.RemoteAddr).
			Msg("http request")
	})
}

func levelForStatus(status int) zerolog.Level {
	switch {
	case status >= 500:
		return zerolog.ErrorLevel
	case status >= 400:
		return zerolog.WarnLevel
	default:
		return zerolog.InfoLevel
	}
}

// ---------------------------------------------------------------------------
// recovererMiddleware — catches panics and returns a 500.
// ---------------------------------------------------------------------------

func recovererMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Error().
					Interface("panic", rec).
					Str("stack", string(debug.Stack())).
					Str("request_id", RequestIDFromContext(r)).
					Msg("http handler panic recovered")
				writeError(w, r, http.StatusInternalServerError, "internal_error", "an unexpected error occurred")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// ---------------------------------------------------------------------------
// apiKeyAuthMiddleware — validates X-API-Key header.
// ---------------------------------------------------------------------------

// apiKeyAuthMiddleware returns a middleware that requires the X-API-Key header
// to match one of the configured valid keys. Returns 401 on failure.
func apiKeyAuthMiddleware(validKeys []string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			provided := r.Header.Get("X-API-Key")
			if provided == "" {
				writeError(w, r, http.StatusUnauthorized, "missing_api_key", "X-API-Key header is required")
				return
			}
			for _, k := range validKeys {
				if provided == k {
					next.ServeHTTP(w, r)
					return
				}
			}
			writeError(w, r, http.StatusUnauthorized, "invalid_api_key", "the provided API key is not valid")
		})
	}
}

// ---------------------------------------------------------------------------
// tenantExtractorMiddleware — requires X-Tenant-ID and sets it in context.
// ---------------------------------------------------------------------------

func tenantExtractorMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tenantID := r.Header.Get("X-Tenant-ID")
		if tenantID == "" {
			writeError(w, r, http.StatusBadRequest, "missing_tenant_id", "X-Tenant-ID header is required")
			return
		}
		ctx := context.WithValue(r.Context(), contextKeyTenantID, tenantID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// ---------------------------------------------------------------------------
// contentTypeEnforcer — POST/PUT/PATCH must declare application/json.
// ---------------------------------------------------------------------------

func contentTypeEnforcer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost, http.MethodPut, http.MethodPatch:
			ct := r.Header.Get("Content-Type")
			if !strings.HasPrefix(ct, "application/json") {
				writeError(w, r, http.StatusUnsupportedMediaType, "unsupported_media_type",
					"Content-Type must be application/json")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// ---------------------------------------------------------------------------
// responseWriter — captures status code for logging.
// ---------------------------------------------------------------------------

type responseWriter struct {
	http.ResponseWriter
	status int
}

func newResponseWriter(w http.ResponseWriter) *responseWriter {
	return &responseWriter{ResponseWriter: w, status: http.StatusOK}
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}
