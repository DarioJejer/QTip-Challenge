package http_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	httpadapter "github.com/DarioJejer/go-email-queue/internal/adapters/http"
	"github.com/DarioJejer/go-email-queue/internal/adapters/stubs"
	"github.com/DarioJejer/go-email-queue/internal/config"
)

// newTestRouter builds a Router wired with stubs and a single known API key.
func newTestRouter(t *testing.T) *httpadapter.Router {
	t.Helper()
	cfg := &config.Config{
		Server: config.ServerConfig{
			APIKeys:      []string{"test-key"},
			ReadTimeout:  0,
			WriteTimeout: 0,
		},
	}
	return httpadapter.NewRouter(cfg, stubs.NewStubProducer())
}

// do performs an HTTP request against the router and returns the recorder.
func do(t *testing.T, router *httpadapter.Router, method, path string, headers map[string]string, body string) *httptest.ResponseRecorder {
	t.Helper()
	var reqBody *strings.Reader
	if body != "" {
		reqBody = strings.NewReader(body)
	} else {
		reqBody = strings.NewReader("")
	}
	req := httptest.NewRequest(method, path, reqBody)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rr := httptest.NewRecorder()
	router.Handler().ServeHTTP(rr, req)
	return rr
}

// authHeaders returns headers that satisfy both APIKeyAuth and TenantExtractor.
func authHeaders() map[string]string {
	return map[string]string{
		"X-API-Key":    "test-key",
		"X-Tenant-ID":  "tenant-abc",
		"Content-Type": "application/json",
	}
}

// ---------------------------------------------------------------------------
// /healthz
// ---------------------------------------------------------------------------

func TestHealthz_AlwaysReturns200(t *testing.T) {
	router := newTestRouter(t)
	rr := do(t, router, http.MethodGet, "/healthz", nil, "")
	assert.Equal(t, http.StatusOK, rr.Code)

	var body map[string]string
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&body))
	assert.Equal(t, "ok", body["status"])
}

// ---------------------------------------------------------------------------
// /readyz
// ---------------------------------------------------------------------------

func TestReadyz_NotReady_Returns503(t *testing.T) {
	router := newTestRouter(t)
	// Default state is not-ready.
	rr := do(t, router, http.MethodGet, "/readyz", nil, "")
	assert.Equal(t, http.StatusServiceUnavailable, rr.Code)

	var body map[string]string
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&body))
	assert.Equal(t, "not_ready", body["status"])
}

func TestReadyz_Ready_Returns200(t *testing.T) {
	router := newTestRouter(t)
	router.SetReady(true)
	rr := do(t, router, http.MethodGet, "/readyz", nil, "")
	assert.Equal(t, http.StatusOK, rr.Code)

	var body map[string]string
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&body))
	assert.Equal(t, "ready", body["status"])
}

func TestReadyz_ToggleReady(t *testing.T) {
	router := newTestRouter(t)

	router.SetReady(true)
	assert.True(t, router.IsReady())

	router.SetReady(false)
	assert.False(t, router.IsReady())
}

// ---------------------------------------------------------------------------
// APIKeyAuth middleware
// ---------------------------------------------------------------------------

func TestAPIKeyAuth(t *testing.T) {
	tests := []struct {
		name       string
		headers    map[string]string
		wantStatus int
		wantCode   string
	}{
		{
			name:       "valid key passes",
			headers:    map[string]string{"X-API-Key": "test-key", "X-Tenant-ID": "t1", "Content-Type": "application/json"},
			wantStatus: http.StatusNotImplemented, // stub handler
		},
		{
			name:       "missing key returns 401",
			headers:    map[string]string{"X-Tenant-ID": "t1"},
			wantStatus: http.StatusUnauthorized,
			wantCode:   "missing_api_key",
		},
		{
			name:       "wrong key returns 401",
			headers:    map[string]string{"X-API-Key": "wrong-key", "X-Tenant-ID": "t1"},
			wantStatus: http.StatusUnauthorized,
			wantCode:   "invalid_api_key",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			router := newTestRouter(t)
			rr := do(t, router, http.MethodPost, "/v1/tasks", tc.headers, `{"recipient":"a@b.com","type":"billing","template_id":"t"}`)
			assert.Equal(t, tc.wantStatus, rr.Code)
			if tc.wantCode != "" {
				var body map[string]string
				require.NoError(t, json.NewDecoder(rr.Body).Decode(&body))
				assert.Equal(t, tc.wantCode, body["code"])
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TenantExtractor middleware
// ---------------------------------------------------------------------------

func TestTenantExtractor(t *testing.T) {
	tests := []struct {
		name       string
		headers    map[string]string
		wantStatus int
		wantCode   string
	}{
		{
			name:       "tenant present passes",
			headers:    authHeaders(),
			wantStatus: http.StatusNotImplemented,
		},
		{
			name: "missing tenant returns 400",
			headers: map[string]string{
				"X-API-Key":    "test-key",
				"Content-Type": "application/json",
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "missing_tenant_id",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			router := newTestRouter(t)
			rr := do(t, router, http.MethodPost, "/v1/tasks", tc.headers, `{"recipient":"a@b.com","type":"billing","template_id":"t"}`)
			assert.Equal(t, tc.wantStatus, rr.Code)
			if tc.wantCode != "" {
				var body map[string]string
				require.NoError(t, json.NewDecoder(rr.Body).Decode(&body))
				assert.Equal(t, tc.wantCode, body["code"])
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ContentType enforcer middleware
// ---------------------------------------------------------------------------

func TestContentTypeEnforcer(t *testing.T) {
	tests := []struct {
		name        string
		contentType string
		wantStatus  int
	}{
		{"application/json passes", "application/json", http.StatusNotImplemented},
		{"application/json; charset=utf-8 passes", "application/json; charset=utf-8", http.StatusNotImplemented},
		{"text/plain rejected", "text/plain", http.StatusUnsupportedMediaType},
		{"no content-type rejected", "", http.StatusUnsupportedMediaType},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			router := newTestRouter(t)
			h := map[string]string{
				"X-API-Key":   "test-key",
				"X-Tenant-ID": "t1",
			}
			if tc.contentType != "" {
				h["Content-Type"] = tc.contentType
			}
			rr := do(t, router, http.MethodPost, "/v1/tasks", h, `{}`)
			assert.Equal(t, tc.wantStatus, rr.Code)
		})
	}
}

// ---------------------------------------------------------------------------
// Stubbed business endpoints — all return 501
// ---------------------------------------------------------------------------

func TestStubEndpoints_Return501(t *testing.T) {
	tests := []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodPost, "/v1/tasks", `{"recipient":"a@b.com","type":"billing","template_id":"t"}`},
		{http.MethodGet, "/v1/tasks/task-123", ""},
		{http.MethodDelete, "/v1/tasks/task-123", ""},
		{http.MethodGet, "/v1/dlq", ""},
	}

	for _, tc := range tests {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			router := newTestRouter(t)
			h := map[string]string{
				"X-API-Key":    "test-key",
				"X-Tenant-ID":  "t1",
				"Content-Type": "application/json",
			}
			rr := do(t, router, tc.method, tc.path, h, tc.body)
			assert.Equal(t, http.StatusNotImplemented, rr.Code)

			var body map[string]string
			require.NoError(t, json.NewDecoder(rr.Body).Decode(&body))
			assert.Equal(t, "not_implemented", body["code"])
			assert.NotEmpty(t, body["request_id"], "request_id must be present in error response")
		})
	}
}

// ---------------------------------------------------------------------------
// X-Request-ID propagation
// ---------------------------------------------------------------------------

func TestRequestID_GeneratedWhenAbsent(t *testing.T) {
	router := newTestRouter(t)
	rr := do(t, router, http.MethodGet, "/healthz", nil, "")
	assert.NotEmpty(t, rr.Header().Get("X-Request-ID"))
}

func TestRequestID_EchoedWhenProvided(t *testing.T) {
	router := newTestRouter(t)
	rr := do(t, router, http.MethodGet, "/healthz", map[string]string{"X-Request-ID": "my-id-123"}, "")
	assert.Equal(t, "my-id-123", rr.Header().Get("X-Request-ID"))
}

// ---------------------------------------------------------------------------
// EnqueueTaskRequest.Validate
// ---------------------------------------------------------------------------

func TestEnqueueTaskRequest_Validate(t *testing.T) {
	tests := []struct {
		name      string
		req       httpadapter.EnqueueTaskRequest
		wantErr   bool
		wantField string
	}{
		{
			name: "valid request",
			req: httpadapter.EnqueueTaskRequest{
				Recipient:  "user@example.com",
				Type:       "transactional",
				TemplateID: "welcome-v1",
				Priority:   1,
			},
			wantErr: false,
		},
		{
			name:      "missing recipient",
			req:       httpadapter.EnqueueTaskRequest{Type: "billing", TemplateID: "t", Priority: 0},
			wantErr:   true,
			wantField: "recipient",
		},
		{
			name:      "invalid type",
			req:       httpadapter.EnqueueTaskRequest{Recipient: "a@b.com", Type: "unknown", TemplateID: "t", Priority: 0},
			wantErr:   true,
			wantField: "type",
		},
		{
			name:      "missing template_id",
			req:       httpadapter.EnqueueTaskRequest{Recipient: "a@b.com", Type: "billing", Priority: 0},
			wantErr:   true,
			wantField: "template_id",
		},
		{
			name:      "invalid priority",
			req:       httpadapter.EnqueueTaskRequest{Recipient: "a@b.com", Type: "billing", TemplateID: "t", Priority: 99},
			wantErr:   true,
			wantField: "priority",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.req.Validate()
			if !tc.wantErr {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantField)
		})
	}
}
