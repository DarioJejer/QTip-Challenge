package email

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/DarioJejer/go-email-queue/internal/adapters/stubs"
	"github.com/DarioJejer/go-email-queue/internal/domain"
	"github.com/DarioJejer/go-email-queue/internal/ports"
)

func newTestSendGridSender(t *testing.T, handler http.HandlerFunc) *SendGridSender {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	s := NewSendGridSender("SG.test-key", "noreply@example.com", "QTip", noop.NewTracerProvider().Tracer("test"), stubs.NewStubMetricsRecorder())
	s.baseURL = srv.URL
	s.httpClient = srv.Client()
	return s
}

func TestSendGridSender_Success(t *testing.T) {
	t.Parallel()
	sender := newTestSendGridSender(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/v3/mail/send", r.URL.Path)
		assert.Equal(t, "Bearer SG.test-key", r.Header.Get("Authorization"))
		w.WriteHeader(http.StatusAccepted)
	})

	err := sender.Send(context.Background(), stubTask("task-sg-ok"))
	require.NoError(t, err)
}

func TestSendGridSender_NonRetryable4xx(t *testing.T) {
	t.Parallel()
	sender := newTestSendGridSender(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"errors":[{"message":"invalid email"}]}`))
	})

	err := sender.Send(context.Background(), stubTask("task-sg-4xx"))
	require.Error(t, err)
	assert.True(t, errors.Is(err, ports.ErrNonRetryable))
}

func TestSendGridSender_Retryable5xx(t *testing.T) {
	t.Parallel()
	sender := newTestSendGridSender(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})

	err := sender.Send(context.Background(), stubTask("task-sg-5xx"))
	require.Error(t, err)
	assert.False(t, errors.Is(err, ports.ErrNonRetryable))
	assert.False(t, errors.Is(err, ports.ErrCircuitOpen))
}

func TestSendGridSender_CircuitBreakerOpensAfterFiveFailures(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	sender := newTestSendGridSender(t, func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	})

	task := stubTask("task-cb")
	for i := 0; i < circuitMaxFailures; i++ {
		err := sender.Send(context.Background(), task)
		require.Error(t, err)
		assert.False(t, errors.Is(err, ports.ErrCircuitOpen))
	}

	before := calls.Load()
	err := sender.Send(context.Background(), task)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ports.ErrCircuitOpen))
	assert.Equal(t, before, calls.Load(), "circuit open must short-circuit without calling SendGrid")
}

func TestSendGridSender_HealthCheck(t *testing.T) {
	t.Parallel()
	sender := newTestSendGridSender(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == sendGridProfilePath {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})

	require.NoError(t, sender.HealthCheck(context.Background()))
}

func TestSendGridSender_SendBatch_MultiError(t *testing.T) {
	t.Parallel()
	var n atomic.Int32
	sender := newTestSendGridSender(t, func(w http.ResponseWriter, _ *http.Request) {
		if n.Add(1) == 1 {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		w.WriteHeader(http.StatusBadRequest)
	})

	tasks := []*domain.EmailTask{stubTask("b1"), stubTask("b2")}
	err := sender.SendBatch(context.Background(), tasks)
	require.Error(t, err)
	var multi *domain.MultiError
	require.ErrorAs(t, err, &multi)
	assert.Len(t, multi.Errors, 1)
}
