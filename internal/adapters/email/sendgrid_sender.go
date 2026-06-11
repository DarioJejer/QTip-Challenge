package email

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/DarioJejer/go-email-queue/internal/domain"
	"github.com/DarioJejer/go-email-queue/internal/observability"
	"github.com/DarioJejer/go-email-queue/internal/ports"
)

const (
	sendGridDefaultBaseURL = "https://api.sendgrid.com"
	sendGridMailPath       = "/v3/mail/send"
	sendGridProfilePath    = "/v3/user/profile"
	circuitMaxFailures     = 5
	circuitOpenDuration    = 30 * time.Second
)

// SendGridSender delivers emails via the SendGrid v3 Mail Send API (M3-07).
type SendGridSender struct {
	apiKey     string
	fromEmail  string
	fromName   string
	tracer     trace.Tracer
	metrics    ports.MetricsRecorder
	httpClient *http.Client
	breaker    *circuitBreaker
	baseURL    string
}

// Compile-time interface satisfaction check.
var _ ports.EmailSender = (*SendGridSender)(nil)

// NewSendGridSender constructs a production SendGrid adapter with circuit breaker
// and OTel instrumentation. fromEmail is required; fromName may be empty.
func NewSendGridSender(
	apiKey, fromEmail, fromName string,
	tracer trace.Tracer,
	metrics ports.MetricsRecorder,
) *SendGridSender {
	return &SendGridSender{
		apiKey:    apiKey,
		fromEmail: fromEmail,
		fromName:  fromName,
		tracer:    tracer,
		metrics:   metrics,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		breaker: newCircuitBreaker(circuitMaxFailures, circuitOpenDuration),
		baseURL: sendGridDefaultBaseURL,
	}
}

// Send posts a single templated email to SendGrid.
func (s *SendGridSender) Send(ctx context.Context, task *domain.EmailTask) error {
	ctx, span := s.tracer.Start(ctx, "email.send",
		trace.WithAttributes(
			attribute.String("task.id", task.ID),
			attribute.String("recipient.email", task.Recipient),
			attribute.String("template.id", task.TemplateID),
		),
	)
	defer span.End()

	if err := s.breaker.Allow(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	body, err := s.buildRequestBody(task)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("sendgrid: build request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.baseURL+sendGridMailPath, bytes.NewReader(body))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("sendgrid: create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+s.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		s.breaker.RecordFailure(ctx)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("sendgrid: request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		s.breaker.RecordSuccess()
		span.SetAttributes(attribute.Bool("send.success", true))
		logger := observability.LoggerFromContext(ctx)
		logger.Info().
			Str("event", "email.sent").
			Str("mode", "sendgrid").
			Str("task_id", task.ID).
			Str("recipient", task.Recipient).
			Str("template_id", task.TemplateID).
			Int("status", resp.StatusCode).
			Msg("email.sent (sendgrid)")
		return nil

	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		wrapped := fmt.Errorf("sendgrid: client error %d: %s: %w", resp.StatusCode, string(respBody), ports.ErrNonRetryable)
		span.RecordError(wrapped)
		span.SetStatus(codes.Error, wrapped.Error())
		span.SetAttributes(attribute.Bool("send.success", false))
		return wrapped

	default:
		s.breaker.RecordFailure(ctx)
		sendErr := fmt.Errorf("sendgrid: server error %d: %s", resp.StatusCode, string(respBody))
		span.RecordError(sendErr)
		span.SetStatus(codes.Error, sendErr.Error())
		span.SetAttributes(attribute.Bool("send.success", false))
		return sendErr
	}
}

// SendBatch delivers each task via Send; partial failures are collected.
func (s *SendGridSender) SendBatch(ctx context.Context, tasks []*domain.EmailTask) error {
	var errs []error
	for _, task := range tasks {
		if err := s.Send(ctx, task); err != nil {
			errs = append(errs, fmt.Errorf("task %s: %w", task.ID, err))
		}
	}
	if len(errs) > 0 {
		return &domain.MultiError{Errors: errs}
	}
	return nil
}

// HealthCheck performs a lightweight GET to the SendGrid user profile endpoint.
func (s *SendGridSender) HealthCheck(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.baseURL+sendGridProfilePath, nil)
	if err != nil {
		return fmt.Errorf("sendgrid: health check request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+s.apiKey)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("sendgrid: health check failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	return fmt.Errorf("sendgrid: health check status %d: %s", resp.StatusCode, string(body))
}

type sendGridMailRequest struct {
	Personalizations []sendGridPersonalization `json:"personalizations"`
	From             sendGridEmailAddress      `json:"from"`
	TemplateID       string                    `json:"template_id"`
}

type sendGridPersonalization struct {
	To                  []sendGridEmailAddress `json:"to"`
	DynamicTemplateData map[string]any         `json:"dynamic_template_data,omitempty"`
}

type sendGridEmailAddress struct {
	Email string `json:"email"`
	Name  string `json:"name,omitempty"`
}

func (s *SendGridSender) buildRequestBody(task *domain.EmailTask) ([]byte, error) {
	from := sendGridEmailAddress{Email: s.fromEmail, Name: s.fromName}
	payload := sendGridMailRequest{
		Personalizations: []sendGridPersonalization{{
			To:                  []sendGridEmailAddress{{Email: task.Recipient}},
			DynamicTemplateData: task.TemplateData,
		}},
		From:       from,
		TemplateID: task.TemplateID,
	}
	return json.Marshal(payload)
}
