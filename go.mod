module github.com/DarioJejer/go-email-queue

go 1.23

require (
	// Redis broker — ADR-001, ADR-002
	github.com/redis/go-redis/v9 v9.7.0

	// Domain & serialization — ADR-003
	github.com/google/uuid v1.6.0

	// HTTP router
	github.com/go-chi/chi/v5 v5.2.0

	// Structured logging — ADR-007
	github.com/rs/zerolog v1.33.0

	// Observability: metrics — ADR-007
	github.com/prometheus/client_golang v1.20.0

	// Observability: distributed tracing — ADR-007
	go.opentelemetry.io/otel                                      v1.33.0
	go.opentelemetry.io/otel/trace                                v1.33.0
	go.opentelemetry.io/otel/metric                               v1.33.0
	go.opentelemetry.io/otel/sdk                                  v1.33.0
	go.opentelemetry.io/otel/sdk/metric                           v1.33.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.33.0
	go.opentelemetry.io/otel/exporters/prometheus                 v0.55.0

	// Concurrency — ADR-004 (semaphore-bounded worker pool)
	golang.org/x/sync v0.10.0

	// Kubernetes CPU limit awareness — ADR-008
	go.uber.org/automaxprocs v1.6.0

	// Testing
	github.com/stretchr/testify v1.10.0
	github.com/alicebob/miniredis/v2 v2.33.0 // hermetic Redis for unit tests
)
