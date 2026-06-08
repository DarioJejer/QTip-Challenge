# =============================================================================
# Multi-stage Dockerfile for go-email-queue
# =============================================================================
# Final image: gcr.io/distroless/static-debian12 (~2 MB base)
# Target size: < 20 MB total (binary ~12 MB stripped + certs ~1 MB)
# Runs as UID 65532 (distroless nonroot) — matches ADR-008 securityContext

# -----------------------------------------------------------------------------
# Stage 1 — builder
# golang:1.23-alpine gives a minimal Go toolchain (~300 MB) without glibc.
# CGO_ENABLED=0 produces a fully static binary that runs in distroless.
# -----------------------------------------------------------------------------
FROM golang:1.23-alpine AS builder

# ca-certificates: copied to final stage for HTTPS (email provider + OTel)
# git: needed by go mod download for VCS stamping
RUN apk add --no-cache ca-certificates git

WORKDIR /build

# Copy dependency manifests first so Docker layer-caches the module download
# separately from source changes. The download layer is only invalidated when
# go.mod or go.sum change.
COPY go.mod go.sum ./
RUN go mod download

# Copy the full source tree.
COPY . .

# Build argument for version embedding — passed from `make docker-build`.
ARG VERSION=dev

# -trimpath: removes local file system paths from stack traces
# -w -s:     strips DWARF debug info and symbol table (reduces binary size ~30%)
# CGO_ENABLED=0 + GOOS=linux: fully static, OS-independent binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build \
    -trimpath \
    -ldflags "-X main.version=${VERSION} -w -s" \
    -o /server \
    ./cmd/server/

# -----------------------------------------------------------------------------
# Stage 2 — final (distroless)
# gcr.io/distroless/static-debian12 contains only:
#   /etc/passwd, /etc/ssl/certs, /lib/x86_64-linux-gnu/libc.so.6 stub
# No shell, no package manager, no curl — minimal attack surface.
# -----------------------------------------------------------------------------
FROM gcr.io/distroless/static-debian12

# Copy CA certificates from builder so the binary can make HTTPS calls
# to the email provider and the OTel collector without a TLS handshake error.
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Copy the compiled binary.
COPY --from=builder /server /server

# Run as the distroless "nonroot" user (UID 65532).
# Matches securityContext.runAsUser: 65532 in ADR-008.
USER 65532

# 8080 — HTTP API (chi router: /v1/tasks, /healthz, /readyz)
# 9090 — Prometheus metrics (/metrics)
EXPOSE 8080 9090

ENTRYPOINT ["/server"]
