# =============================================================================
# go-email-queue — Makefile
# =============================================================================
# Usage: make <target>
# Run `make help` to list all available targets.

BINARY_NAME   := server
BINARY_PATH   := bin/$(BINARY_NAME)
MODULE        := github.com/DarioJejer/go-email-queue

# Embed version and build timestamp at link time.
# Falls back to "dev" when not inside a git repository.
VERSION    := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
BUILD_TIME := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")

LDFLAGS := -ldflags "-X main.version=$(VERSION) -X main.buildTime=$(BUILD_TIME) -w -s"
GOFLAGS := -trimpath

.DEFAULT_GOAL := help
.PHONY: all build test test-cover lint vet tidy run docker-build docker-run clean generate help

## all: vet + lint + test + build
all: vet lint test build

## build: compile the server binary into bin/
build:
	@echo ">> Building $(BINARY_PATH) (version=$(VERSION))"
	@mkdir -p bin
	CGO_ENABLED=0 go build $(GOFLAGS) $(LDFLAGS) -o $(BINARY_PATH) ./cmd/server/

## test: run all tests with race detector and coverage profiling
test:
	@echo ">> Testing"
	go test -race -coverprofile=coverage.out -covermode=atomic ./...

## test-cover: run tests then open HTML coverage report
test-cover: test
	@echo ">> Opening coverage report"
	go tool cover -html=coverage.out

## lint: run golangci-lint (requires golangci-lint in PATH)
lint:
	@echo ">> Linting"
	golangci-lint run ./...

## vet: run go vet on all packages
vet:
	@echo ">> Vetting"
	go vet ./...

## tidy: sync go.mod and go.sum
tidy:
	@echo ">> Tidying modules"
	go mod tidy

## run: start Redis via docker-compose, then run the server locally
# Requires docker-compose. All env vars can be overridden on the command line,
# e.g.: make run LOG_LEVEL=debug
run:
	@echo ">> Starting Redis (docker-compose)"
	docker-compose up -d redis
	@echo ">> Waiting for Redis to be healthy..."
	@until docker-compose exec redis redis-cli ping 2>/dev/null | grep -q PONG; do \
		printf '.'; sleep 1; \
	done; echo " ready"
	@echo ">> Running server (Ctrl+C to stop)"
	REDIS_URL=redis://localhost:6379 \
	API_KEYS=dev-key-1 \
	LOG_FORMAT=console \
	LOG_LEVEL=debug \
	go run $(LDFLAGS) ./cmd/server/

## docker-build: build the Docker image
docker-build:
	@echo ">> Building Docker image go-email-queue:$(VERSION)"
	docker build \
		--build-arg VERSION=$(VERSION) \
		-t go-email-queue:$(VERSION) \
		-t go-email-queue:latest \
		.

## docker-run: run the Docker image using .env.local for configuration
# Create .env.local from .env.example before first use.
docker-run:
	@echo ">> Running go-email-queue:latest"
	docker run --rm \
		-p 8080:8080 \
		-p 9090:9090 \
		--env-file .env.local \
		go-email-queue:latest

## clean: remove build artifacts and coverage files
clean:
	@echo ">> Cleaning"
	@rm -rf bin/ coverage.out

## generate: run go generate on all packages
generate:
	@echo ">> Generating"
	go generate ./...

## help: print this help
help:
	@echo "Usage: make <target>\n"
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/## /  /'
