# kqlparser Makefile
# Hierarchical targets with logical naming convention
#
# Naming convention:
#   verb        - top-level action (build, test, lint, fmt, clean)
#   verb-scope  - scoped action (test-unit, lint-vet)
#   verb-scope-x - further refinement (test-cover-html)

.PHONY: help
.DEFAULT_GOAL := help

# ============================================================================
# Configuration
# ============================================================================

GO          := go
GOTEST      := $(GO) test
GOBUILD     := $(GO) build
GOVET       := $(GO) vet
GOFMT       := gofmt
GOLINT      := golangci-lint

MODULE      := github.com/cloudygreybeard/kqlparser
PACKAGES    := ./...
COVER_FILE  := coverage.out
COVER_HTML  := coverage.html

# Build info
VERSION     ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT      ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_DATE  := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")

# ============================================================================
# Help
# ============================================================================

.PHONY: help
help: ## Show this help
	@echo "kqlparser - KQL Parser Library for Go"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z0-9_-]+:.*?## / {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST) | sort

# ============================================================================
# Build
# ============================================================================

.PHONY: build build-check
build: ## Build all packages
	$(GOBUILD) $(PACKAGES)

build-check: ## Check that packages compile without producing output
	$(GOBUILD) -o /dev/null $(PACKAGES)

# ============================================================================
# Test
# ============================================================================

.PHONY: test test-unit test-verbose test-short test-cover test-cover-html test-cover-func test-bench test-bench-cpu test-bench-mem
test: test-unit ## Run all tests (alias for test-unit)

test-unit: ## Run unit tests
	$(GOTEST) -race $(PACKAGES)

test-verbose: ## Run tests with verbose output
	$(GOTEST) -v -race $(PACKAGES)

test-short: ## Run tests in short mode
	$(GOTEST) -short $(PACKAGES)

test-cover: ## Run tests with coverage
	$(GOTEST) -race -coverprofile=$(COVER_FILE) -covermode=atomic $(PACKAGES)

test-cover-html: test-cover ## Generate HTML coverage report
	$(GO) tool cover -html=$(COVER_FILE) -o $(COVER_HTML)
	@echo "Coverage report: $(COVER_HTML)"

test-cover-func: test-cover ## Show function coverage summary
	$(GO) tool cover -func=$(COVER_FILE)

test-bench: ## Run benchmarks
	$(GOTEST) -bench=. -benchmem $(PACKAGES)

test-bench-cpu: ## Run benchmarks with CPU profiling
	$(GOTEST) -bench=. -benchmem -cpuprofile=cpu.prof $(PACKAGES)

test-bench-mem: ## Run benchmarks with memory profiling
	$(GOTEST) -bench=. -benchmem -memprofile=mem.prof $(PACKAGES)

# ============================================================================
# Lint
# ============================================================================

.PHONY: lint lint-all lint-vet lint-fmt lint-golangci lint-staticcheck
lint: lint-all ## Run all linters

lint-all: lint-vet lint-fmt lint-golangci ## Run all lint checks

lint-vet: ## Run go vet
	$(GOVET) $(PACKAGES)

lint-fmt: ## Check formatting (no changes)
	@test -z "$$($(GOFMT) -l .)" || { echo "Files need formatting:"; $(GOFMT) -l .; exit 1; }

lint-golangci: ## Run golangci-lint (if installed)
	@which $(GOLINT) > /dev/null 2>&1 && $(GOLINT) run $(PACKAGES) || echo "golangci-lint not installed, skipping"

lint-staticcheck: ## Run staticcheck (if installed)
	@which staticcheck > /dev/null 2>&1 && staticcheck $(PACKAGES) || echo "staticcheck not installed, skipping"

# ============================================================================
# Format
# ============================================================================

.PHONY: fmt fmt-go fmt-imports
fmt: fmt-go ## Format all code

fmt-go: ## Format Go code
	$(GOFMT) -w -s .

fmt-imports: ## Format imports (requires goimports)
	@which goimports > /dev/null 2>&1 && goimports -w . || echo "goimports not installed, skipping"

# ============================================================================
# Dependencies
# ============================================================================

.PHONY: deps deps-tidy deps-verify deps-update deps-graph
deps: ## Download dependencies
	$(GO) mod download

deps-tidy: ## Tidy go.mod
	$(GO) mod tidy

deps-verify: ## Verify dependencies
	$(GO) mod verify

deps-update: ## Update all dependencies
	$(GO) get -u $(PACKAGES)
	$(GO) mod tidy

deps-graph: ## Show dependency graph
	$(GO) mod graph

# ============================================================================
# Generate
# ============================================================================

.PHONY: generate
generate: ## Run go generate
	$(GO) generate $(PACKAGES)

# ============================================================================
# Clean
# ============================================================================

.PHONY: clean clean-cache clean-testcache clean-all
clean: ## Clean build artifacts
	$(GO) clean
	rm -f $(COVER_FILE) $(COVER_HTML)
	rm -f cpu.prof mem.prof

clean-cache: ## Clean Go build cache
	$(GO) clean -cache

clean-testcache: ## Clean test cache
	$(GO) clean -testcache

clean-all: clean clean-cache clean-testcache ## Clean everything

# ============================================================================
# Development Workflow
# ============================================================================

.PHONY: check ci dev
check: build lint-all test-unit ## Full pre-commit check (build, lint, test)

ci: deps check test-cover-func ## CI pipeline (deps, check, coverage)

dev: fmt check ## Development cycle (format, check)

# ============================================================================
# Tools
# ============================================================================

.PHONY: tools
tools: ## Install development tools
	$(GO) install golang.org/x/tools/cmd/goimports@latest
	$(GO) install honnef.co/go/tools/cmd/staticcheck@latest
	@echo "Note: Install golangci-lint separately: https://golangci-lint.run/usage/install/"

# ============================================================================
# Info
# ============================================================================

.PHONY: info info-packages info-deps
info: ## Show build info
	@echo "Module:     $(MODULE)"
	@echo "Version:    $(VERSION)"
	@echo "Commit:     $(COMMIT)"
	@echo "Build Date: $(BUILD_DATE)"
	@echo "Go Version: $(shell $(GO) version)"

info-packages: ## List all packages
	$(GO) list $(PACKAGES)

info-deps: ## List direct dependencies
	$(GO) list -m all
