# yellow-pages — build & quality targets.
#
# GOFLAGS=-mod=readonly is exported for every go invocation so builds never
# silently mutate go.mod/go.sum (supply-chain gate).

BIN_DIR    := bin
BINARY     := $(BIN_DIR)/yp
PKG        := ./cmd/yp
VERSION    ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS    := -s -w -X main.version=$(VERSION)

# Proto lives under proto/discovery/v1. Generation pins the plugin versions so
# the output is reproducible (install them with `make proto-tools`).
PROTO_DIR                  := proto
PROTO_FILE                 := $(PROTO_DIR)/discovery/v1/discovery.proto
PROTOC_GEN_GO_VERSION      := v1.36.11
PROTOC_GEN_GO_GRPC_VERSION := v1.6.0

export GOFLAGS := -mod=readonly

.PHONY: all build test vet lint buf-lint buf-breaking vuln tidy-check verify proto proto-tools clean help

all: build ## Build the binary

build: ## Build the yp binary
	@echo ">> building $(BINARY) ($(VERSION))"
	@mkdir -p $(BIN_DIR)
	@go build -ldflags '$(LDFLAGS)' -o $(BINARY) $(PKG)

test: ## Run tests with the race detector
	@echo ">> testing (race)"
	@go test -race ./...

vet: ## Run go vet
	@echo ">> vetting"
	@go vet ./...

lint: ## Run golangci-lint (includes gosec)
	@echo ">> linting"
	@golangci-lint run

buf-lint: ## Lint the proto contract (requires buf)
	@echo ">> buf lint"
	@buf lint

buf-breaking: ## Fail on a breaking proto change vs the committed contract (append-only gate)
	@echo ">> buf breaking"
	@# Baseline is the branch HEAD, not main: the greenfield rebuild intentionally
	@# replaced the legacy proto, so main is not a valid discovery.v1 baseline.
	@buf breaking --against '.git#ref=HEAD'

vuln: ## Run govulncheck
	@echo ">> vulnerability scan"
	@GOFLAGS= go tool govulncheck ./...

tidy-check: ## Fail if go.mod/go.sum are not tidy and verified
	@echo ">> verifying modules"
	@go mod verify
	@GOFLAGS= go mod tidy -diff

verify: tidy-check vet lint buf-lint buf-breaking test vuln ## Run the full local quality gate

proto: ## Regenerate Go from proto (needs protoc + pinned plugins)
	@echo ">> generating proto"
	@protoc \
		--proto_path=$(PROTO_DIR) \
		--go_out=$(PROTO_DIR) --go_opt=paths=source_relative \
		--go-grpc_out=$(PROTO_DIR) --go-grpc_opt=paths=source_relative \
		$(PROTO_FILE)

proto-tools: ## Install the pinned protoc-gen-go[-grpc] plugins
	@echo ">> installing proto plugins"
	@go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	@go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)

clean: ## Remove build artifacts
	@rm -rf $(BIN_DIR)

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'
