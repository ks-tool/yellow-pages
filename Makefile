PROTO_DIR := proto
GEN_DIR := proto/gen
PROTO_FILE := $(PROTO_DIR)/service_discovery.proto

.PHONY: proto
proto: ## Generate Go code from protobuf
	@echo "Generating Go code from protobuf..."
	@mkdir -p $(GEN_DIR)
	@protoc \
		--proto_path=$(PROTO_DIR) \
		--go_out=$(GEN_DIR) --go_opt=paths=source_relative \
		--go-grpc_out=$(GEN_DIR) --go-grpc_opt=paths=source_relative \
		$(PROTO_FILE)

build: ## Build binary
	@echo "Build binary ..."
	@GOEXPERIMENT=jsonv2 go build -o bin/yp -ldflags='-s -w' ./cmd/yellow-pages
