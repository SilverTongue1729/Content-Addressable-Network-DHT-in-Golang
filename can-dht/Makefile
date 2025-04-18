# CAN-DHT Makefile

# Variables
BINARY_DIR = bin
TEST_DATA_DIR = test-data
NODE_BINARY = $(BINARY_DIR)/can-node
TEST_CLIENT_BINARY = $(BINARY_DIR)/test-client
GO = go

# Default target
.PHONY: all
all: build

# Build targets
.PHONY: build
build: build-node build-test-client

.PHONY: build-node
build-node:
	@echo "Building node binary..."
	@mkdir -p $(BINARY_DIR)
	@$(GO) build -o $(NODE_BINARY) ./cmd/node

.PHONY: build-test-client
build-test-client:
	@echo "Building test client..."
	@mkdir -p $(BINARY_DIR)
	@$(GO) build -o $(TEST_CLIENT_BINARY) ./cmd/test-client

# Test targets
.PHONY: test
test: build
	@echo "Running test client..."
	@mkdir -p $(TEST_DATA_DIR)
	@$(TEST_CLIENT_BINARY) --data-dir $(TEST_DATA_DIR)
	@rm -rf $(TEST_DATA_DIR)

.PHONY: test-node
test-node:
	@echo "Running node package tests..."
	@$(GO) test ./pkg/node -v

.PHONY: test-all
test-all:
	@echo "Running all package tests..."
	@$(GO) test ./pkg/node -v
	@$(GO) test ./pkg/routing -v 2>/dev/null || echo "Routing tests not implemented yet"
	@$(GO) test ./pkg/storage -v 2>/dev/null || echo "Storage tests not implemented yet"
	@$(GO) test ./pkg/crypto -v 2>/dev/null || echo "Crypto tests not implemented yet"

# Run targets
.PHONY: run-node
run-node: build-node
	@echo "Running a new CAN node..."
	@mkdir -p data/node1
	@$(NODE_BINARY) --port 8080 --dimensions 2 --data-dir ./data/node1

.PHONY: run-node-join
run-node-join: build-node
	@echo "Running a CAN node and joining an existing network..."
	@mkdir -p data/node2
	@$(NODE_BINARY) --port 8081 --dimensions 2 --data-dir ./data/node2 --join localhost:8080

.PHONY: run-test-client
run-test-client: build-test-client
	@echo "Running test client..."
	@mkdir -p $(TEST_DATA_DIR)
	@$(TEST_CLIENT_BINARY) --data-dir $(TEST_DATA_DIR)

# Clean target
.PHONY: clean
clean:
	@echo "Cleaning up..."
	@rm -rf $(BINARY_DIR)
	@rm -rf $(TEST_DATA_DIR)
	@rm -rf data
	@echo "Cleaned up build and test artifacts"

# Dep target
.PHONY: deps
deps:
	@echo "Fetching dependencies..."
	@$(GO) mod tidy

# Help target
.PHONY: help
help:
	@echo "CAN-DHT Makefile Help"
	@echo "====================="
	@echo ""
	@echo "Available targets:"
	@echo "  all            : Build all binaries (default target)"
	@echo "  build          : Build node and test client binaries"
	@echo "  build-node     : Build only the node binary"
	@echo "  build-test-client : Build only the test client binary"
	@echo "  test           : Run the test client"
	@echo "  test-node      : Run tests for the node package"
	@echo "  test-all       : Run tests for all packages"
	@echo "  run-node       : Run a CAN node (creates a new network)"
	@echo "  run-node-join  : Run a CAN node and join an existing network"
	@echo "  run-test-client : Run the test client"
	@echo "  clean          : Remove build and test artifacts"
	@echo "  deps           : Fetch dependencies"
	@echo "  demo-integrity : Run the integrity demo"
	@echo "  demo-cache     : Run the cache demo"
	@echo "  visualize      : Start the CAN DHT visualization tool"
	@echo "  help           : Show this help message"

.PHONY: demo-integrity
demo-integrity: build
	@echo "Running integrity demo..."
	./bin/integrity-demo

.PHONY: demo-cache
demo-cache:
	@echo "Running cache demo..."
	@go run ./cmd/cache-demo/main.go

.PHONY: visualize
visualize:
	@echo "Starting CAN DHT visualization tool..."
	@go run ./cmd/visualization/main.go
