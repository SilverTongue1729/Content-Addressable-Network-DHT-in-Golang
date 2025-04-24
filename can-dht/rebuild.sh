#!/bin/bash

# Exit on error
set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}CAN-DHT Complete Rebuild Script${NC}"
echo "================================="

# Clean everything
echo -e "${YELLOW}Cleaning up previous builds...${NC}"
make clean
make clean-proto
rm -rf data bin

# Go version check
echo -e "${YELLOW}Checking Go version...${NC}"
GO_VERSION=$(go version)
echo "Using $GO_VERSION"

# Update dependencies
echo -e "${YELLOW}Updating dependencies...${NC}"
go mod tidy
go mod download

# Install protoc plugins if needed
echo -e "${YELLOW}Installing protoc plugins...${NC}"
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Generate proto files
echo -e "${YELLOW}Generating proto files...${NC}"
./generate.sh

# Build core components
echo -e "${YELLOW}Building core binaries...${NC}"
mkdir -p bin
go build -o bin/can-node ./cmd/node
go build -o bin/test-client ./cmd/test-client
go build -o bin/can-cli ./cmd/can-cli

# Build additional utilities if possible
echo -e "${YELLOW}Building additional utilities...${NC}"
go build -o bin/cache-demo ./cmd/cache-demo || echo -e "${RED}Failed to build cache-demo${NC}"
go build -o bin/integrity-demo ./cmd/integrity-demo || echo -e "${RED}Failed to build integrity-demo${NC}"
go build -o bin/routing-demo ./cmd/routing-demo || echo -e "${RED}Failed to build routing-demo${NC}"
go build -o bin/visualization ./cmd/visualization || echo -e "${RED}Failed to build visualization${NC}"

# Run tests
echo -e "${YELLOW}Running tests...${NC}"
go test ./pkg/node -v
go test ./pkg/routing -v || echo -e "${RED}Routing tests failed or not implemented${NC}"
go test ./pkg/storage -v || echo -e "${RED}Storage tests failed or not implemented${NC}"
go test ./pkg/crypto -v || echo -e "${RED}Crypto tests failed or not implemented${NC}"

# List built binaries
echo -e "${YELLOW}Built binaries:${NC}"
ls -la bin/

echo -e "${GREEN}Build process complete!${NC}"
echo "You can now run the following commands:"
echo "  ./bin/can-node --port 8080 --dimensions 2 --data-dir ./data/node1"
echo "  ./bin/can-cli --node localhost:8080" 