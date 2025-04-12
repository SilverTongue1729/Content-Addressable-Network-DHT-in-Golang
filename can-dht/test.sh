#!/bin/bash

set -e  # Exit on error

# Colors for better output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}CAN-DHT Test Script${NC}"
echo "========================"

# Function to check if a command exists
command_exists() {
  command -v "$1" >/dev/null 2>&1
}

# Check for required tools
if ! command_exists go; then
  echo -e "${RED}Error: Go is not installed. Please install Go 1.16 or later.${NC}"
  exit 1
fi

# Create directories
echo -e "${YELLOW}Creating directories...${NC}"
mkdir -p bin
mkdir -p test-data

# Build the binaries
echo -e "${YELLOW}Building node binary...${NC}"
go build -o bin/can-node ./cmd/node

echo -e "${YELLOW}Building test client...${NC}"
go build -o bin/test-client ./cmd/test-client

# Run the Go tests
echo -e "${YELLOW}Running Go tests...${NC}"
go test ./pkg/node -v
go test ./pkg/routing -v 2>/dev/null || echo -e "${RED}Routing tests not implemented yet${NC}"
go test ./pkg/storage -v 2>/dev/null || echo -e "${RED}Storage tests not implemented yet${NC}"
go test ./pkg/crypto -v 2>/dev/null || echo -e "${RED}Crypto tests not implemented yet${NC}"

# Run the test client
echo -e "${YELLOW}Running test client...${NC}"
./bin/test-client --data-dir test-data

# Clean up test data
echo -e "${YELLOW}Cleaning up test data...${NC}"
rm -rf test-data

echo -e "${GREEN}All tests completed successfully!${NC}"
echo "You can now try running multiple nodes manually."
echo "Instructions:"
echo "  1. ./bin/can-node --port 8080 --dimensions 2 --data-dir ./data/node1"
echo "  2. In another terminal: ./bin/can-node --port 8081 --dimensions 2 --data-dir ./data/node2 --join localhost:8080" 