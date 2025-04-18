#!/bin/bash

# Run script for CAN DHT Visualization Tool

echo "Starting CAN DHT Visualization Tool..."

# Set the directory to the project root
cd "$(dirname "$0")/../.." || exit

# Make sure Go modules are up to date
go mod tidy

# Build and run the visualization tool
go run cmd/visualization/main.go

echo "Visualization tool stopped." 