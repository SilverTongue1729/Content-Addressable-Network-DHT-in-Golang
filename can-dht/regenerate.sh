#!/bin/bash

# This script regenerates the protocol buffer code for the CAN DHT

# Ensure we have the required protoc plugins
echo "Checking for required plugins..."
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Create the internal proto directory if it doesn't exist
mkdir -p internal/proto

# Generate the Go code from proto files
echo "Generating protocol buffer code..."
protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    proto/can.proto 

# Check if generation was successful
if [ $? -eq 0 ]; then
    echo "Protocol buffer code generated successfully"
else
    echo "Failed to generate protocol buffer code"
    exit 1
fi

# Make the script executable
chmod +x generate.sh

echo "Done!" 