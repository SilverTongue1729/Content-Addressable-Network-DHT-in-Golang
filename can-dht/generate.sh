#!/bin/bash

# Create the internal proto directory if it doesn't exist
mkdir -p internal/proto

# Generate the Go code from proto files
protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    proto/*.proto 