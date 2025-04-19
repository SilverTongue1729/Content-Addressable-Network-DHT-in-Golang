#!/bin/bash

# Script to regenerate protobuf files
echo "Regenerating protocol buffer files..."

# Make sure the output directory exists
mkdir -p internal/proto

# Generate Go code from proto files
protoc --proto_path=internal/proto \
       --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       internal/proto/service.proto

echo "Protocol buffer files regenerated." 