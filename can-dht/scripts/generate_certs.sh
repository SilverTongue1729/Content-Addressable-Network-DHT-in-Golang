#!/bin/bash

# Script to generate self-signed certificates for testing mTLS in CAN-DHT

# Create directories
mkdir -p certs
cd certs

# Generate CA key and certificate
echo "Generating CA key and certificate..."
openssl genrsa -out ca.key 2048
openssl req -new -x509 -key ca.key -out ca.crt -days 365 -subj "/CN=CAN-DHT-CA/O=CAN-DHT"

# Generate node key and CSR
echo "Generating node key and CSR..."
openssl genrsa -out node.key 2048
openssl req -new -key node.key -out node.csr -subj "/CN=can-dht-node/O=CAN-DHT"

# Sign the node certificate
echo "Signing node certificate with CA..."
openssl x509 -req -in node.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out node.crt -days 365

# Clean up CSR
rm node.csr

echo "Certificates generated successfully in the 'certs' directory:"
ls -la

echo ""
echo "Use the following command to start a node with mTLS:"
echo "go run cmd/node/main.go --mtls=true"
echo ""

cd .. 