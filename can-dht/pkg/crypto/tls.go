package crypto

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
)

// TLSConfig contains paths to certificates and keys
type TLSConfig struct {
	CertFile   string // Path to certificate file
	KeyFile    string // Path to key file
	CAFile     string // Path to CA certificate file
	ServerName string // Expected server name for verification
}

// DefaultTLSConfig returns a default TLS configuration
func DefaultTLSConfig() TLSConfig {
	return TLSConfig{
		CertFile:   "certs/node.crt",
		KeyFile:    "certs/node.key",
		CAFile:     "certs/ca.crt",
		ServerName: "can-dht-node",
	}
}

// LoadClientTLSCredentials loads client TLS credentials for mTLS
func LoadClientTLSCredentials(config TLSConfig) (*tls.Config, error) {
	// Load certificate of the CA who signed server's certificate
	pemServerCA, err := ioutil.ReadFile(config.CAFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA certificate: %w", err)
	}

	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(pemServerCA) {
		return nil, fmt.Errorf("failed to add server CA's certificate")
	}

	// Load client's certificate and private key
	clientCert, err := tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load client certificate and key: %w", err)
	}

	// Create the credentials and return
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{clientCert},
		RootCAs:      certPool,
		ServerName:   config.ServerName,
	}

	return tlsConfig, nil
}

// LoadServerTLSCredentials loads server TLS credentials for mTLS
func LoadServerTLSCredentials(config TLSConfig) (*tls.Config, error) {
	// Load certificate of the CA who signed client's certificates
	pemClientCA, err := ioutil.ReadFile(config.CAFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA certificate: %w", err)
	}

	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(pemClientCA) {
		return nil, fmt.Errorf("failed to add client CA's certificate")
	}

	// Load server's certificate and private key
	serverCert, err := tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load server certificate and key: %w", err)
	}

	// Create the credentials and return
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    certPool,
	}

	return tlsConfig, nil
}

// GenerateCertificates generates self-signed certificates for testing purposes
// In production, you'd use properly issued certificates
func GenerateSelfSignedCerts(baseDir string) error {
	certsDir := filepath.Join(baseDir, "certs")
	
	// Create directory if it doesn't exist
	if err := os.MkdirAll(certsDir, 0755); err != nil {
		return fmt.Errorf("failed to create certificates directory: %w", err)
	}
	
	// For a real implementation, you would generate proper certificates here
	// This is a placeholder that would need to be replaced with actual certificate generation
	
	// For development, you can use openssl commands:
	// 1. Generate CA key: openssl genrsa -out ca.key 2048
	// 2. Generate CA cert: openssl req -new -x509 -key ca.key -out ca.crt
	// 3. Generate server key: openssl genrsa -out server.key 2048
	// 4. Generate server CSR: openssl req -new -key server.key -out server.csr
	// 5. Sign server cert: openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt
	
	return nil
} 