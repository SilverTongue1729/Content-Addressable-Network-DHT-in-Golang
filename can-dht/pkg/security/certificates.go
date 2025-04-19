package security

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	// Default certificate validity period
	defaultCertValidity = 8760 * time.Hour // 1 year
	defaultKeySize     = 2048
	caCertFilename     = "ca-cert.pem"
	caKeyFilename      = "ca-key.pem"
	nodeCertFilename   = "node-cert.pem"
	nodeKeyFilename    = "node-key.pem"
)

// CertManager handles certificate generation, storage, and loading
type CertManager struct {
	// Path to store certificates
	CertDir string
	
	// CA certificate
	CACert *x509.Certificate
	CAKey  *rsa.PrivateKey
	
	// Node certificate and key
	NodeCert *x509.Certificate
	NodeKey  *rsa.PrivateKey
	
	// TLS configurations
	ServerTLSConfig *tls.Config
	ClientTLSConfig *tls.Config
	
	mu sync.RWMutex
}

// CertConfig holds configuration for certificate generation
type CertConfig struct {
	// Common name for the certificate (typically node ID)
	CommonName string
	
	// Addresses for the SAN extension
	Addresses []string
	
	// Certificate validity period
	Validity time.Duration
	
	// Key size in bits
	KeySize int
}

// DefaultCertConfig returns a default certificate configuration
func DefaultCertConfig(commonName string, addresses []string) CertConfig {
	return CertConfig{
		CommonName: commonName,
		Addresses: addresses,
		Validity:  defaultCertValidity,
		KeySize:   defaultKeySize,
	}
}

// NewCertManager creates a new certificate manager
func NewCertManager(certDir string) (*CertManager, error) {
	// Create directory if it doesn't exist
	if err := os.MkdirAll(certDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create cert directory: %w", err)
	}
	
	cm := &CertManager{
		CertDir: certDir,
	}
	
	return cm, nil
}

// ensureCA ensures that a CA certificate exists, creating it if necessary
func (cm *CertManager) ensureCA() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	
	caKeyPath := filepath.Join(cm.CertDir, caKeyFilename)
	caCertPath := filepath.Join(cm.CertDir, caCertFilename)
	
	// Check if CA files already exist
	if _, err := os.Stat(caCertPath); err == nil {
		if _, err := os.Stat(caKeyPath); err == nil {
			// Files exist, load them
			return cm.loadCA()
		}
	}
	
	// Generate new CA
	return cm.generateCA()
}

// loadCA loads the CA certificate and private key from files
func (cm *CertManager) loadCA() error {
	caKeyPath := filepath.Join(cm.CertDir, caKeyFilename)
	caCertPath := filepath.Join(cm.CertDir, caCertFilename)
	
	// Load CA certificate
	caCertBytes, err := os.ReadFile(caCertPath)
	if err != nil {
		return fmt.Errorf("failed to read CA certificate: %w", err)
	}
	
	caCertBlock, _ := pem.Decode(caCertBytes)
	if caCertBlock == nil {
		return fmt.Errorf("failed to decode CA certificate PEM")
	}
	
	caCert, err := x509.ParseCertificate(caCertBlock.Bytes)
	if err != nil {
		return fmt.Errorf("failed to parse CA certificate: %w", err)
	}
	
	// Load CA key
	caKeyBytes, err := os.ReadFile(caKeyPath)
	if err != nil {
		return fmt.Errorf("failed to read CA key: %w", err)
	}
	
	caKeyBlock, _ := pem.Decode(caKeyBytes)
	if caKeyBlock == nil {
		return fmt.Errorf("failed to decode CA key PEM")
	}
	
	caKey, err := x509.ParsePKCS1PrivateKey(caKeyBlock.Bytes)
	if err != nil {
		return fmt.Errorf("failed to parse CA key: %w", err)
	}
	
	cm.CACert = caCert
	cm.CAKey = caKey
	
	return nil
}

// generateCA creates a new CA certificate and private key
func (cm *CertManager) generateCA() error {
	// Generate a new CA private key
	caKey, err := rsa.GenerateKey(rand.Reader, defaultKeySize)
	if err != nil {
		return fmt.Errorf("failed to generate CA private key: %w", err)
	}
	
	// Prepare self-signed CA certificate template
	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return fmt.Errorf("failed to generate serial number: %w", err)
	}
	
	now := time.Now()
	caTemplate := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName:   "CAN-DHT Root CA",
			Organization: []string{"CAN-DHT"},
		},
		NotBefore:             now,
		NotAfter:              now.Add(defaultCertValidity * 3), // CA valid for 3x node cert duration
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLen:            1,
	}
	
	// Create the CA certificate
	caCertDER, err := x509.CreateCertificate(
		rand.Reader,
		&caTemplate,
		&caTemplate, // Self-signed
		&caKey.PublicKey,
		caKey,
	)
	if err != nil {
		return fmt.Errorf("failed to create CA certificate: %w", err)
	}
	
	// Parse the created certificate to get the x509.Certificate
	caCert, err := x509.ParseCertificate(caCertDER)
	if err != nil {
		return fmt.Errorf("failed to parse created CA certificate: %w", err)
	}
	
	// PEM encode the certificate and key
	caKeyPEM := new(bytes.Buffer)
	pem.Encode(caKeyPEM, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(caKey),
	})
	
	caCertPEM := new(bytes.Buffer)
	pem.Encode(caCertPEM, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caCertDER,
	})
	
	// Save the files
	if err := os.WriteFile(filepath.Join(cm.CertDir, caKeyFilename), caKeyPEM.Bytes(), 0600); err != nil {
		return fmt.Errorf("failed to write CA key file: %w", err)
	}
	
	if err := os.WriteFile(filepath.Join(cm.CertDir, caCertFilename), caCertPEM.Bytes(), 0644); err != nil {
		return fmt.Errorf("failed to write CA cert file: %w", err)
	}
	
	cm.CACert = caCert
	cm.CAKey = caKey
	
	return nil
}

// GenerateNodeCertificate generates a node certificate signed by the CA
func (cm *CertManager) GenerateNodeCertificate(config CertConfig) error {
	// Ensure CA exists
	if err := cm.ensureCA(); err != nil {
		return fmt.Errorf("failed to ensure CA: %w", err)
	}
	
	cm.mu.Lock()
	defer cm.mu.Unlock()
	
	// Generate node private key
	nodeKey, err := rsa.GenerateKey(rand.Reader, config.KeySize)
	if err != nil {
		return fmt.Errorf("failed to generate node private key: %w", err)
	}
	
	// Create certificate template
	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return fmt.Errorf("failed to generate serial number: %w", err)
	}
	
	now := time.Now()
	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName:   config.CommonName,
			Organization: []string{"CAN-DHT Node"},
		},
		NotBefore:             now,
		NotAfter:              now.Add(config.Validity),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  false,
	}
	
	// Add IP and DNS SANs
	for _, addr := range config.Addresses {
		if ip := net.ParseIP(addr); ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
		} else {
			template.DNSNames = append(template.DNSNames, addr)
		}
	}
	
	// Create the certificate
	nodeCertDER, err := x509.CreateCertificate(
		rand.Reader,
		&template,
		cm.CACert,
		&nodeKey.PublicKey,
		cm.CAKey,
	)
	if err != nil {
		return fmt.Errorf("failed to create node certificate: %w", err)
	}
	
	// Parse the created certificate
	nodeCert, err := x509.ParseCertificate(nodeCertDER)
	if err != nil {
		return fmt.Errorf("failed to parse created node certificate: %w", err)
	}
	
	// PEM encode the certificate and key
	nodeKeyPEM := new(bytes.Buffer)
	pem.Encode(nodeKeyPEM, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(nodeKey),
	})
	
	nodeCertPEM := new(bytes.Buffer)
	pem.Encode(nodeCertPEM, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: nodeCertDER,
	})
	
	// Save the files
	if err := os.WriteFile(filepath.Join(cm.CertDir, nodeKeyFilename), nodeKeyPEM.Bytes(), 0600); err != nil {
		return fmt.Errorf("failed to write node key file: %w", err)
	}
	
	if err := os.WriteFile(filepath.Join(cm.CertDir, nodeCertFilename), nodeCertPEM.Bytes(), 0644); err != nil {
		return fmt.Errorf("failed to write node cert file: %w", err)
	}
	
	cm.NodeCert = nodeCert
	cm.NodeKey = nodeKey
	
	return nil
}

// LoadNodeCertificate loads the node certificate and key from disk
func (cm *CertManager) LoadNodeCertificate() error {
	nodeCertPath := filepath.Join(cm.CertDir, nodeCertFilename)
	nodeKeyPath := filepath.Join(cm.CertDir, nodeKeyFilename)
	
	// Check if files exist
	if _, err := os.Stat(nodeCertPath); err != nil {
		return fmt.Errorf("node certificate not found: %w", err)
	}
	if _, err := os.Stat(nodeKeyPath); err != nil {
		return fmt.Errorf("node key not found: %w", err)
	}
	
	// Load node certificate
	nodeCertBytes, err := os.ReadFile(nodeCertPath)
	if err != nil {
		return fmt.Errorf("failed to read node certificate: %w", err)
	}
	
	nodeCertBlock, _ := pem.Decode(nodeCertBytes)
	if nodeCertBlock == nil {
		return fmt.Errorf("failed to decode node certificate PEM")
	}
	
	nodeCert, err := x509.ParseCertificate(nodeCertBlock.Bytes)
	if err != nil {
		return fmt.Errorf("failed to parse node certificate: %w", err)
	}
	
	// Load node key
	nodeKeyBytes, err := os.ReadFile(nodeKeyPath)
	if err != nil {
		return fmt.Errorf("failed to read node key: %w", err)
	}
	
	nodeKeyBlock, _ := pem.Decode(nodeKeyBytes)
	if nodeKeyBlock == nil {
		return fmt.Errorf("failed to decode node key PEM")
	}
	
	nodeKey, err := x509.ParsePKCS1PrivateKey(nodeKeyBlock.Bytes)
	if err != nil {
		return fmt.Errorf("failed to parse node key: %w", err)
	}
	
	cm.mu.Lock()
	cm.NodeCert = nodeCert
	cm.NodeKey = nodeKey
	cm.mu.Unlock()
	
	return nil
}

// SetupTLSConfig prepares the TLS configurations for client and server
func (cm *CertManager) SetupTLSConfig() error {
	// Ensure CA is loaded
	if err := cm.ensureCA(); err != nil {
		return fmt.Errorf("failed to ensure CA: %w", err)
	}
	
	// Ensure node certificate is loaded
	if cm.NodeCert == nil || cm.NodeKey == nil {
		if err := cm.LoadNodeCertificate(); err != nil {
			return fmt.Errorf("failed to load node certificate: %w", err)
		}
	}
	
	cm.mu.Lock()
	defer cm.mu.Unlock()
	
	// Create CA certificate pool
	caPool := x509.NewCertPool()
	caPool.AddCert(cm.CACert)
	
	// Create server TLS certificate
	serverCert := tls.Certificate{
		Certificate: [][]byte{cm.NodeCert.Raw},
		PrivateKey:  cm.NodeKey,
	}
	
	// Create server TLS config
	serverConfig := &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    caPool,
		MinVersion:   tls.VersionTLS12,
	}
	
	// Create client TLS config
	clientConfig := &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		RootCAs:      caPool,
		MinVersion:   tls.VersionTLS12,
	}
	
	cm.ServerTLSConfig = serverConfig
	cm.ClientTLSConfig = clientConfig
	
	return nil
}

// GetServerTLSConfig returns a deep copy of the server TLS config
func (cm *CertManager) GetServerTLSConfig() *tls.Config {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	
	if cm.ServerTLSConfig == nil {
		return nil
	}
	
	// Create a deep copy to avoid concurrent modification issues
	config := cm.ServerTLSConfig.Clone()
	return config
}

// GetClientTLSConfig returns a deep copy of the client TLS config
func (cm *CertManager) GetClientTLSConfig() *tls.Config {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	
	if cm.ClientTLSConfig == nil {
		return nil
	}
	
	// Create a deep copy to avoid concurrent modification issues
	config := cm.ClientTLSConfig.Clone()
	return config
}

// RenewNodeCertificate renews the node certificate before it expires
func (cm *CertManager) RenewNodeCertificate(config CertConfig) error {
	// Check if certificate needs renewal (e.g., if < 30 days left)
	if cm.NodeCert != nil {
		if time.Now().Add(30 * 24 * time.Hour).Before(cm.NodeCert.NotAfter) {
			// No renewal needed yet
			return nil
		}
	}
	
	// Generate new certificate
	return cm.GenerateNodeCertificate(config)
} 