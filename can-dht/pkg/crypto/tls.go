package crypto

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/can-dht/pkg/node"
)

// TLSManager handles certificate generation and TLS configuration
type TLSManager struct {
	CAKey         *rsa.PrivateKey
	CACert        *x509.Certificate
	NodeID        node.NodeID
	CertDirectory string
}

// NewTLSManager creates a new TLS manager for the node
func NewTLSManager(nodeID node.NodeID, certDir string) (*TLSManager, error) {
	// Create certificate directory if it doesn't exist
	if err := os.MkdirAll(certDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create certificate directory: %w", err)
	}

	manager := &TLSManager{
		NodeID:        nodeID,
		CertDirectory: certDir,
	}

	// Check if CA certificate and key exist, create if not
	caKeyPath := filepath.Join(certDir, "ca.key")
	caCertPath := filepath.Join(certDir, "ca.crt")

	if _, err := os.Stat(caKeyPath); os.IsNotExist(err) {
		// Create new CA
		if err := manager.createCA(caKeyPath, caCertPath); err != nil {
			return nil, fmt.Errorf("failed to create CA: %w", err)
		}
	} else {
		// Load existing CA
		if err := manager.loadCA(caKeyPath, caCertPath); err != nil {
			return nil, fmt.Errorf("failed to load CA: %w", err)
		}
	}

	return manager, nil
}

// createCA creates a new Certificate Authority
func (m *TLSManager) createCA(keyPath, certPath string) error {
	// Generate a new key
	caKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return fmt.Errorf("failed to generate CA key: %w", err)
	}
	m.CAKey = caKey

	// Create CA certificate
	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return fmt.Errorf("failed to generate serial number: %w", err)
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"CAN DHT CA"},
			CommonName:   "CAN DHT Root CA",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(10, 0, 0), // 10 years validity
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLen:            1,
	}

	// Self-sign the CA certificate
	caCertBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &caKey.PublicKey, caKey)
	if err != nil {
		return fmt.Errorf("failed to create CA certificate: %w", err)
	}

	// Parse the certificate
	caCert, err := x509.ParseCertificate(caCertBytes)
	if err != nil {
		return fmt.Errorf("failed to parse CA certificate: %w", err)
	}
	m.CACert = caCert

	// Save the CA key
	keyFile, err := os.OpenFile(keyPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("failed to open CA key file: %w", err)
	}
	defer keyFile.Close()

	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(caKey),
	})
	if _, err := keyFile.Write(keyPEM); err != nil {
		return fmt.Errorf("failed to write CA key file: %w", err)
	}

	// Save the CA certificate
	certFile, err := os.OpenFile(certPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to open CA certificate file: %w", err)
	}
	defer certFile.Close()

	certPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caCertBytes,
	})
	if _, err := certFile.Write(certPEM); err != nil {
		return fmt.Errorf("failed to write CA certificate file: %w", err)
	}

	return nil
}

// loadCA loads an existing Certificate Authority
func (m *TLSManager) loadCA(keyPath, certPath string) error {
	// Load CA key
	keyBytes, err := ioutil.ReadFile(keyPath)
	if err != nil {
		return fmt.Errorf("failed to read CA key file: %w", err)
	}

	keyBlock, _ := pem.Decode(keyBytes)
	if keyBlock == nil {
		return fmt.Errorf("failed to decode CA key PEM")
	}

	caKey, err := x509.ParsePKCS1PrivateKey(keyBlock.Bytes)
	if err != nil {
		return fmt.Errorf("failed to parse CA key: %w", err)
	}
	m.CAKey = caKey

	// Load CA certificate
	certBytes, err := ioutil.ReadFile(certPath)
	if err != nil {
		return fmt.Errorf("failed to read CA certificate file: %w", err)
	}

	certBlock, _ := pem.Decode(certBytes)
	if certBlock == nil {
		return fmt.Errorf("failed to decode CA certificate PEM")
	}

	caCert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return fmt.Errorf("failed to parse CA certificate: %w", err)
	}
	m.CACert = caCert

	return nil
}

// GenerateNodeCertificate generates a certificate for a node
func (m *TLSManager) GenerateNodeCertificate(address string) (tls.Certificate, error) {
	nodeKeyPath := filepath.Join(m.CertDirectory, fmt.Sprintf("%s.key", m.NodeID))
	nodeCertPath := filepath.Join(m.CertDirectory, fmt.Sprintf("%s.crt", m.NodeID))

	// Check if certificate already exists
	if _, err := os.Stat(nodeKeyPath); err == nil {
		// Load existing certificate
		return tls.LoadX509KeyPair(nodeCertPath, nodeKeyPath)
	}

	// Generate a new key
	nodeKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("failed to generate node key: %w", err)
	}

	// Create node certificate
	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("failed to generate serial number: %w", err)
	}

	// Extract host and port from address
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		host = address // Use full address if parsing fails
	}

	// Set up Subject Alternative Names
	var ips []net.IP
	var dnsNames []string

	if ip := net.ParseIP(host); ip != nil {
		ips = append(ips, ip)
	} else {
		dnsNames = append(dnsNames, host)
	}

	// Always add localhost
	dnsNames = append(dnsNames, "localhost")
	ips = append(ips, net.ParseIP("127.0.0.1"))
	ips = append(ips, net.ParseIP("::1"))

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"CAN DHT"},
			CommonName:   string(m.NodeID),
		},
		DNSNames:     dnsNames,
		IPAddresses:  ips,
		NotBefore:    time.Now(),
		NotAfter:     time.Now().AddDate(1, 0, 0), // 1 year validity
		SubjectKeyId: []byte(m.NodeID),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}

	// Sign the certificate with the CA
	certBytes, err := x509.CreateCertificate(rand.Reader, &template, m.CACert, &nodeKey.PublicKey, m.CAKey)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("failed to create node certificate: %w", err)
	}

	// Save the node key
	keyFile, err := os.OpenFile(nodeKeyPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("failed to open node key file: %w", err)
	}
	defer keyFile.Close()

	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(nodeKey),
	})
	if _, err := keyFile.Write(keyPEM); err != nil {
		return tls.Certificate{}, fmt.Errorf("failed to write node key file: %w", err)
	}

	// Save the node certificate
	certFile, err := os.OpenFile(nodeCertPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("failed to open node certificate file: %w", err)
	}
	defer certFile.Close()

	certPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certBytes,
	})
	if _, err := certFile.Write(certPEM); err != nil {
		return tls.Certificate{}, fmt.Errorf("failed to write node certificate file: %w", err)
	}

	// Return the certificate
	return tls.Certificate{
		Certificate: [][]byte{certBytes, m.CACert.Raw},
		PrivateKey:  nodeKey,
		Leaf:        &template,
	}, nil
}

// GetTLSConfig returns a TLS configuration for the node
func (m *TLSManager) GetTLSConfig(address string) (*tls.Config, error) {
	// Generate or load node certificate
	cert, err := m.GenerateNodeCertificate(address)
	if err != nil {
		return nil, fmt.Errorf("failed to get node certificate: %w", err)
	}

	// Create CA certificate pool
	caPool := x509.NewCertPool()
	caPool.AddCert(m.CACert)

	// Create TLS config
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    caPool,
		RootCAs:      caPool,
		MinVersion:   tls.VersionTLS12,
	}

	return tlsConfig, nil
} 