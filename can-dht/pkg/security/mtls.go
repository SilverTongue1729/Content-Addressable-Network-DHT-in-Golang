package security

import (
	"context"
	"fmt"
	"time"
	"log"
	
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// TLSManager provides TLS functionality for gRPC connections
type TLSManager struct {
	CertManager *CertManager
}

// NewTLSManager creates a new TLS manager
func NewTLSManager(certDir string) (*TLSManager, error) {
	certManager, err := NewCertManager(certDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create certificate manager: %w", err)
	}
	
	return &TLSManager{
		CertManager: certManager,
	}, nil
}

// SetupCertificates initializes the certificates for this node
func (tm *TLSManager) SetupCertificates(nodeID string, addresses []string) error {
	// Generate or load CA
	if err := tm.CertManager.ensureCA(); err != nil {
		return fmt.Errorf("failed to ensure CA: %w", err)
	}
	
	// Look for existing node certificate
	err := tm.CertManager.LoadNodeCertificate()
	if err != nil {
		// Generate a new certificate
		log.Printf("Generating new node certificate for node %s", nodeID)
		config := DefaultCertConfig(nodeID, addresses)
		if err := tm.CertManager.GenerateNodeCertificate(config); err != nil {
			return fmt.Errorf("failed to generate node certificate: %w", err)
		}
	} else {
		// Check if certificate needs renewal
		config := DefaultCertConfig(nodeID, addresses)
		if err := tm.CertManager.RenewNodeCertificate(config); err != nil {
			log.Printf("Warning: failed to renew node certificate: %v", err)
		}
	}
	
	// Setup TLS configuration
	if err := tm.CertManager.SetupTLSConfig(); err != nil {
		return fmt.Errorf("failed to setup TLS config: %w", err)
	}
	
	return nil
}

// GetGRPCDialOptions returns the gRPC dial options for TLS
func (tm *TLSManager) GetGRPCDialOptions() []grpc.DialOption {
	clientTLSConfig := tm.CertManager.GetClientTLSConfig()
	if clientTLSConfig == nil {
		log.Printf("Warning: no TLS config available, using insecure connection")
		return []grpc.DialOption{grpc.WithInsecure()}
	}
	
	creds := credentials.NewTLS(clientTLSConfig)
	return []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithBlock(),
		grpc.WithTimeout(5 * time.Second),
	}
}

// GetGRPCServerOptions returns the gRPC server options for TLS
func (tm *TLSManager) GetGRPCServerOptions() []grpc.ServerOption {
	serverTLSConfig := tm.CertManager.GetServerTLSConfig()
	if serverTLSConfig == nil {
		log.Printf("Warning: no TLS config available, using insecure server")
		return []grpc.ServerOption{}
	}
	
	creds := credentials.NewTLS(serverTLSConfig)
	return []grpc.ServerOption{
		grpc.Creds(creds),
	}
}

// CloneTLSManagerWithContext creates a copy of the TLS manager with request context
func (tm *TLSManager) CloneTLSManagerWithContext(ctx context.Context) *TLSManager {
	return &TLSManager{
		CertManager: tm.CertManager,
	}
}

// StartCertificateRenewalTask starts a background task for certificate renewal
func (tm *TLSManager) StartCertificateRenewalTask(ctx context.Context, nodeID string, addresses []string) {
	go func() {
		ticker := time.NewTicker(24 * time.Hour) // Check daily
		defer ticker.Stop()
		
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				config := DefaultCertConfig(nodeID, addresses)
				err := tm.CertManager.RenewNodeCertificate(config)
				if err != nil {
					log.Printf("Failed to renew node certificate: %v", err)
				}
			}
		}
	}()
} 