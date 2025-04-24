package integrity

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/can-dht/pkg/crypto"
)

// StorageInterface defines methods needed by integrity checking
type StorageInterface interface {
	// GetAllKeys returns all keys in the storage
	GetAllKeys() ([]string, error)
	
	// Get retrieves a value by key
	Get(key string) ([]byte, bool, error)
	
	// Put stores a value with a key
	Put(key string, value []byte) error
}

// IntegrityData wraps data with integrity verification information
type IntegrityData struct {
	// Value is the actual data
	Value []byte
	
	// HMAC is the integrity hash
	HMAC []byte
	
	// LastVerified is when this data was last verified
	LastVerified time.Time
}

// NOTE: PeriodicChecker has been moved to checker.go
// Please use the implementation from checker.go instead

// Helper functions for integrity data operations

// PrepareDataWithIntegrity adds integrity information to data
func PrepareDataWithIntegrity(value []byte, keyManager *crypto.KeyManager) ([]byte, error) {
	if keyManager == nil {
		return nil, fmt.Errorf("key manager is required for integrity")
	}
	
	// Create integrity data structure
	data := &IntegrityData{
		Value:        value,
		LastVerified: time.Now(),
	}
	
	// Generate HMAC using the integrity key
	h := hmac.New(sha256.New, keyManager.GetIntegrityKey())
	h.Write(value)
	data.HMAC = h.Sum(nil)
	
	// Serialize the structure
	return serializeIntegrityData(data)
}

// VerifyDataIntegrity verifies integrity of data and returns the original value
func VerifyDataIntegrity(data []byte, keyManager *crypto.KeyManager) ([]byte, error) {
	if keyManager == nil {
		return nil, fmt.Errorf("key manager is required for integrity verification")
	}
	
	// Parse integrity data
	integrityData, err := parseIntegrityData(data)
	if err != nil {
		return nil, fmt.Errorf("error parsing integrity data: %w", err)
	}
	
	// Verify HMAC
	h := hmac.New(sha256.New, keyManager.GetIntegrityKey())
	h.Write(integrityData.Value)
	expectedHMAC := h.Sum(nil)
	
	if !bytesEqual(expectedHMAC, integrityData.HMAC) {
		return nil, fmt.Errorf("integrity verification failed: HMAC mismatch")
	}
	
	// Update verification time
	integrityData.LastVerified = time.Now()
	
	return integrityData.Value, nil
}

// parseIntegrityData deserializes integrity data from bytes
func parseIntegrityData(data []byte) (*IntegrityData, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("data too short to be valid integrity data")
	}
	
	valueLen := int(binary.LittleEndian.Uint32(data[0:4]))
	hmacLen := int(binary.LittleEndian.Uint32(data[4:8]))
	
	if len(data) < 8+valueLen+hmacLen+8 {
		return nil, fmt.Errorf("data truncated or corrupted")
	}
	
	valueData := data[8 : 8+valueLen]
	hmacData := data[8+valueLen : 8+valueLen+hmacLen]
	timestampData := data[8+valueLen+hmacLen : 8+valueLen+hmacLen+8]
	
	timestamp := bytesToInt64(timestampData)
	
	return &IntegrityData{
		Value:        valueData,
		HMAC:         hmacData,
		LastVerified: time.Unix(timestamp, 0),
	}, nil
}

// serializeIntegrityData serializes integrity data to bytes
func serializeIntegrityData(data *IntegrityData) ([]byte, error) {
	valueLen := len(data.Value)
	hmacLen := len(data.HMAC)
	
	// Format: [valueLen(4)][hmacLen(4)][value(valueLen)][hmac(hmacLen)][timestamp(8)]
	result := make([]byte, 8+valueLen+hmacLen+8)
	
	binary.LittleEndian.PutUint32(result[0:4], uint32(valueLen))
	binary.LittleEndian.PutUint32(result[4:8], uint32(hmacLen))
	
	copy(result[8:8+valueLen], data.Value)
	copy(result[8+valueLen:8+valueLen+hmacLen], data.HMAC)
	
	timestampBytes := int64ToBytes(data.LastVerified.Unix())
	copy(result[8+valueLen+hmacLen:], timestampBytes)
	
	return result, nil
}

// int64ToBytes converts int64 to byte slice
func int64ToBytes(val int64) []byte {
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, uint64(val))
	return bytes
}

// bytesToInt64 converts byte slice to int64
func bytesToInt64(data []byte) int64 {
	return int64(binary.LittleEndian.Uint64(data))
}

// bytesEqual compares two byte slices for equality
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	
	return true
} 