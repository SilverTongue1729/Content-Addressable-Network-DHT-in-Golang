package integrity

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

// IntegrityLevel defines the level of integrity checking
type IntegrityLevel int

const (
	// LevelBasic provides basic integrity checking
	LevelBasic IntegrityLevel = iota
	// LevelStrong provides enhanced integrity with HMAC-SHA256
	LevelStrong
	// LevelComplete provides complete integrity with signatures and timestamps
	LevelComplete
)

// CorruptionSeverity defines the severity of detected corruption
type CorruptionSeverity int

const (
	// SeverityLow represents low-severity corruption (recoverable)
	SeverityLow CorruptionSeverity = iota
	// SeverityMedium represents medium-severity corruption
	SeverityMedium
	// SeverityHigh represents high-severity corruption (critical data)
	SeverityHigh
	// SeverityCritical represents critical corruption (system integrity)
	SeverityCritical
)

// IntegrityData holds integrity verification data
type IntegrityData struct {
	// Hash is the SHA-256 hash of the data
	Hash string `json:"hash"`
	
	// HMAC is the HMAC-SHA256 of the data (if strong integrity is used)
	HMAC string `json:"hmac,omitempty"`
	
	// Timestamp records when the integrity data was created
	Timestamp int64 `json:"timestamp"`
	
	// Version is the version of the integrity data format
	Version int `json:"version"`
}

// IntegrityOptions configures integrity verification
type IntegrityOptions struct {
	// Level specifies the integrity level to use
	Level IntegrityLevel
	
	// KeyRotationInterval defines how often to rotate integrity keys
	KeyRotationInterval time.Duration
	
	// CheckInterval defines how often to run integrity checks
	CheckInterval time.Duration
	
	// BatchSize is the number of items to check in each batch
	BatchSize int
	
	// CriticalKeys are keys that require immediate notification on corruption
	CriticalKeys []string
}

// DefaultIntegrityOptions returns default options
func DefaultIntegrityOptions() IntegrityOptions {
	return IntegrityOptions{
		Level:               LevelStrong,
		KeyRotationInterval: 24 * time.Hour,
		CheckInterval:       1 * time.Hour,
		BatchSize:           100,
	}
}

// ConsistencyResult represents the result of a consistency check
type ConsistencyResult struct {
	// NodeID is the ID of the node that holds this replica
	NodeID string
	
	// IntegrityData is the integrity data for this replica
	IntegrityData IntegrityData
	
	// Timestamp is when the check was performed
	Timestamp time.Time
	
	// IsValid indicates if the integrity check passed
	IsValid bool
	
	// Error is any error that occurred during the check
	Error error
}

// VerifyIntegrity computes and verifies integrity data for the given bytes
func VerifyIntegrity(data []byte, integrityData IntegrityData, key []byte) (bool, error) {
	// Compute SHA-256 hash
	hasher := sha256.New()
	hasher.Write(data)
	computedHash := hex.EncodeToString(hasher.Sum(nil))
	
	// Verify hash
	if computedHash != integrityData.Hash {
		return false, fmt.Errorf("hash mismatch: expected %s, got %s", integrityData.Hash, computedHash)
	}
	
	// For basic integrity, just check hash
	if integrityData.HMAC == "" {
		return true, nil
	}
	
	// For strong integrity, check HMAC
	if len(key) > 0 {
		h := hmac.New(sha256.New, key)
		h.Write(data)
		computedHMAC := hex.EncodeToString(h.Sum(nil))
		
		if computedHMAC != integrityData.HMAC {
			return false, fmt.Errorf("HMAC mismatch: expected %s, got %s", integrityData.HMAC, computedHMAC)
		}
	}
	
	return true, nil
}

// ComputeIntegrityData generates integrity data for the given bytes
func ComputeIntegrityData(data []byte, level IntegrityLevel, key []byte) (IntegrityData, error) {
	integrityData := IntegrityData{
		Timestamp: time.Now().Unix(),
		Version:   1,
	}
	
	// Compute SHA-256 hash
	hasher := sha256.New()
	hasher.Write(data)
	integrityData.Hash = hex.EncodeToString(hasher.Sum(nil))
	
	// For strong or complete integrity, add HMAC
	if level >= LevelStrong && len(key) > 0 {
		h := hmac.New(sha256.New, key)
		h.Write(data)
		integrityData.HMAC = hex.EncodeToString(h.Sum(nil))
	}
	
	return integrityData, nil
}

// SerializeIntegrityData converts integrity data to a byte slice
func SerializeIntegrityData(integrityData IntegrityData) ([]byte, error) {
	return json.Marshal(integrityData)
}

// DeserializeIntegrityData parses integrity data from a byte slice
func DeserializeIntegrityData(data []byte) (IntegrityData, error) {
	var integrityData IntegrityData
	err := json.Unmarshal(data, &integrityData)
	return integrityData, err
}

// IntegrityVerifier defines the interface for integrity verification
type IntegrityVerifier interface {
	// VerifyData checks the integrity of data
	VerifyData(data []byte, integrityData IntegrityData) (bool, error)
	
	// ComputeIntegrityData generates integrity data for data
	ComputeIntegrityData(data []byte) (IntegrityData, error)
	
	// GetIntegrityLevel returns the level of integrity verification
	GetIntegrityLevel() IntegrityLevel
}

// DataWithIntegrity combines data with its integrity information
type DataWithIntegrity struct {
	// Data is the actual data
	Data []byte
	
	// IntegrityData is the integrity verification data
	IntegrityData IntegrityData
}

// SerializeDataWithIntegrity serializes data with its integrity information
func SerializeDataWithIntegrity(data []byte, integrityData IntegrityData) ([]byte, error) {
	combined := struct {
		Data         []byte       `json:"data"`
		IntegrityData IntegrityData `json:"integrity"`
	}{
		Data:         data,
		IntegrityData: integrityData,
	}
	
	return json.Marshal(combined)
}

// DeserializeDataWithIntegrity deserializes data with its integrity information
func DeserializeDataWithIntegrity(combined []byte) ([]byte, IntegrityData, error) {
	var result struct {
		Data         []byte       `json:"data"`
		IntegrityData IntegrityData `json:"integrity"`
	}
	
	err := json.Unmarshal(combined, &result)
	if err != nil {
		return nil, IntegrityData{}, err
	}
	
	return result.Data, result.IntegrityData, nil
} 