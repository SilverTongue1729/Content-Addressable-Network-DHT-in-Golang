package integrity

import (
	"crypto/sha256"
)

// ComputeHash computes a SHA-256 hash of the provided data
func ComputeHash(data []byte) []byte {
	hash := sha256.Sum256(data)
	return hash[:]
}

// VerifyHash verifies that the data matches the provided hash
func VerifyHash(data, hash []byte) bool {
	computedHash := ComputeHash(data)
	
	// Check if the hashes match
	if len(computedHash) != len(hash) {
		return false
	}
	
	for i := 0; i < len(computedHash); i++ {
		if computedHash[i] != hash[i] {
			return false
		}
	}
	
	return true
} 