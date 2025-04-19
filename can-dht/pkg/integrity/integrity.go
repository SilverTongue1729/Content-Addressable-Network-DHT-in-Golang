package integrity

import (
	"crypto"
)

// calculateChecksum computes a checksum of the data
func calculateChecksum(data []byte) []byte {
	hasher := crypto.SHA256.New()
	hasher.Write(data)
	return hasher.Sum(nil)
} 