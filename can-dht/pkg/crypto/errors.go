package crypto

import (
	"errors"
	"fmt"
)

// Predefined encryption errors
var (
	// ErrEncryption is returned when encryption fails
	ErrEncryption = errors.New("encryption failed")
	
	// ErrDecryption is returned when decryption fails
	ErrDecryption = errors.New("decryption failed")
	
	// ErrIntegrityFailed is returned when integrity verification fails
	ErrIntegrityFailed = errors.New("integrity verification failed")
	
	// ErrInvalidKey is returned when an invalid key is used
	ErrInvalidKey = errors.New("invalid encryption key")
	
	// ErrKeyRotationFailed is returned when key rotation fails
	ErrKeyRotationFailed = errors.New("key rotation failed")
	
	// ErrKeyManagement is returned when there's a key management issue
	ErrKeyManagement = errors.New("key management error")
)

// EncryptionError represents an error that occurred during encryption or decryption
type EncryptionError struct {
	// Operation is the operation that failed (e.g., "encrypt", "decrypt", "verify")
	Operation string
	
	// Key is a description of the key being used (e.g., "AES-256", "version 3")
	Key string
	
	// Cause is the underlying error
	Cause error
}

// Error implements the error interface
func (e *EncryptionError) Error() string {
	msg := fmt.Sprintf("encryption error during %s", e.Operation)
	if e.Key != "" {
		msg += fmt.Sprintf(" with key %s", e.Key)
	}
	if e.Cause != nil {
		msg += fmt.Sprintf(": %v", e.Cause)
	}
	return msg
}

// Unwrap returns the underlying error
func (e *EncryptionError) Unwrap() error {
	return e.Cause
}

// Is implements errors.Is
func (e *EncryptionError) Is(target error) bool {
	switch target {
	case ErrEncryption:
		return e.Operation == "encrypt"
	case ErrDecryption:
		return e.Operation == "decrypt"
	case ErrIntegrityFailed:
		return e.Operation == "verify"
	case ErrInvalidKey:
		return e.Cause != nil && errors.Is(e.Cause, ErrInvalidKey)
	case ErrKeyRotationFailed:
		return e.Operation == "rotate"
	case ErrKeyManagement:
		return true // All EncryptionErrors are key management errors
	}
	return false
}

// NewEncryptionError creates a new EncryptionError
func NewEncryptionError(operation string, key string, cause error) error {
	return &EncryptionError{
		Operation: operation,
		Key:       key,
		Cause:     cause,
	}
}

// IsEncryptionError checks if an error is an EncryptionError
func IsEncryptionError(err error) bool {
	var encErr *EncryptionError
	return errors.As(err, &encErr)
}

// IsIntegrityError checks if an error is an integrity verification error
func IsIntegrityError(err error) bool {
	return errors.Is(err, ErrIntegrityFailed)
}

// IsKeyRotationError checks if an error is a key rotation error
func IsKeyRotationError(err error) bool {
	return errors.Is(err, ErrKeyRotationFailed)
} 