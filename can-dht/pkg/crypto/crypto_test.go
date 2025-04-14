package crypto

import (
	"bytes"
	"testing"
)

func TestBasicEncryptionAndDecryption(t *testing.T) {
	// Create a key manager
	keyManager, err := NewKeyManager()
	if err != nil {
		t.Fatalf("Failed to create key manager: %v", err)
	}

	// Test data
	originalData := []byte("This is sensitive data that needs to be encrypted")

	// Encrypt the data
	ciphertext, err := keyManager.EncryptWithAESGCM(originalData)
	if err != nil {
		t.Fatalf("Encryption failed: %v", err)
	}

	// Make sure the ciphertext is different from the plaintext
	if bytes.Equal(ciphertext, originalData) {
		t.Error("Ciphertext should be different from plaintext")
	}

	// Decrypt the data
	decryptedData, err := keyManager.DecryptWithAESGCM(ciphertext)
	if err != nil {
		t.Fatalf("Decryption failed: %v", err)
	}

	// Verify the decrypted data matches the original
	if !bytes.Equal(decryptedData, originalData) {
		t.Errorf("Decrypted data doesn't match original. Expected %q, got %q", originalData, decryptedData)
	}
}

func TestHMACOperations(t *testing.T) {
	// Create a key manager
	keyManager, err := NewKeyManager()
	if err != nil {
		t.Fatalf("Failed to create key manager: %v", err)
	}

	// Test data
	data := []byte("This data will be authenticated with HMAC")

	// Generate HMAC
	hmac := keyManager.GenerateHMAC(data)
	if hmac == nil || len(hmac) == 0 {
		t.Fatal("HMAC generation failed")
	}

	// Verify valid HMAC
	if !keyManager.VerifyHMAC(data, hmac) {
		t.Error("HMAC verification failed for valid data")
	}

	// Verify HMAC detects tampering
	tamperedData := append([]byte{}, data...)
	tamperedData[0] = tamperedData[0] ^ 0xFF // Flip some bits
	if keyManager.VerifyHMAC(tamperedData, hmac) {
		t.Error("HMAC verification should fail for tampered data")
	}
}

func TestSecureDataOperations(t *testing.T) {
	// Create a key manager
	keyManager, err := NewKeyManager()
	if err != nil {
		t.Fatalf("Failed to create key manager: %v", err)
	}

	// Test data
	originalData := []byte("This data will be encrypted and authenticated")

	// Encrypt and authenticate
	secureData, err := keyManager.EncryptAndAuthenticate(originalData)
	if err != nil {
		t.Fatalf("Encryption and authentication failed: %v", err)
	}

	// Serialize to string
	serialized := SerializeSecureData(secureData)
	if serialized == "" {
		t.Fatal("Serialization failed")
	}

	// Deserialize from string
	deserializedData, err := DeserializeSecureData(serialized)
	if err != nil {
		t.Fatalf("Deserialization failed: %v", err)
	}

	// Verify ciphertext and HMAC were properly deserialized
	if !bytes.Equal(deserializedData.Ciphertext, secureData.Ciphertext) {
		t.Error("Deserialized ciphertext doesn't match original")
	}
	if !bytes.Equal(deserializedData.HMAC, secureData.HMAC) {
		t.Error("Deserialized HMAC doesn't match original")
	}

	// Decrypt and verify
	decryptedData, err := keyManager.DecryptAndVerify(deserializedData)
	if err != nil {
		t.Fatalf("Decryption and verification failed: %v", err)
	}

	// Verify the decrypted data matches the original
	if !bytes.Equal(decryptedData, originalData) {
		t.Errorf("Decrypted data doesn't match original. Expected %q, got %q", originalData, decryptedData)
	}

	// Test tampering detection
	tamperedData := &SecureData{
		Ciphertext: append([]byte{}, deserializedData.Ciphertext...),
		HMAC:       deserializedData.HMAC,
	}
	tamperedData.Ciphertext[0] = tamperedData.Ciphertext[0] ^ 0xFF // Tamper with ciphertext

	// Decryption should fail for tampered data
	_, err = keyManager.DecryptAndVerify(tamperedData)
	if err == nil {
		t.Error("Decryption should fail for tampered data")
	}
}
