package main

import (
	"fmt"
	"log"

	"github.com/can-dht/pkg/crypto"
	"github.com/can-dht/pkg/node"
)

func main() {
	fmt.Println("CAN-DHT Security Demonstration")
	fmt.Println("===============================")

	// Create a simple node
	nodeID := node.NodeID("demo-node")
	address := "localhost:8080"

	// Create zone
	dimensions := 2
	min := make(node.Point, dimensions)
	max := make(node.Point, dimensions)
	for i := 0; i < dimensions; i++ {
		min[i] = 0.0
		max[i] = 1.0
	}

	zone, err := node.NewZone(min, max)
	if err != nil {
		log.Fatalf("Failed to create zone: %v", err)
	}

	// Create node
	demoNode := node.NewNode(nodeID, address, zone, dimensions)

	// Create key manager for encryption
	keyManager, err := crypto.NewKeyManager()
	if err != nil {
		log.Fatalf("Failed to create key manager: %v", err)
	}

	fmt.Println("\n1. Storing plaintext (unencrypted) data:")
	fmt.Println("----------------------------------------")

	// Store some plaintext data
	key1 := "user123"
	value1 := "John Doe's personal information"
	demoNode.Put(key1, value1)

	// Retrieve and display plaintext data
	retrievedValue1, found := demoNode.Get(key1)
	if !found {
		log.Fatalf("Failed to retrieve value for key: %s", key1)
	}

	fmt.Printf("Original value: %s\n", value1)
	fmt.Printf("Retrieved value: %s\n", retrievedValue1)
	fmt.Println("Notice that the data is stored as plaintext, which is not secure.")

	fmt.Println("\n2. Storing encrypted data:")
	fmt.Println("-------------------------")

	// Store some encrypted data
	key2 := "user456"
	value2 := "Jane Smith's sensitive information"

	// Encrypt the data
	secureData, err := keyManager.EncryptAndAuthenticate([]byte(value2))
	if err != nil {
		log.Fatalf("Failed to encrypt data: %v", err)
	}

	// Serialize the secure data
	serializedData := crypto.SerializeSecureData(secureData)

	// Store the encrypted data
	demoNode.Put(key2, serializedData)

	// Retrieve and display encrypted data
	retrievedSerializedData, found := demoNode.Get(key2)
	if !found {
		log.Fatalf("Failed to retrieve value for key: %s", key2)
	}

	fmt.Printf("Original value: %s\n", value2)
	fmt.Printf("Stored (encrypted) value: %s\n", retrievedSerializedData)

	// Decrypt and display
	retrievedSecureData, err := crypto.DeserializeSecureData(retrievedSerializedData)
	if err != nil {
		log.Fatalf("Failed to deserialize secure data: %v", err)
	}

	decryptedData, err := keyManager.DecryptAndVerify(retrievedSecureData)
	if err != nil {
		log.Fatalf("Failed to decrypt data: %v", err)
	}

	fmt.Printf("Decrypted value: %s\n", string(decryptedData))

	fmt.Println("\n3. Data Tampering Detection:")
	fmt.Println("---------------------------")

	// Store another piece of encrypted data
	key3 := "user789"
	value3 := "Very sensitive financial information"

	// Encrypt the data
	secureData3, err := keyManager.EncryptAndAuthenticate([]byte(value3))
	if err != nil {
		log.Fatalf("Failed to encrypt data: %v", err)
	}

	// Serialize the secure data
	serializedData3 := crypto.SerializeSecureData(secureData3)

	// Store the encrypted data
	demoNode.Put(key3, serializedData3)

	fmt.Printf("Original value: %s\n", value3)
	fmt.Printf("Stored (encrypted) value: %s\n", serializedData3)

	// Simulate tampering - store a modified version of the data
	tamperedData := serializedData3[:len(serializedData3)-10] + "TAMPERED" + serializedData3[len(serializedData3)-5:]
	demoNode.Put(key3, tamperedData)

	fmt.Printf("Tampered value: %s\n", tamperedData)

	// Try to retrieve and decrypt the tampered data
	retrievedTamperedData, found := demoNode.Get(key3)
	if !found {
		log.Fatalf("Failed to retrieve value for key: %s", key3)
	}

	retrievedSecureData3, err := crypto.DeserializeSecureData(retrievedTamperedData)
	if err != nil {
		fmt.Printf("Deserialization failed (tampering detected): %v\n", err)
	} else {
		decryptedData3, err := keyManager.DecryptAndVerify(retrievedSecureData3)
		if err != nil {
			fmt.Printf("Decryption failed (tampering detected): %v\n", err)
		} else {
			fmt.Printf("Decrypted value: %s\n", string(decryptedData3))
		}
	}

	fmt.Println("\n4. Using Different Keys:")
	fmt.Println("----------------------")

	// Create a different key manager
	differentKeyManager, err := crypto.NewKeyManager()
	if err != nil {
		log.Fatalf("Failed to create different key manager: %v", err)
	}

	// Try to decrypt with a different key
	key4 := "user101"
	value4 := "Data encrypted with the first key"

	// Encrypt with the first key manager
	secureData4, err := keyManager.EncryptAndAuthenticate([]byte(value4))
	if err != nil {
		log.Fatalf("Failed to encrypt data: %v", err)
	}

	// Serialize and store
	serializedData4 := crypto.SerializeSecureData(secureData4)
	demoNode.Put(key4, serializedData4)

	// Retrieve
	retrievedSerializedData4, found := demoNode.Get(key4)
	if !found {
		log.Fatalf("Failed to retrieve value for key: %s", key4)
	}

	retrievedSecureData4, err := crypto.DeserializeSecureData(retrievedSerializedData4)
	if err != nil {
		log.Fatalf("Failed to deserialize secure data: %v", err)
	}

	// Try to decrypt with the different key manager
	fmt.Printf("Original value: %s\n", value4)
	_, err = differentKeyManager.DecryptAndVerify(retrievedSecureData4)
	if err != nil {
		fmt.Printf("Decryption with different key failed (as expected): %v\n", err)
	}

	// Decrypt with the original key
	decryptedData4, err := keyManager.DecryptAndVerify(retrievedSecureData4)
	if err != nil {
		log.Fatalf("Failed to decrypt with original key: %v", err)
	}

	fmt.Printf("Decrypted with original key: %s\n", string(decryptedData4))

	fmt.Println("\nSecurity Demo Complete")
}
