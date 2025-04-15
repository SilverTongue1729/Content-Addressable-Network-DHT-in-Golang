package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/can-dht/pkg/crypto"
	"github.com/can-dht/pkg/integrity"
	"github.com/can-dht/pkg/storage"
)

// Helper function to print a separator line
func printSection(title string) {
	fmt.Println("\n" + title)
	fmt.Println(string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}))
}

func main() {
	fmt.Println("CAN-DHT Periodic Integrity Checks Demonstration")
	fmt.Println("===============================================")
	fmt.Println("This demo shows how periodic integrity checks work to detect and repair data corruption.")

	// Create a temporary directory for the demo
	tempDir, err := os.MkdirTemp("", "integrity-demo")
	if err != nil {
		log.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	printSection("1. Setting Up Storage and Encryption")

	// Create primary data store
	storeOpts := storage.DefaultStoreOptions()
	storeOpts.DataDir = tempDir + "/primary"
	primaryStore, err := storage.NewStore(storeOpts)
	if err != nil {
		log.Fatalf("Failed to create primary store: %v", err)
	}
	defer primaryStore.Close()

	// Create replica store for data recovery
	replicaOpts := storage.DefaultStoreOptions()
	replicaOpts.DataDir = tempDir + "/replica"
	replicaStore, err := storage.NewStore(replicaOpts)
	if err != nil {
		log.Fatalf("Failed to create replica store: %v", err)
	}
	defer replicaStore.Close()

	// Create key manager for encryption
	keyManager, err := crypto.NewKeyManager()
	if err != nil {
		log.Fatalf("Failed to create key manager: %v", err)
	}

	fmt.Println("Created primary data store at:", storeOpts.DataDir)
	fmt.Println("Created replica data store at:", replicaOpts.DataDir)
	fmt.Println("Created encryption key manager")

	printSection("2. Storing Data with Integrity Protection")

	// Store some data with integrity protection
	keys := []string{
		"user1:profile",
		"user2:profile",
		"shared:document",
		"system:config",
		"app:settings",
	}

	values := []string{
		"Alice's personal information",
		"Bob's personal information",
		"Important shared document content",
		"System configuration data",
		"Application settings",
	}

	fmt.Println("Storing initial data with integrity protection:")
	for i, key := range keys {
		// Encrypt the data
		secureData, err := keyManager.EncryptAndAuthenticate([]byte(values[i]))
		if err != nil {
			log.Fatalf("Failed to encrypt data for key %s: %v", key, err)
		}

		// Serialize the secure data
		serialized := crypto.SerializeSecureData(secureData)

		// Store in primary and replica
		err = primaryStore.Put(key, []byte(serialized))
		if err != nil {
			log.Fatalf("Failed to store data in primary for key %s: %v", key, err)
		}

		err = replicaStore.Put(key, []byte(serialized))
		if err != nil {
			log.Fatalf("Failed to store data in replica for key %s: %v", key, err)
		}

		fmt.Printf("- Stored key: %s (value: %s)\n", key, values[i])
	}

	printSection("3. Setting Up Periodic Integrity Checks")

	// For demo purposes, use a short interval
	checkInterval := 5 * time.Second

	// Create the integrity checker
	checker := integrity.NewPeriodicChecker(primaryStore, keyManager, checkInterval)

	// Add the replica store for recovery
	checker.AddReplica(replicaStore)

	// Custom handler for corruption events
	checker.OnCorruptionFound = func(key string, result *integrity.CheckResult) {
		fmt.Printf("\n[ALERT] Corruption detected for key: %s\n", key)
		fmt.Printf("  Error: %v\n", result.Error)
		if result.RepairedOK {
			fmt.Printf("  Status: Successfully repaired from replica\n")

			// Immediately retrieve, decrypt and display the repaired data
			data, exists, err := primaryStore.Get(key)
			if err == nil && exists {
				secureData, err := crypto.DeserializeSecureData(string(data))
				if err == nil {
					plaintext, err := keyManager.DecryptAndVerify(secureData)
					if err == nil {
						fmt.Printf("  Recovered content: %s\n", string(plaintext))
					}
				}
			}
		} else {
			fmt.Printf("  Status: Could not repair (no valid replica found)\n")
		}
	}

	// Custom handler for check completion
	checker.OnCheckCompleted = func(stats *integrity.IntegrityStats) {
		fmt.Printf("\n[INFO] Integrity check completed at %s\n", stats.LastCheckTime.Format(time.RFC3339))
		fmt.Printf("  Checked: %d keys\n", stats.TotalChecks)
		fmt.Printf("  Corrupted: %d keys\n", stats.CorruptedData)
		fmt.Printf("  Repaired: %d keys\n", stats.RepairedData)
		fmt.Printf("  Unrepaired: %d keys\n", stats.UnrepairedData)
	}

	fmt.Printf("Set up integrity checker with %.0f second interval\n", checkInterval.Seconds())
	fmt.Println("Added replica store for data recovery")

	printSection("4. Initial Integrity Check (All Data Should Pass)")

	// Run an initial check manually - everything should be fine
	checker.RunManualCheck()

	// Wait for check to complete
	time.Sleep(1 * time.Second)

	printSection("5. Simulating Data Corruption")

	// Simulate corruption in primary store by modifying bytes in stored data
	corruptedKey := keys[2] // "shared:document"
	corruptedData, exists, err := primaryStore.Get(corruptedKey)
	if err != nil || !exists {
		log.Fatalf("Failed to get data for corruption: %v", err)
	}

	// Corrupt the data by modifying some bytes
	if len(corruptedData) > 10 {
		// Corrupt the middle portion of the data
		startIdx := len(corruptedData) / 2
		corruptedData[startIdx] = corruptedData[startIdx] ^ 0xFF // Flip bits
		corruptedData[startIdx+1] = corruptedData[startIdx+1] ^ 0xFF
		corruptedData[startIdx+2] = corruptedData[startIdx+2] ^ 0xFF

		err = primaryStore.Put(corruptedKey, corruptedData)
		if err != nil {
			log.Fatalf("Failed to write corrupted data: %v", err)
		}
		fmt.Printf("Corrupted data for key: %s (flipped bits in stored data)\n", corruptedKey)
	}

	// Simulate silent corruption with no replica backup
	corrupted2Key := keys[4] // "app:settings"
	corrupted2Data, exists, err := primaryStore.Get(corrupted2Key)
	if err != nil || !exists {
		log.Fatalf("Failed to get data for second corruption: %v", err)
	}

	// Corrupt both primary and replica
	if len(corrupted2Data) > 10 {
		startIdx := len(corrupted2Data) / 3
		corrupted2Data[startIdx] = corrupted2Data[startIdx] ^ 0xAA

		err = primaryStore.Put(corrupted2Key, corrupted2Data)
		if err != nil {
			log.Fatalf("Failed to write second corrupted data: %v", err)
		}

		// Also corrupt the replica to simulate complete data loss
		err = replicaStore.Put(corrupted2Key, corrupted2Data)
		if err != nil {
			log.Fatalf("Failed to corrupt replica: %v", err)
		}

		fmt.Printf("Corrupted data for key: %s (in both primary and replica)\n", corrupted2Key)
	}

	printSection("6. Running Integrity Check to Detect and Repair Corruption")

	fmt.Println("Running manual integrity check...")
	checker.RunManualCheck()

	// Wait for check and repair to complete
	time.Sleep(2 * time.Second)

	printSection("7. Verifying Repairs")

	// Check if the first corrupted item was repaired
	for _, key := range []string{corruptedKey, corrupted2Key} {
		result, exists := checker.GetLastCheckResult(key)
		if !exists {
			fmt.Printf("No check result for key: %s\n", key)
			continue
		}

		fmt.Printf("Integrity status for key %s:\n", key)
		switch result.Status {
		case integrity.StatusOK:
			fmt.Println("  Status: OK (data is valid)")
		case integrity.StatusCorrupted:
			fmt.Println("  Status: CORRUPTED")
			if result.RepairedOK {
				fmt.Println("  Repair: Successfully repaired from replica")

				// Verify by decrypting
				data, exists, err := primaryStore.Get(key)
				if err != nil || !exists {
					fmt.Printf("  Verification: Failed to retrieve repaired data\n")
					continue
				}

				secureData, err := crypto.DeserializeSecureData(string(data))
				if err != nil {
					fmt.Printf("  Verification: Failed to deserialize repaired data\n")
					continue
				}

				plaintext, err := keyManager.DecryptAndVerify(secureData)
				if err != nil {
					fmt.Printf("  Verification: Repaired data still fails integrity check\n")
				} else {
					fmt.Printf("  Verification: Repaired data passes integrity check\n")
					fmt.Printf("  Decrypted content: %s\n", string(plaintext))
				}
			} else {
				fmt.Println("  Repair: Failed (no valid replica available)")
			}
		case integrity.StatusMissing:
			fmt.Println("  Status: MISSING (data not found)")
		default:
			fmt.Println("  Status: VERIFICATION FAILED")
		}
	}

	printSection("8. Periodic Checks in Production Environment")

	fmt.Println("In a production environment, periodic checks would run automatically at specified intervals.")
	fmt.Println("For example, our system might run these checks:")
	fmt.Println("  - Every hour for critical system data")
	fmt.Println("  - Every day for user data")
	fmt.Println("  - Every week for archival data")
	fmt.Println()
	fmt.Println("This ensures that:")
	fmt.Println("  - Data corruption is detected promptly")
	fmt.Println("  - Silent corruption doesn't go unnoticed")
	fmt.Println("  - Corrupted data can be repaired from replicas")
	fmt.Println("  - Alerts are generated for data that can't be repaired")

	fmt.Println("\nPeriodic Integrity Checks Demonstration Complete")
}
