package main

import (
	"fmt"
	"log"

	"github.com/can-dht/pkg/crypto"
)

// Helper function to print a separator line
func printSection(title string) {
	fmt.Println("\n" + title)
	fmt.Println(string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}))
}

func main() {
	fmt.Println("CAN-DHT Enhanced Authentication and Authorization Demo")
	fmt.Println("====================================================")
	fmt.Println("This demo shows how different users can use the same key independently")
	fmt.Println("and demonstrates CRUD operations with different permission levels.")

	// Create an enhanced authentication manager
	authManager := crypto.NewEnhancedAuthManager()

	printSection("1. User Registration")

	// Register some users
	users := []struct {
		username string
		password string
	}{
		{"alice", "alice123"},
		{"bob", "bob456"},
		{"charlie", "charlie789"},
	}

	for _, user := range users {
		err := authManager.RegisterUser(user.username, user.password)
		if err != nil {
			log.Fatalf("Failed to register user %s: %v", user.username, err)
		}
		fmt.Printf("User registered: %s\n", user.username)
	}

	printSection("2. Same Key Used By Different Users (Key Namespacing)")

	// Same key "profile" used by different users
	sameKey := "profile"

	// Alice creates her profile
	err := authManager.CreateOwnData("alice", "alice123", sameKey, []byte("Alice's personal information"))
	if err != nil {
		log.Fatalf("Failed to create Alice's data: %v", err)
	}
	fmt.Printf("Alice created data with key '%s'\n", sameKey)

	// Bob creates his profile with the same key
	err = authManager.CreateOwnData("bob", "bob456", sameKey, []byte("Bob's personal information"))
	if err != nil {
		log.Fatalf("Failed to create Bob's data: %v", err)
	}
	fmt.Printf("Bob created data with the same key '%s'\n", sameKey)

	// Charlie creates his profile with the same key
	err = authManager.CreateOwnData("charlie", "charlie789", sameKey, []byte("Charlie's personal information"))
	if err != nil {
		log.Fatalf("Failed to create Charlie's data: %v", err)
	}
	fmt.Printf("Charlie created data with the same key '%s'\n", sameKey)

	// Read all three profiles to show they are separate
	aliceProfile, err := authManager.ReadOwnData("alice", "alice123", sameKey)
	if err != nil {
		log.Fatalf("Failed to read Alice's data: %v", err)
	}
	fmt.Printf("Alice's profile: %s\n", string(aliceProfile))

	bobProfile, err := authManager.ReadOwnData("bob", "bob456", sameKey)
	if err != nil {
		log.Fatalf("Failed to read Bob's data: %v", err)
	}
	fmt.Printf("Bob's profile: %s\n", string(bobProfile))

	charlieProfile, err := authManager.ReadOwnData("charlie", "charlie789", sameKey)
	if err != nil {
		log.Fatalf("Failed to read Charlie's data: %v", err)
	}
	fmt.Printf("Charlie's profile: %s\n", string(charlieProfile))

	printSection("3. CRUD Operations Demo - Create")

	// Alice creates a shared document
	sharedDocKey := "shared-doc"
	fmt.Printf("Alice creates a shared document with key '%s'\n", sharedDocKey)

	// Define permissions for Bob and Charlie
	permissions := []crypto.UserPermission{
		{UserID: "bob", Permission: crypto.PermissionRead | crypto.PermissionUpdate},
		{UserID: "charlie", Permission: crypto.PermissionRead},
	}

	err = authManager.CreateData("alice", "alice", "alice123", sharedDocKey,
		[]byte("Shared document content created by Alice"), permissions)
	if err != nil {
		log.Fatalf("Failed to create shared document: %v", err)
	}
	fmt.Println("Alice created a shared document with different permissions for Bob and Charlie")

	printSection("4. CRUD Operations Demo - Read")

	// Alice reads the document (as owner, has all permissions)
	aliceDoc, err := authManager.ReadData("alice", "alice123", "alice", sharedDocKey)
	if err != nil {
		log.Fatalf("Failed to read Alice's shared document: %v", err)
	}
	fmt.Printf("Alice reads the document: %s\n", string(aliceDoc))

	// Bob reads the document (has READ permission)
	bobDoc, err := authManager.ReadData("bob", "bob456", "alice", sharedDocKey)
	if err != nil {
		log.Fatalf("Failed to read Alice's shared document as Bob: %v", err)
	}
	fmt.Printf("Bob reads the document: %s\n", string(bobDoc))

	// Charlie reads the document (has READ permission)
	charlieDoc, err := authManager.ReadData("charlie", "charlie789", "alice", sharedDocKey)
	if err != nil {
		log.Fatalf("Failed to read Alice's shared document as Charlie: %v", err)
	}
	fmt.Printf("Charlie reads the document: %s\n", string(charlieDoc))

	printSection("5. CRUD Operations Demo - Update")

	// Bob updates the document (has UPDATE permission)
	fmt.Println("Bob tries to update the document (has UPDATE permission)")
	err = authManager.UpdateData("bob", "bob456", "alice", sharedDocKey,
		[]byte("Shared document content updated by Bob"))
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Println("Bob successfully updated the document")
	}

	// Charlie tries to update the document (doesn't have UPDATE permission)
	fmt.Println("Charlie tries to update the document (doesn't have UPDATE permission)")
	err = authManager.UpdateData("charlie", "charlie789", "alice", sharedDocKey,
		[]byte("Attempted update by Charlie"))
	if err != nil {
		fmt.Printf("Error as expected: %v\n", err)
	} else {
		fmt.Println("Charlie updated the document (this shouldn't happen)")
	}

	// Verify the document content after update attempts
	updatedDoc, err := authManager.ReadData("alice", "alice123", "alice", sharedDocKey)
	if err != nil {
		log.Fatalf("Failed to read updated document: %v", err)
	}
	fmt.Printf("Document content after update attempts: %s\n", string(updatedDoc))

	printSection("6. CRUD Operations Demo - Delete")

	// Charlie tries to delete the document (doesn't have DELETE permission)
	fmt.Println("Charlie tries to delete the document (doesn't have DELETE permission)")
	err = authManager.DeleteData("charlie", "charlie789", "alice", sharedDocKey)
	if err != nil {
		fmt.Printf("Error as expected: %v\n", err)
	} else {
		fmt.Println("Charlie deleted the document (this shouldn't happen)")
	}

	// Alice modifies permissions to give Bob delete permission
	fmt.Println("Alice modifies permissions to give Bob delete permission")
	err = authManager.ModifyPermissions("alice", "alice123", "bob", "alice", sharedDocKey,
		crypto.PermissionRead|crypto.PermissionUpdate|crypto.PermissionDelete)
	if err != nil {
		log.Fatalf("Failed to modify permissions: %v", err)
	}

	// Bob deletes the document (now has DELETE permission)
	fmt.Println("Bob tries to delete the document (now has DELETE permission)")
	err = authManager.DeleteData("bob", "bob456", "alice", sharedDocKey)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Println("Bob successfully deleted the document")
	}

	// Verify the document was deleted
	fmt.Println("Alice tries to read the deleted document")
	_, err = authManager.ReadData("alice", "alice123", "alice", sharedDocKey)
	if err != nil {
		fmt.Printf("Error as expected (document deleted): %v\n", err)
	} else {
		fmt.Println("Document still exists (this shouldn't happen)")
	}

	printSection("7. Listing Available Data")

	// Alice lists her available data
	aliceKeys, err := authManager.ListDataKeys("alice", "alice123")
	if err != nil {
		log.Fatalf("Failed to list Alice's data keys: %v", err)
	}
	fmt.Println("Alice's available data keys:")
	for _, key := range aliceKeys {
		fmt.Printf("- %s\n", key)
	}

	// Bob lists his available data
	bobKeys, err := authManager.ListDataKeys("bob", "bob456")
	if err != nil {
		log.Fatalf("Failed to list Bob's data keys: %v", err)
	}
	fmt.Println("Bob's available data keys:")
	for _, key := range bobKeys {
		fmt.Printf("- %s\n", key)
	}

	printSection("8. Permission Management")

	// Create a new document for permission management demo
	permDocKey := "permission-doc"
	err = authManager.CreateOwnData("alice", "alice123", permDocKey,
		[]byte("Document for permission management demo"))
	if err != nil {
		log.Fatalf("Failed to create permission document: %v", err)
	}
	fmt.Printf("Alice created a new document with key '%s'\n", permDocKey)

	// Alice gives Charlie READ permission
	fmt.Println("Alice gives Charlie READ permission")
	err = authManager.ModifyPermissions("alice", "alice123", "charlie", "alice", permDocKey,
		crypto.PermissionRead)
	if err != nil {
		log.Fatalf("Failed to give Charlie read permission: %v", err)
	}

	// Charlie reads the document
	charliePermDoc, err := authManager.ReadData("charlie", "charlie789", "alice", permDocKey)
	if err != nil {
		log.Fatalf("Charlie failed to read the document: %v", err)
	}
	fmt.Printf("Charlie reads the document: %s\n", string(charliePermDoc))

	// Alice revokes Charlie's permission
	fmt.Println("Alice revokes Charlie's permission")
	err = authManager.ModifyPermissions("alice", "alice123", "charlie", "alice", permDocKey,
		crypto.PermissionNone)
	if err != nil {
		log.Fatalf("Failed to revoke Charlie's permission: %v", err)
	}

	// Charlie tries to read the document again
	fmt.Println("Charlie tries to read the document again (permission revoked)")
	_, err = authManager.ReadData("charlie", "charlie789", "alice", permDocKey)
	if err != nil {
		fmt.Printf("Error as expected (permission revoked): %v\n", err)
	} else {
		fmt.Println("Charlie read the document (this shouldn't happen)")
	}

	fmt.Println("\nEnhanced Authentication and Authorization Demo Complete")
}
