package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"time"

	pb "github.com/can-dht/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	fmt.Println("=== CAN DHT Interactive CLI with User Authentication ===")
	fmt.Println("Type 'help' for a list of commands.")

	var (
		currentUser     string
		currentPassword string
		currentNodeAddr string
		currentClient   pb.CANServiceClient
		currentConn     *grpc.ClientConn
	)

	reader := bufio.NewReader(os.Stdin)

	for {
		// Display prompt based on login status and connected node
		prompt := "> "
		if currentUser != "" {
			prompt = fmt.Sprintf("%s@", currentUser)
			if currentNodeAddr != "" {
				prompt += currentNodeAddr
			}
			prompt += "> "
		}

		fmt.Print(prompt)
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading input:", err)
			continue
		}

		// Trim whitespace and split into command and args
		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}

		parts := strings.Fields(input)
		command := parts[0]
		args := parts[1:]

		switch command {
		case "help":
			printHelp()

		case "exit", "quit":
			fmt.Println("Goodbye!")
			// Close connection if open
			if currentConn != nil {
				currentConn.Close()
			}
			return

		case "login":
			if len(args) != 2 {
				fmt.Println("Usage: login <username> <password>")
				continue
			}
			username, password := args[0], args[1]

			// Simple validation (in a real system, this would verify against a database)
			currentUser = username
			currentPassword = password
			fmt.Printf("Welcome, %s! Your data will be encrypted using your credentials.\n", currentUser)

		case "logout":
			if currentUser == "" {
				fmt.Println("You are not logged in.")
				continue
			}
			fmt.Printf("Goodbye, %s!\n", currentUser)
			currentUser = ""
			currentPassword = ""

		case "connect":
			if len(args) != 1 {
				fmt.Println("Usage: connect <node_address>")
				fmt.Println("Example: connect localhost:8080")
				continue
			}

			// Close existing connection if any
			if currentConn != nil {
				currentConn.Close()
				currentConn = nil
				currentClient = nil
			}

			// Connect to the specified node
			nodeAddress := args[0]
			client, conn, err := connectToNode(nodeAddress)
			if err != nil {
				fmt.Printf("Failed to connect to %s: %v\n", nodeAddress, err)
				continue
			}

			currentNodeAddr = nodeAddress
			currentClient = client
			currentConn = conn
			fmt.Printf("Connected to node at %s\n", nodeAddress)

		case "status":
			if currentUser == "" {
				fmt.Println("Login status: Not logged in")
			} else {
				fmt.Printf("Login status: Logged in as %s\n", currentUser)
			}

			if currentNodeAddr == "" {
				fmt.Println("Connection status: Not connected to any node")
			} else {
				fmt.Printf("Connection status: Connected to %s\n", currentNodeAddr)
			}

		case "put":
			if !checkConnection(currentClient) {
				continue
			}

			if !checkLoggedIn(currentUser) {
				continue
			}

			if len(args) < 2 {
				fmt.Println("Usage: put <key> <value>")
				continue
			}

			key := args[0]
			value := strings.Join(args[1:], " ") // Allow spaces in value

			// Encrypt the key based on username and password
			encryptedKey := deriveEncryptedKey(currentUser, currentPassword, key)

			// Encrypt the value
			encryptedValue, err := encryptValue(currentUser, currentPassword, value)
			if err != nil {
				fmt.Printf("Error encrypting value: %v\n", err)
				continue
			}

			err = putKeyValue(currentClient, encryptedKey, encryptedValue)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Printf("Stored key '%s' (encrypted as '%s')\n", key, encryptedKey)
			}

		case "get":
			if !checkConnection(currentClient) {
				continue
			}

			if !checkLoggedIn(currentUser) {
				continue
			}

			if len(args) != 1 {
				fmt.Println("Usage: get <key>")
				continue
			}

			key := args[0]

			// Encrypt the key based on username and password
			encryptedKey := deriveEncryptedKey(currentUser, currentPassword, key)

			encryptedValue, err := getKeyValue(currentClient, encryptedKey)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else if encryptedValue == nil {
				fmt.Printf("Key '%s' not found\n", key)
			} else {
				// Decrypt the value
				decryptedValue, err := decryptValue(currentUser, currentPassword, string(encryptedValue))
				if err != nil {
					fmt.Printf("Error decrypting value: %v\n", err)
					continue
				}

				fmt.Printf("%s = %s\n", key, decryptedValue)
			}

		case "delete":
			if !checkConnection(currentClient) {
				continue
			}

			if !checkLoggedIn(currentUser) {
				continue
			}

			if len(args) != 1 {
				fmt.Println("Usage: delete <key>")
				continue
			}

			key := args[0]

			// Encrypt the key based on username and password
			encryptedKey := deriveEncryptedKey(currentUser, currentPassword, key)

			success, err := deleteKeyValue(currentClient, encryptedKey)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else if success {
				fmt.Printf("Deleted key '%s'\n", key)
			} else {
				fmt.Printf("Key '%s' not found or already deleted\n", key)
			}

		case "leave":
			if !checkConnection(currentClient) {
				continue
			}

			fmt.Printf("Instructing node %s to leave the network...\n", currentNodeAddr)
			success, err := leaveNetwork(currentClient)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else if success {
				fmt.Printf("Node %s is leaving the network\n", currentNodeAddr)
				// Close our connection as the node is leaving
				currentConn.Close()
				currentConn = nil
				currentClient = nil
				currentNodeAddr = ""
			} else {
				fmt.Println("Failed to initiate leave process")
			}

		default:
			fmt.Printf("Unknown command: %s\nType 'help' for a list of commands.\n", command)
		}
	}
}

// deriveEncryptedKey creates a deterministic, consistent encrypted key based on user credentials and the original key
func deriveEncryptedKey(username, password, originalKey string) string {
	// Combine username, password, and key to create a unique but deterministic key
	combinedData := username + ":" + password + ":" + originalKey

	// Use SHA-256 to create a fixed-length hash
	hash := sha256.Sum256([]byte(combinedData))

	// Convert to a Base64 string for use as a key in the DHT
	// We're using Base64 to ensure the key is URL-safe and a reasonable length
	return "user:" + username + ":" + base64.URLEncoding.EncodeToString(hash[:])
}

// encryptValue encrypts the value using the user's credentials
func encryptValue(username, password, value string) (string, error) {
	// Derive a key from the username and password
	key := deriveEncryptionKey(username, password)

	// In a real implementation, you would use proper encryption here
	// For simplicity, we're using a basic XOR encryption with the derived key
	encrypted := simpleEncrypt(value, key)

	return encrypted, nil
}

// decryptValue decrypts the value using the user's credentials
func decryptValue(username, password, encryptedValue string) (string, error) {
	// Derive the same key for decryption
	key := deriveEncryptionKey(username, password)

	// First decode the base64-encoded value
	decodedValue, err := base64.StdEncoding.DecodeString(encryptedValue)
	if err != nil {
		return "", fmt.Errorf("error decoding encrypted value: %w", err)
	}

	// Convert to string for XOR operation
	decodedString := string(decodedValue)

	// Expand the key to match the length of the decoded value
	expandedKey := expandKey(key, len(decodedString))

	// Perform XOR decryption (symmetric operation)
	var result []byte
	for i := 0; i < len(decodedString); i++ {
		result = append(result, decodedString[i]^expandedKey[i])
	}

	return string(result), nil
}

// deriveEncryptionKey creates a key for encryption/decryption based on username and password
func deriveEncryptionKey(username, password string) string {
	// Combine username and password
	combined := username + ":" + password

	// Create a hash
	hash := sha256.Sum256([]byte(combined))

	// Convert to hex string
	return hex.EncodeToString(hash[:])
}

// simpleEncrypt implements a basic XOR encryption for demonstration purposes
// In a real implementation, you would use a proper encryption library
func simpleEncrypt(input, key string) string {
	var result []byte

	// Expand the key if necessary
	expandedKey := expandKey(key, len(input))

	// XOR each byte of the input with the corresponding byte of the expanded key
	for i := 0; i < len(input); i++ {
		result = append(result, input[i]^expandedKey[i])
	}

	// Convert to Base64 for safe storage
	return base64.StdEncoding.EncodeToString(result)
}

// expandKey repeats the key pattern to match the required length
func expandKey(key string, length int) []byte {
	var expandedKey []byte

	for i := 0; i < length; i++ {
		expandedKey = append(expandedKey, key[i%len(key)])
	}

	return expandedKey
}

func printHelp() {
	fmt.Println("\nAvailable commands:")
	fmt.Println("  help                - Show this help message")
	fmt.Println("  exit, quit          - Exit the CLI")
	fmt.Println("  login <user> <pass> - Login with username and password")
	fmt.Println("  logout              - Logout from current session")
	fmt.Println("  connect <address>   - Connect to a CAN node (e.g., localhost:8080)")
	fmt.Println("  status              - Show current login and connection status")
	fmt.Println("  put <key> <value>   - Store an encrypted key-value pair")
	fmt.Println("  get <key>           - Retrieve and decrypt a value by key")
	fmt.Println("  delete <key>        - Delete a key-value pair")
	fmt.Println("  leave               - Tell the current node to leave the network")
	fmt.Println("")
	fmt.Println("To add a new node to the network, run the can-node binary in a separate terminal:")
	fmt.Println("  ./bin/can-node --port 8081 --dimensions 2 --data-dir ./data/node2 --join localhost:8080")
	fmt.Println("")
}

func checkLoggedIn(username string) bool {
	if username == "" {
		fmt.Println("You must login first. Use 'login <username> <password>'")
		return false
	}
	return true
}

func checkConnection(client pb.CANServiceClient) bool {
	if client == nil {
		fmt.Println("Not connected to any node. Use 'connect <address>' first.")
		return false
	}
	return true
}

func connectToNode(address string) (pb.CANServiceClient, *grpc.ClientConn, error) {
	conn, err := grpc.DialContext(
		context.Background(),
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to node at %s: %w", address, err)
	}

	client := pb.NewCANServiceClient(conn)
	return client, conn, nil
}

func putKeyValue(client pb.CANServiceClient, key, value string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := &pb.PutRequest{
		Key:   key,
		Value: []byte(value),
	}

	resp, err := client.Put(ctx, req)
	if err != nil {
		return err
	}

	if !resp.Success {
		return fmt.Errorf("put operation failed")
	}

	return nil
}

func getKeyValue(client pb.CANServiceClient, key string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := &pb.GetRequest{
		Key: key,
	}

	resp, err := client.Get(ctx, req)
	if err != nil {
		return nil, err
	}

	if !resp.Success {
		return nil, fmt.Errorf("get operation failed")
	}

	if !resp.Exists {
		return nil, nil // Key not found
	}

	return resp.Value, nil
}

func deleteKeyValue(client pb.CANServiceClient, key string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := &pb.DeleteRequest{
		Key: key,
	}

	resp, err := client.Delete(ctx, req)
	if err != nil {
		return false, err
	}

	if !resp.Success {
		return false, fmt.Errorf("delete operation failed")
	}

	return resp.Existed, nil
}

func leaveNetwork(client pb.CANServiceClient) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req := &pb.LeaveRequest{}

	resp, err := client.Leave(ctx, req)
	if err != nil {
		return false, err
	}

	return resp.Success, nil
}
