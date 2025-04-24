package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"flag"
)

var (
	nodeAddress = flag.String("node", "localhost:8080", "Address of CAN node to connect to")
	username    = flag.String("user", "", "Username for authentication")
	password    = flag.String("password", "", "Password for authentication")
)

func main() {
	flag.Parse()

	fmt.Println("=== CAN DHT Interactive CLI with User Authentication ===")
	fmt.Println("Type 'help' for a list of commands.")

	var (
		currentUser     string
		currentPassword string
		currentNodeAddr string
	)

	// Initialize with command line parameters if provided
	if *username != "" {
		currentUser = *username
	}
	if *password != "" {
		currentPassword = *password
	}
	if *nodeAddress != "" {
		currentNodeAddr = *nodeAddress
	}

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
			return

		case "login":
			if len(args) != 2 {
				fmt.Println("Usage: login <username> <password>")
				continue
			}
			username, password := args[0], args[1]

			// Simple login simulation
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

			nodeAddress := args[0]
			currentNodeAddr = nodeAddress
			fmt.Printf("Connected to node at %s (simulated)\n", nodeAddress)

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
			if currentUser == "" {
				fmt.Println("Error: You must login first")
				continue
			}

			if currentNodeAddr == "" {
				fmt.Println("Error: You must connect to a node first")
				continue
			}

			if len(args) < 2 {
				fmt.Println("Usage: put <key> <value>")
				continue
			}

			key := args[0]
			value := strings.Join(args[1:], " ") // Allow spaces in value

			// Simulate encryption
			encryptedKey := deriveEncryptedKey(currentUser, currentPassword, key)
			// Just compute this for demonstration, don't need to save it as a variable
			_ = simpleEncrypt(value, deriveEncryptionKey(currentUser, currentPassword))

			fmt.Printf("PUT operation successful (simulated)\n")
			fmt.Printf("Stored key '%s' (encrypted as '%s')\n", key, encryptedKey)

		case "get":
			if currentUser == "" {
				fmt.Println("Error: You must login first")
				continue
			}

			if currentNodeAddr == "" {
				fmt.Println("Error: You must connect to a node first")
				continue
			}

			if len(args) != 1 {
				fmt.Println("Usage: get <key>")
				continue
			}

			key := args[0]
			encryptedKey := deriveEncryptedKey(currentUser, currentPassword, key)
			
			// Simulate retrieval (in a real system, you would retrieve from the DHT)
			fmt.Printf("GET operation successful (simulated)\n")
			fmt.Printf("Retrieved: %s = value for %s\n", key, encryptedKey)

		default:
			fmt.Printf("Unknown command: %s\nType 'help' for available commands.\n", command)
		}
	}
}

// Helper function to derive an encrypted key
func deriveEncryptedKey(username, password, originalKey string) string {
	// Combine username, password, and key to create a composite key
	compositeKey := username + ":" + password + ":" + originalKey
	
	// Hash the composite key to create a consistent, secure derived key
	hash := sha256.Sum256([]byte(compositeKey))
	return hex.EncodeToString(hash[:])
}

// Simple encryption function for demonstration
func simpleEncrypt(input, key string) string {
	// Expand key to match input length
	expandedKey := expandKey(key, len(input))
	
	// XOR each byte of the input with the corresponding byte of the expanded key
	result := make([]byte, len(input))
	for i := 0; i < len(input); i++ {
		result[i] = input[i] ^ expandedKey[i]
	}
	
	// Base64 encode the result for safe storage
	return base64.StdEncoding.EncodeToString(result)
}

// Derive an encryption key from username and password
func deriveEncryptionKey(username, password string) string {
	// Combine username and password
	combined := username + ":" + password
	
	// Hash the combined string to create a consistent encryption key
	hash := sha256.Sum256([]byte(combined))
	return hex.EncodeToString(hash[:])
}

// Expand a key to the desired length
func expandKey(key string, length int) []byte {
	keyBytes := []byte(key)
	result := make([]byte, length)
	
	// Repeat the key as needed to fill the result
	for i := 0; i < length; i++ {
		result[i] = keyBytes[i%len(keyBytes)]
	}
	
	return result
}

// Print help message
func printHelp() {
	fmt.Println("Available Commands:")
	fmt.Println("  help                      - Show this help message")
	fmt.Println("  login <username> <pass>   - Login with credentials")
	fmt.Println("  logout                    - Logout from current session")
	fmt.Println("  connect <node_address>    - Connect to a CAN node")
	fmt.Println("  status                    - Show login and connection status")
	fmt.Println("  put <key> <value>         - Store a key-value pair")
	fmt.Println("  get <key>                 - Retrieve a value by key")
	fmt.Println("  exit, quit                - Exit the CLI")
}
