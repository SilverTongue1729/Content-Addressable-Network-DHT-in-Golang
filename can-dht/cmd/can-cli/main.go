package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	pb "github.com/can-dht/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Simulated user database
type User struct {
	Username string
	Password string
}

var users = []User{
	{Username: "admin", Password: "admin123"},
	{Username: "user1", Password: "password1"},
	{Username: "user2", Password: "password2"},
}

func main() {
	fmt.Println("=== CAN DHT Interactive CLI ===")
	fmt.Println("Type 'help' for a list of commands.")

	var (
		currentUser     *User
		currentNodeAddr string
		currentClient   pb.CANServiceClient
		currentConn     *grpc.ClientConn
	)

	reader := bufio.NewReader(os.Stdin)

	for {
		// Display prompt based on login status and connected node
		prompt := "> "
		if currentUser != nil {
			prompt = fmt.Sprintf("%s@", currentUser.Username)
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
			user := authenticateUser(username, password)
			if user == nil {
				fmt.Println("Invalid username or password.")
				continue
			}
			currentUser = user
			fmt.Printf("Welcome, %s!\n", currentUser.Username)

		case "logout":
			if currentUser == nil {
				fmt.Println("You are not logged in.")
				continue
			}
			fmt.Printf("Goodbye, %s!\n", currentUser.Username)
			currentUser = nil

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
			if currentUser == nil {
				fmt.Println("Login status: Not logged in")
			} else {
				fmt.Printf("Login status: Logged in as %s\n", currentUser.Username)
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

			if len(args) < 2 {
				fmt.Println("Usage: put <key> <value>")
				continue
			}

			key := args[0]
			value := strings.Join(args[1:], " ") // Allow spaces in value

			err := putKeyValue(currentClient, key, value)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Printf("Stored key '%s'\n", key)
			}

		case "get":
			if !checkConnection(currentClient) {
				continue
			}

			if len(args) != 1 {
				fmt.Println("Usage: get <key>")
				continue
			}

			key := args[0]
			value, err := getKeyValue(currentClient, key)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else if value == nil {
				fmt.Printf("Key '%s' not found\n", key)
			} else {
				fmt.Printf("%s = %s\n", key, string(value))
			}

		case "delete":
			if !checkConnection(currentClient) {
				continue
			}

			if len(args) != 1 {
				fmt.Println("Usage: delete <key>")
				continue
			}

			key := args[0]
			success, err := deleteKeyValue(currentClient, key)
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

func printHelp() {
	fmt.Println("\nAvailable commands:")
	fmt.Println("  help                - Show this help message")
	fmt.Println("  exit, quit          - Exit the CLI")
	fmt.Println("  login <user> <pass> - Login with username and password")
	fmt.Println("  logout              - Logout from current session")
	fmt.Println("  connect <address>   - Connect to a CAN node (e.g., localhost:8080)")
	fmt.Println("  status              - Show current login and connection status")
	fmt.Println("  put <key> <value>   - Store a key-value pair")
	fmt.Println("  get <key>           - Retrieve a value by key")
	fmt.Println("  delete <key>        - Delete a key-value pair")
	fmt.Println("  leave               - Tell the current node to leave the network")
	fmt.Println("")
	fmt.Println("To add a new node to the network, run the can-node binary in a separate terminal:")
	fmt.Println("  ./bin/can-node --port 8081 --dimensions 2 --data-dir ./data/node2 --join localhost:8080")
	fmt.Println("")
}

func authenticateUser(username, password string) *User {
	for _, user := range users {
		if user.Username == username && user.Password == password {
			return &user
		}
	}
	return nil
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
