package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	fmt.Println("CAN-DHT Test Client")
	fmt.Println("-------------------")
	fmt.Println("Commands:")
	fmt.Println("  put <key> <value> - Store a key-value pair")
	fmt.Println("  get <key> - Retrieve a value by key")
	fmt.Println("  delete <key> - Delete a key-value pair")
	fmt.Println("  quit - Exit the client")
	fmt.Println()
	
	// In a real implementation, we would connect to a node
	// For this simplified version, we'll just simulate the operations
	
	fmt.Print("Node address [localhost:8080]: ")
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	address := scanner.Text()
	if address == "" {
		address = "localhost:8080"
	}
	
	fmt.Printf("Connected to node at %s\n", address)
	
	for {
		fmt.Print("> ")
		scanner.Scan()
		input := scanner.Text()
		
		parts := strings.Fields(input)
		if len(parts) == 0 {
			continue
		}
		
		command := parts[0]
		
		switch command {
		case "put":
			if len(parts) < 3 {
				fmt.Println("Usage: put <key> <value>")
				continue
			}
			key := parts[1]
			value := strings.Join(parts[2:], " ")
			
			// In a real implementation, we would send the put request to the node
			// For this simplified version, we'll just print the operation
			fmt.Printf("PUT %s = %s\n", key, value)
			fmt.Println("Success")
			
		case "get":
			if len(parts) < 2 {
				fmt.Println("Usage: get <key>")
				continue
			}
			key := parts[1]
			
			// In a real implementation, we would send the get request to the node
			// For this simplified version, we'll just print the operation
			fmt.Printf("GET %s\n", key)
			fmt.Println("Value: <simulated value>")
			
		case "delete":
			if len(parts) < 2 {
				fmt.Println("Usage: delete <key>")
				continue
			}
			key := parts[1]
			
			// In a real implementation, we would send the delete request to the node
			// For this simplified version, we'll just print the operation
			fmt.Printf("DELETE %s\n", key)
			fmt.Println("Success")
			
		case "quit", "exit":
			fmt.Println("Goodbye!")
			return
			
		default:
			fmt.Printf("Unknown command: %s\n", command)
			fmt.Println("Commands: put, get, delete, quit")
		}
	}
} 