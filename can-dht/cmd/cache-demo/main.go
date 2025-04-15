package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/can-dht/pkg/storage"
)

// Helper function to print a separator line
func printSection(title string) {
	fmt.Println("\n" + title)
	fmt.Println(string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}) + string([]byte{'-'}))
}

// Time a function execution and return the elapsed time
func timeExecution(fn func() error) (time.Duration, error) {
	start := time.Now()
	err := fn()
	return time.Since(start), err
}

func main() {
	fmt.Println("CAN-DHT Caching Mechanism Demonstration")
	fmt.Println("=======================================")
	fmt.Println("This demo shows how the caching system improves performance in our DHT.")

	// Create a temporary directory for the demo
	tempDir, err := os.MkdirTemp("", "cache-demo")
	if err != nil {
		log.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	printSection("1. Setting Up Storage with Different Cache Settings")

	// Create a store with short TTL for demonstration purposes
	shortTTLOpts := storage.DefaultStoreOptions()
	shortTTLOpts.DataDir = tempDir + "/cache-demo"
	shortTTLOpts.CacheTTL = 5 * time.Second // Short TTL for demo
	shortTTLOpts.MaxCacheSize = 100

	store, err := storage.NewStore(shortTTLOpts)
	if err != nil {
		log.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	fmt.Println("Created data store at:", shortTTLOpts.DataDir)
	fmt.Printf("Cache settings: TTL=%s, MaxSize=%d entries\n",
		shortTTLOpts.CacheTTL,
		shortTTLOpts.MaxCacheSize)

	printSection("2. Storing Initial Data")

	// Store some sample data
	dataSize := 1024 * 10 // 10KB per value
	largeValue := make([]byte, dataSize)
	for i := 0; i < dataSize; i++ {
		largeValue[i] = byte(i % 256) // Fill with some pattern
	}

	// Store multiple entries
	fmt.Printf("Storing %d entries with ~10KB each...\n", 10)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("large-key-%d", i)
		if err := store.Put(key, largeValue); err != nil {
			log.Fatalf("Failed to store data: %v", err)
		}
	}
	fmt.Println("Data stored successfully.")

	printSection("3. Demonstrating Cache Hit vs Cache Miss")

	// First access (cache miss)
	fmt.Println("First access (cache miss expected):")
	key := "large-key-1"
	missTime, err := timeExecution(func() error {
		value, exists, err := store.Get(key)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("key not found")
		}
		fmt.Printf("Retrieved %d bytes for key '%s'\n", len(value), key)
		return nil
	})
	if err != nil {
		log.Fatalf("Failed to retrieve data: %v", err)
	}
	fmt.Printf("Access time: %s\n", missTime)

	// Second access (cache hit)
	fmt.Println("\nSecond access (cache hit expected):")
	hitTime, err := timeExecution(func() error {
		value, exists, err := store.Get(key)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("key not found")
		}
		fmt.Printf("Retrieved %d bytes for key '%s'\n", len(value), key)
		return nil
	})
	if err != nil {
		log.Fatalf("Failed to retrieve data: %v", err)
	}
	fmt.Printf("Access time: %s\n", hitTime)

	// Performance comparison
	speedup := float64(missTime) / float64(hitTime)
	fmt.Printf("\nCache performance: %.2fx faster with cache hit\n", speedup)

	printSection("4. Demonstrating Cache Expiration (TTL)")

	fmt.Printf("Waiting for cache to expire (TTL = %s)...\n", shortTTLOpts.CacheTTL)
	time.Sleep(shortTTLOpts.CacheTTL + time.Second)

	fmt.Println("Access after TTL expiration (cache miss expected):")
	expiredTime, err := timeExecution(func() error {
		value, exists, err := store.Get(key)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("key not found")
		}
		fmt.Printf("Retrieved %d bytes for key '%s'\n", len(value), key)
		return nil
	})
	if err != nil {
		log.Fatalf("Failed to retrieve data: %v", err)
	}
	fmt.Printf("Access time: %s\n", expiredTime)

	printSection("5. Demonstrating Cache Auto-Population")

	// Access after expiration should repopulate cache
	fmt.Println("First access after expiration repopulates cache")

	// Now access again to show cache hit
	fmt.Println("\nAccess after repopulation (cache hit expected):")
	repopTime, err := timeExecution(func() error {
		value, exists, err := store.Get(key)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("key not found")
		}
		fmt.Printf("Retrieved %d bytes for key '%s'\n", len(value), key)
		return nil
	})
	if err != nil {
		log.Fatalf("Failed to retrieve data: %v", err)
	}
	fmt.Printf("Access time: %s\n", repopTime)

	printSection("6. Simulating Multiple Access Patterns")

	// Simulate multiple clients accessing different data
	fmt.Println("Simulating multiple client access patterns:")
	fmt.Println("- Hot keys accessed frequently")
	fmt.Println("- Cold keys accessed rarely")

	// Access hot keys multiple times
	hotKeys := []string{"large-key-0", "large-key-1", "large-key-2"}
	fmt.Println("\nAccessing hot keys multiple times:")

	hotTotalTime := time.Duration(0)
	for i := 0; i < 5; i++ {
		for _, hotKey := range hotKeys {
			accessTime, err := timeExecution(func() error {
				_, exists, err := store.Get(hotKey)
				if err != nil {
					return err
				}
				if !exists {
					return fmt.Errorf("key not found")
				}
				return nil
			})
			if err != nil {
				log.Fatalf("Failed to retrieve data: %v", err)
			}
			hotTotalTime += accessTime
		}
	}

	// Access cold keys once
	coldKeys := []string{"large-key-7", "large-key-8", "large-key-9"}
	fmt.Println("\nAccessing cold keys once:")

	coldTotalTime := time.Duration(0)
	for _, coldKey := range coldKeys {
		accessTime, err := timeExecution(func() error {
			_, exists, err := store.Get(coldKey)
			if err != nil {
				return err
			}
			if !exists {
				return fmt.Errorf("key not found")
			}
			return nil
		})
		if err != nil {
			log.Fatalf("Failed to retrieve data: %v", err)
		}
		coldTotalTime += accessTime
	}

	// Calculate average access times
	hotAvg := hotTotalTime / time.Duration(len(hotKeys)*5)
	coldAvg := coldTotalTime / time.Duration(len(coldKeys))

	fmt.Printf("\nAverage access time for hot keys: %s\n", hotAvg)
	fmt.Printf("Average access time for cold keys: %s\n", coldAvg)
	fmt.Printf("Hot keys are accessed %.2fx faster due to caching\n", float64(coldAvg)/float64(hotAvg))

	printSection("7. Cache Benefits in a Distributed System")

	fmt.Println("In a distributed DHT, caching provides the following benefits:")
	fmt.Println("  1. Reduced network traffic between nodes")
	fmt.Println("  2. Lower latency for frequently accessed data")
	fmt.Println("  3. Decreased load on nodes responsible for popular data")
	fmt.Println("  4. Automatic adaptation to access patterns")
	fmt.Println("  5. Resilience to temporary network issues")

	fmt.Println("\nCaching Mechanism Demonstration Complete")
}
