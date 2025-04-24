package storage

import (
	"encoding/json"
	"fmt"
	// Properly comment out unused import
	// "log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/boltdb/bolt"
)

const (
	// DefaultBucketName is the name of the bucket where key-value pairs are stored
	DefaultBucketName = "can-dht"
)

// CacheEntry represents an entry in the LRU cache
type CacheEntry struct {
	Value     []byte
	Timestamp time.Time // For TTL expiration
}

// StoreOptions configures the behavior of the Store
type StoreOptions struct {
	// DataDir is the directory where data will be stored
	DataDir string

	// BucketName is the name of the bolt bucket where data will be stored
	BucketName string

	// EnableCache enables the LRU cache
	EnableCache bool

	// CacheSize is the maximum number of entries in the cache
	CacheSize int

	// CacheTTL is the time-to-live for cached entries
	CacheTTL time.Duration
}

// DefaultStoreOptions returns the default store options
func DefaultStoreOptions() *StoreOptions {
	return &StoreOptions{
		DataDir:     "data",
		BucketName:  DefaultBucketName,
		EnableCache: true,
		CacheSize:   1000,  // Default cache size is 1000 entries
		CacheTTL:    time.Hour, // Default TTL is 1 hour
	}
}

// Store manages persistent storage of key-value pairs
type Store struct {
	// DB is the underlying bolt database
	DB *bolt.DB

	// Options contains the store configuration
	Options *StoreOptions

	// Cache is an LRU cache for frequently accessed key-value pairs
	Cache map[string]*CacheEntry

	// CacheKeys maintains the order of cache keys for LRU eviction
	CacheKeys []string

	// Path to the database file
	Path string

	mu sync.RWMutex
}

// NewStore creates a new store with the given options
func NewStore(options *StoreOptions) (*Store, error) {
	// Create data directory if it doesn't exist
	if err := os.MkdirAll(options.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	// Open the database
	dbPath := filepath.Join(options.DataDir, "can.db")
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Create the bucket
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(options.BucketName))
		return err
	}); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create bucket: %w", err)
	}

	// Create the store
	store := &Store{
		DB:      db,
		Options: options,
		Path:    options.DataDir, // Set the Path field
	}

	// Initialize cache if enabled
	if options.EnableCache {
		store.Cache = make(map[string]*CacheEntry)
		store.CacheKeys = make([]string, 0, options.CacheSize)

		// Start cache cleanup routine
		go store.cleanupCache()
	}

	return store, nil
}

// cleanupCache periodically removes expired entries from the cache
func (s *Store) cleanupCache() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		s.mu.Lock()
		now := time.Now()
		expiredKeys := make([]string, 0)
		
		// Find expired keys
		for key, entry := range s.Cache {
			if now.Sub(entry.Timestamp) > s.Options.CacheTTL {
				expiredKeys = append(expiredKeys, key)
			}
		}
		
		// Remove expired keys from cache
		for _, key := range expiredKeys {
			delete(s.Cache, key)
		}
		
		// Update cache keys list
		if len(expiredKeys) > 0 {
			s.updateCacheKeysList()
		}
		
		s.mu.Unlock()
	}
}

// updateCacheKeysList rebuilds the list of cache keys
func (s *Store) updateCacheKeysList() {
	// Create a new list with the current keys
	newKeys := make([]string, 0, len(s.Cache))
	for key := range s.Cache {
		newKeys = append(newKeys, key)
	}
	s.CacheKeys = newKeys
}

// Put stores a key-value pair
func (s *Store) Put(key string, value []byte) error {
	// Update the database
	err := s.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(s.Options.BucketName))
		return b.Put([]byte(key), value)
	})

	// If successful and cache is enabled, update the cache
	if err == nil && s.Options.EnableCache {
		s.mu.Lock()
		defer s.mu.Unlock()

		// Check if the key is already in the cache
		_, exists := s.Cache[key]

		// Add or update the cache entry
		s.Cache[key] = &CacheEntry{
			Value:     value,
			Timestamp: time.Now(),
		}

		// If the key is new, add it to the keys list and potentially evict
		if !exists {
			s.CacheKeys = append(s.CacheKeys, key)
			
			// If cache exceeds size limit, evict the oldest entry
			if len(s.CacheKeys) > s.Options.CacheSize {
				oldestKey := s.CacheKeys[0]
				delete(s.Cache, oldestKey)
				s.CacheKeys = s.CacheKeys[1:]
			}
		} else {
			// Move the key to the end of the list (most recently used)
			s.updateKeyPosition(key)
		}
	}

	return err
}

// updateKeyPosition moves a key to the end of the keys list
func (s *Store) updateKeyPosition(key string) {
	// Find the key position
	pos := -1
	for i, k := range s.CacheKeys {
		if k == key {
			pos = i
			break
		}
	}

	// If found, remove it and append to the end
	if pos >= 0 {
		// Remove from current position
		s.CacheKeys = append(s.CacheKeys[:pos], s.CacheKeys[pos+1:]...)
		// Add to the end
		s.CacheKeys = append(s.CacheKeys, key)
	}
}

// Get retrieves a value by key
func (s *Store) Get(key string) ([]byte, bool, error) {
	// If cache is enabled, check the cache first
	if s.Options.EnableCache {
		s.mu.RLock()
		entry, exists := s.Cache[key]
		s.mu.RUnlock()

		if exists {
			// Check if the entry is expired
			if time.Since(entry.Timestamp) <= s.Options.CacheTTL {
				// Update the key position in the LRU list
				s.mu.Lock()
				s.updateKeyPosition(key)
				entry.Timestamp = time.Now() // Update timestamp
				s.mu.Unlock()
				
				// Cache hit
				return entry.Value, true, nil
			}
			
			// Entry expired, remove from cache
			s.mu.Lock()
			delete(s.Cache, key)
			s.updateCacheKeysList()
			s.mu.Unlock()
		}
	}

	// Cache miss or disabled, read from database
	var value []byte
	var found bool

	err := s.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(s.Options.BucketName))
		value = b.Get([]byte(key))
		found = value != nil
		if found {
			// Make a copy of the value since Bolt's value is only valid during the transaction
			valueCopy := make([]byte, len(value))
			copy(valueCopy, value)
			value = valueCopy
		}
		return nil
	})

	// If found and cache is enabled, add to cache
	if err == nil && found && s.Options.EnableCache {
		s.mu.Lock()
		defer s.mu.Unlock()

		// Add to cache
		s.Cache[key] = &CacheEntry{
			Value:     value,
			Timestamp: time.Now(),
		}

		// Update cache keys list
		_, exists := s.Cache[key]
		if !exists {
			s.CacheKeys = append(s.CacheKeys, key)
			
			// Evict if necessary
			if len(s.CacheKeys) > s.Options.CacheSize {
				oldestKey := s.CacheKeys[0]
				delete(s.Cache, oldestKey)
				s.CacheKeys = s.CacheKeys[1:]
			}
		} else {
			s.updateKeyPosition(key)
		}
	}

	return value, found, err
}

// Delete removes a key-value pair
func (s *Store) Delete(key string) error {
	// Remove from the database
	err := s.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(s.Options.BucketName))
		return b.Delete([]byte(key))
	})

	// If successful and cache is enabled, remove from cache
	if err == nil && s.Options.EnableCache {
		s.mu.Lock()
		defer s.mu.Unlock()

		// Check if the key is in the cache
		if _, exists := s.Cache[key]; exists {
			delete(s.Cache, key)
			s.updateCacheKeysList()
		}
	}

	return err
}

// Close closes the database
func (s *Store) Close() error {
	return s.DB.Close()
}

// GetAllKeys returns all keys in the store
func (s *Store) GetAllKeys() ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var keys []string
	err := s.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(s.Options.BucketName))
		c := b.Cursor()

		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			keys = append(keys, string(k))
		}
		return nil
	})

	return keys, err
}

// GetAll returns all key-value pairs in the store
func (s *Store) GetAll() (map[string][]byte, error) {
	result := make(map[string][]byte)

	err := s.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(s.Options.BucketName))
		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			result[string(k)] = append([]byte{}, v...)
		}
		return nil
	})

	return result, err
}

// SaveMetadata saves node metadata to a JSON file
func (s *Store) SaveMetadata(nodeID string, metadata interface{}, dataDir string) error {
	data, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	filename := filepath.Join(dataDir, fmt.Sprintf("node_%s_metadata.json", nodeID))
	return os.WriteFile(filename, data, 0644)
}

// LoadMetadata loads node metadata from a JSON file
func (s *Store) LoadMetadata(nodeID string, metadata interface{}, dataDir string) error {
	filename := filepath.Join(dataDir, fmt.Sprintf("node_%s_metadata.json", nodeID))

	data, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("failed to read metadata file: %w", err)
	}

	return json.Unmarshal(data, metadata)
}
