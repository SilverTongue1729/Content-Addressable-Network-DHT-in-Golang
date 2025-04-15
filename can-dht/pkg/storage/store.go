package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
)

// Store represents a key-value store for a node
type Store struct {
	db        *badger.DB
	cache     map[string]cacheEntry
	cacheMu   sync.RWMutex
	cacheTTL  time.Duration
	cacheSize int
	maxCache  int
}

type cacheEntry struct {
	Value     []byte
	ExpiresAt time.Time
}

// StoreOptions contains configuration for the store
type StoreOptions struct {
	// DataDir is the directory where the data will be stored
	DataDir string

	// CacheTTL is the time-to-live for cached entries
	CacheTTL time.Duration

	// MaxCacheSize is the maximum number of entries in the cache
	MaxCacheSize int
}

// DefaultStoreOptions returns the default store options
func DefaultStoreOptions() StoreOptions {
	return StoreOptions{
		DataDir:      "data",
		CacheTTL:     5 * time.Minute,
		MaxCacheSize: 1000,
	}
}

// NewStore creates a new storage instance
func NewStore(options StoreOptions) (*Store, error) {
	// Create data directory if it doesn't exist
	if err := os.MkdirAll(options.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	// Open Badger database
	opts := badger.DefaultOptions(options.DataDir)
	opts.Logger = nil // Disable logging

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	return &Store{
		db:        db,
		cache:     make(map[string]cacheEntry),
		cacheTTL:  options.CacheTTL,
		maxCache:  options.MaxCacheSize,
		cacheSize: 0,
	}, nil
}

// Close closes the store
func (s *Store) Close() error {
	return s.db.Close()
}

// Put stores a value for a key
func (s *Store) Put(key string, value []byte) error {
	// Update cache
	s.cacheMu.Lock()
	s.cache[key] = cacheEntry{
		Value:     value,
		ExpiresAt: time.Now().Add(s.cacheTTL),
	}

	// Handle cache eviction if needed
	if s.cacheSize >= s.maxCache {
		// Simple eviction strategy: clear half the cache
		count := 0
		for k := range s.cache {
			if count > s.maxCache/2 {
				break
			}
			delete(s.cache, k)
			count++
		}
		s.cacheSize = s.cacheSize - count
	} else {
		s.cacheSize++
	}
	s.cacheMu.Unlock()

	// Store in database
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), value)
	})
}

// Get retrieves a value for a key
func (s *Store) Get(key string) ([]byte, bool, error) {
	// Check cache first
	s.cacheMu.RLock()
	entry, found := s.cache[key]
	if found && time.Now().Before(entry.ExpiresAt) {
		s.cacheMu.RUnlock()
		return entry.Value, true, nil
	}
	s.cacheMu.RUnlock()

	// Not in cache or expired, fetch from database
	var value []byte
	var exists bool

	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err == badger.ErrKeyNotFound {
			exists = false
			return nil
		}
		if err != nil {
			return err
		}

		exists = true
		value, err = item.ValueCopy(nil)
		return err
	})

	if err != nil {
		return nil, false, err
	}

	// If found, update cache
	if exists {
		s.cacheMu.Lock()
		s.cache[key] = cacheEntry{
			Value:     value,
			ExpiresAt: time.Now().Add(s.cacheTTL),
		}
		if s.cacheSize < s.maxCache {
			s.cacheSize++
		}
		s.cacheMu.Unlock()
	}

	return value, exists, nil
}

// GetAllKeys returns all keys in the store
func (s *Store) GetAllKeys() ([]string, error) {
	s.cacheMu.RLock()
	defer s.cacheMu.RUnlock()

	var keys []string
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())
			keys = append(keys, key)
		}
		return nil
	})

	return keys, err
}

// Delete removes a key-value pair
func (s *Store) Delete(key string) error {
	// Remove from cache
	s.cacheMu.Lock()
	if _, exists := s.cache[key]; exists {
		delete(s.cache, key)
		s.cacheSize--
	}
	s.cacheMu.Unlock()

	// Remove from database
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
}

// GetAll returns all key-value pairs in the store
func (s *Store) GetAll() (map[string][]byte, error) {
	result := make(map[string][]byte)

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())

			valueErr := item.Value(func(val []byte) error {
				// Make a copy of the value
				result[key] = append([]byte{}, val...)
				return nil
			})

			if valueErr != nil {
				return valueErr
			}
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
