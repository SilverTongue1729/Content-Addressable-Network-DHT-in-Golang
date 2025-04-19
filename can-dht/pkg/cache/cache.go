package cache

import (
	"container/list"
	"sync"
	"time"
)

// CacheEntry represents a value in the cache with its expiration time
type CacheEntry struct {
	Value      []byte
	Expiration time.Time
}

// CacheMetrics tracks statistics about cache operations
type CacheMetrics struct {
	Hits      uint64
	Misses    uint64
	Evictions uint64
}

// LRUCache implements a thread-safe LRU cache with TTL
type LRUCache struct {
	// Maximum number of items in the cache
	capacity int
	// Default TTL for cache entries
	defaultTTL time.Duration
	// The cache storage
	items     map[string]*list.Element
	evictList *list.List
	// Cache metrics
	metrics CacheMetrics
	mu      sync.RWMutex
}

// entry is used internally to store key-value pairs in the eviction list
type entry struct {
	key   string
	value *CacheEntry
}

// NewLRUCache creates a new LRU cache with the specified capacity and default TTL
func NewLRUCache(capacity int, defaultTTL time.Duration) *LRUCache {
	return &LRUCache{
		capacity:   capacity,
		defaultTTL: defaultTTL,
		items:      make(map[string]*list.Element),
		evictList:  list.New(),
		metrics:    CacheMetrics{},
	}
}

// Get retrieves a value from the cache, returning nil if not found or expired
func (c *LRUCache) Get(key string) ([]byte, bool) {
	c.mu.RLock()
	element, exists := c.items[key]
	c.mu.RUnlock()

	if !exists {
		c.mu.Lock()
		c.metrics.Misses++
		c.mu.Unlock()
		return nil, false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if element still exists (might have been removed by another goroutine)
	element, exists = c.items[key]
	if !exists {
		c.metrics.Misses++
		return nil, false
	}

	cacheEntry := element.Value.(*entry).value

	// Check if the entry has expired
	if time.Now().After(cacheEntry.Expiration) {
		c.evictList.Remove(element)
		delete(c.items, key)
		c.metrics.Evictions++
		c.metrics.Misses++
		return nil, false
	}

	// Move to front (most recently used)
	c.evictList.MoveToFront(element)
	c.metrics.Hits++
	return cacheEntry.Value, true
}

// Put adds or updates a value in the cache with the default TTL
func (c *LRUCache) Put(key string, value []byte) {
	c.PutWithTTL(key, value, c.defaultTTL)
}

// PutWithTTL adds or updates a value in the cache with a specific TTL
func (c *LRUCache) PutWithTTL(key string, value []byte, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Set expiration time
	expiration := time.Now().Add(ttl)
	
	// Check if key already exists
	if element, exists := c.items[key]; exists {
		c.evictList.MoveToFront(element)
		element.Value.(*entry).value = &CacheEntry{
			Value:      value,
			Expiration: expiration,
		}
		return
	}

	// Add new item
	ent := &entry{
		key: key,
		value: &CacheEntry{
			Value:      value,
			Expiration: expiration,
		},
	}
	element := c.evictList.PushFront(ent)
	c.items[key] = element

	// Evict if over capacity
	if c.evictList.Len() > c.capacity {
		c.removeOldest()
	}
}

// Delete removes a key from the cache
func (c *LRUCache) Delete(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	element, exists := c.items[key]
	if !exists {
		return false
	}

	c.evictList.Remove(element)
	delete(c.items, key)
	return true
}

// Clear empties the cache
func (c *LRUCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[string]*list.Element)
	c.evictList = list.New()
}

// Len returns the number of items in the cache
func (c *LRUCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.evictList.Len()
}

// GetMetrics returns a copy of the current metrics
func (c *LRUCache) GetMetrics() CacheMetrics {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.metrics
}

// removeOldest removes the oldest item from the cache
func (c *LRUCache) removeOldest() {
	element := c.evictList.Back()
	if element == nil {
		return
	}

	// Remove from list
	c.evictList.Remove(element)
	
	// Get the key and remove from items map
	kv := element.Value.(*entry)
	delete(c.items, kv.key)
	c.metrics.Evictions++
}

// CleanExpired removes all expired entries from the cache
func (c *LRUCache) CleanExpired() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	removed := 0

	for {
		element := c.evictList.Back()
		if element == nil {
			break
		}

		kv := element.Value.(*entry)
		if !now.After(kv.value.Expiration) {
			break
		}

		c.evictList.Remove(element)
		delete(c.items, kv.key)
		removed++
		c.metrics.Evictions++
	}

	return removed
} 