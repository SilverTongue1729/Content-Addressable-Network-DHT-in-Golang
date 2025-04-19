package service

import (
	"sync"
	"time"
)

// CacheMetrics tracks cache performance metrics
type CacheMetrics struct {
	Hits            int
	Misses          int
	Evictions       int
	Expirations     int
	AverageLatency  time.Duration
	HitRatio        float64
	TotalOperations int
}

// LRUNode represents a node in the LRU cache
type LRUNode struct {
	Key             string
	Value           []byte
	ExpiresAt       time.Time
	AccessCount     int
	LastAccessed    time.Time
	CreatedAt       time.Time
	Next            *LRUNode
	Prev            *LRUNode
	IsReplica       bool
	IsHotKeyReplica bool
}

// LRUCache implements a Least Recently Used cache with adaptive TTL
type LRUCache struct {
	// Map for O(1) lookup
	cache map[string]*LRUNode
	
	// Doubly linked list for LRU ordering
	head *LRUNode
	tail *LRUNode
	
	// Maximum capacity
	capacity int
	
	// Default TTL for entries
	defaultTTL time.Duration
	
	// Minimum TTL for adaptive TTL adjustment
	minTTL time.Duration
	
	// Maximum TTL for adaptive TTL adjustment
	maxTTL time.Duration
	
	// Current size
	size int
	
	// Hot key threshold
	hotKeyThreshold int
	
	// Metrics
	metrics CacheMetrics
	
	// Prefetch callbacks
	prefetchCallback func(key string) ([]byte, error)
	
	// Background cleanup and prefetch
	stopCleanup      chan struct{}
	stopPrefetch     chan struct{}
	
	mu sync.RWMutex
}

// NewLRUCache creates a new LRU cache with the given capacity and TTL
func NewLRUCache(capacity int, defaultTTL time.Duration) *LRUCache {
	cache := &LRUCache{
		cache:            make(map[string]*LRUNode),
		capacity:         capacity,
		defaultTTL:       defaultTTL,
		minTTL:           10 * time.Second,       // Minimum TTL of 10 seconds
		maxTTL:           24 * time.Hour,         // Maximum TTL of 24 hours
		hotKeyThreshold:  10,                     // Consider keys hot after 10 accesses
		stopCleanup:      make(chan struct{}),
		stopPrefetch:     make(chan struct{}),
	}
	
	// Initialize head and tail sentinels
	cache.head = &LRUNode{}
	cache.tail = &LRUNode{}
	cache.head.Next = cache.tail
	cache.tail.Prev = cache.head
	
	// Start background cleanup goroutine
	go cache.periodicCleanup()
	
	return cache
}

// Get retrieves a value from the cache
func (c *LRUCache) Get(key string) ([]byte, bool) {
	c.mu.RLock()
	node, exists := c.cache[key]
	c.mu.RUnlock()
	
	if !exists {
		c.mu.Lock()
		c.metrics.Misses++
		c.metrics.TotalOperations++
		c.mu.Unlock()
		return nil, false
	}
	
	// Check if the entry has expired
	c.mu.Lock()
	defer c.mu.Unlock()
	
	if time.Now().After(node.ExpiresAt) {
		c.removeNode(node)
		delete(c.cache, key)
		c.size--
		c.metrics.Expirations++
		c.metrics.Misses++
		c.metrics.TotalOperations++
		return nil, false
	}
	
	// Update access stats
	node.AccessCount++
	node.LastAccessed = time.Now()
	
	// Move to front (most recently used)
	c.moveToFront(node)
	
	// Update metrics
	c.metrics.Hits++
	c.metrics.TotalOperations++
	c.metrics.HitRatio = float64(c.metrics.Hits) / float64(c.metrics.TotalOperations)
	
	// Update TTL based on access pattern
	c.adaptTTL(node)
	
	// Return a copy of the value
	valueCopy := make([]byte, len(node.Value))
	copy(valueCopy, node.Value)
	
	return valueCopy, true
}

// Put adds or updates a key-value pair in the cache
func (c *LRUCache) Put(key string, value []byte, ttl time.Duration) {
	if ttl == 0 {
		ttl = c.defaultTTL
	}
	
	c.mu.Lock()
	defer c.mu.Unlock()
	
	// Check if the key already exists
	if node, exists := c.cache[key]; exists {
		// Update the value
		node.Value = make([]byte, len(value))
		copy(node.Value, value)
		node.ExpiresAt = time.Now().Add(ttl)
		node.AccessCount++
		node.LastAccessed = time.Now()
		
		// Move to front
		c.moveToFront(node)
	} else {
		// Create a new node
		node := &LRUNode{
			Key:          key,
			Value:        make([]byte, len(value)),
			ExpiresAt:    time.Now().Add(ttl),
			AccessCount:  1,
			LastAccessed: time.Now(),
			CreatedAt:    time.Now(),
		}
		copy(node.Value, value)
		
		// Add to cache
		c.cache[key] = node
		c.addToFront(node)
		c.size++
		
		// Evict if over capacity
		if c.size > c.capacity {
			c.evictLRU()
		}
	}
	
	c.metrics.TotalOperations++
}

// Delete removes a key from the cache
func (c *LRUCache) Delete(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	if node, exists := c.cache[key]; exists {
		c.removeNode(node)
		delete(c.cache, key)
		c.size--
		return true
	}
	
	return false
}

// GetMetrics returns the current cache metrics
func (c *LRUCache) GetMetrics() CacheMetrics {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	metrics := c.metrics
	return metrics
}

// Clear removes all entries from the cache
func (c *LRUCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	c.cache = make(map[string]*LRUNode)
	c.head.Next = c.tail
	c.tail.Prev = c.head
	c.size = 0
}

// Size returns the current size of the cache
func (c *LRUCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	return c.size
}

// GetHotKeys returns keys that are frequently accessed
func (c *LRUCache) GetHotKeys(threshold int) []string {
	if threshold == 0 {
		threshold = c.hotKeyThreshold
	}
	
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	hotKeys := make([]string, 0)
	for key, node := range c.cache {
		if node.AccessCount >= threshold {
			hotKeys = append(hotKeys, key)
		}
	}
	
	return hotKeys
}

// SetPrefetchCallback sets a callback function for prefetching missing keys
func (c *LRUCache) SetPrefetchCallback(callback func(key string) ([]byte, error)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	c.prefetchCallback = callback
	
	// Start prefetch goroutine if not already running
	go c.prefetchHotKeys()
}

// Stop stops background goroutines
func (c *LRUCache) Stop() {
	c.stopCleanup <- struct{}{}
	c.stopPrefetch <- struct{}{}
}

// Internal methods

// moveToFront moves a node to the front of the list (most recently used)
func (c *LRUCache) moveToFront(node *LRUNode) {
	c.removeNode(node)
	c.addToFront(node)
}

// addToFront adds a node to the front of the list
func (c *LRUCache) addToFront(node *LRUNode) {
	node.Next = c.head.Next
	node.Prev = c.head
	c.head.Next.Prev = node
	c.head.Next = node
}

// removeNode removes a node from the list
func (c *LRUCache) removeNode(node *LRUNode) {
	node.Prev.Next = node.Next
	node.Next.Prev = node.Prev
}

// evictLRU evicts the least recently used item
func (c *LRUCache) evictLRU() {
	// Tail.Prev is the least recently used item
	nodeToRemove := c.tail.Prev
	if nodeToRemove == c.head {
		// List is empty
		return
	}
	
	c.removeNode(nodeToRemove)
	delete(c.cache, nodeToRemove.Key)
	c.size--
	c.metrics.Evictions++
}

// adaptTTL adjusts the TTL based on access patterns
func (c *LRUCache) adaptTTL(node *LRUNode) {
	// Adjust TTL based on access frequency
	// The more frequently accessed, the longer the TTL
	accessRate := float64(node.AccessCount) / max(float64(time.Since(node.CreatedAt).Seconds()), 1.0)
	
	var newTTL time.Duration
	if accessRate > 1.0 {
		// Frequently accessed, extend TTL
		factor := min(accessRate, 10.0) // Cap the factor at 10x
		newTTL = time.Duration(float64(c.defaultTTL) * factor)
		if newTTL > c.maxTTL {
			newTTL = c.maxTTL
		}
	} else {
		// Less frequently accessed, reduce TTL
		newTTL = time.Duration(float64(c.defaultTTL) * max(accessRate, 0.5))
		if newTTL < c.minTTL {
			newTTL = c.minTTL
		}
	}
	
	// Update expiration time
	timeToExpire := time.Until(node.ExpiresAt)
	if newTTL > timeToExpire {
		node.ExpiresAt = time.Now().Add(newTTL)
	}
}

// periodicCleanup removes expired entries periodically
func (c *LRUCache) periodicCleanup() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			c.cleanupExpired()
		case <-c.stopCleanup:
			return
		}
	}
}

// cleanupExpired removes all expired entries
func (c *LRUCache) cleanupExpired() {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	now := time.Now()
	keysToRemove := make([]string, 0)
	
	for key, node := range c.cache {
		if now.After(node.ExpiresAt) {
			keysToRemove = append(keysToRemove, key)
		}
	}
	
	for _, key := range keysToRemove {
		if node, exists := c.cache[key]; exists {
			c.removeNode(node)
			delete(c.cache, key)
			c.size--
			c.metrics.Expirations++
		}
	}
}

// prefetchHotKeys periodically prefetches hot keys
func (c *LRUCache) prefetchHotKeys() {
	if c.prefetchCallback == nil {
		return
	}
	
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			// Get hot keys
			hotKeys := c.GetHotKeys(c.hotKeyThreshold)
			
			// Asynchronously fetch related keys (predictive prefetching)
			for _, key := range hotKeys {
				go func(k string) {
					// This is a simple example - in a real system, you would
					// have logic to determine related keys to prefetch
					relatedKey := k + "-related"
					
					// Check if already in cache
					c.mu.RLock()
					_, exists := c.cache[relatedKey]
					c.mu.RUnlock()
					
					if !exists && c.prefetchCallback != nil {
						// Fetch related key
						value, err := c.prefetchCallback(relatedKey)
						if err == nil && value != nil {
							c.Put(relatedKey, value, c.defaultTTL)
						}
					}
				}(key)
			}
		case <-c.stopPrefetch:
			return
		}
	}
}

// Helper functions

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
} 