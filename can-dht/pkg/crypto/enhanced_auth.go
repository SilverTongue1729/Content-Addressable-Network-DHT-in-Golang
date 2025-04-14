package crypto

import (
	"crypto/rand"
	"fmt"
	"strings"
	"sync"
)

// Permission represents a CRUD permission
type Permission uint8

const (
	PermissionNone   Permission = 0
	PermissionRead   Permission = 1 << 0
	PermissionCreate Permission = 1 << 1
	PermissionUpdate Permission = 1 << 2
	PermissionDelete Permission = 1 << 3
	PermissionAdmin  Permission = 1 << 4
	PermissionAll    Permission = PermissionRead | PermissionCreate | PermissionUpdate | PermissionDelete | PermissionAdmin
)

// UserPermission stores permission info for a user on a specific data object
type UserPermission struct {
	UserID     string
	Permission Permission
}

// DataEntry represents a stored data item with its metadata
type DataEntry struct {
	// The owner of the data
	Owner string

	// The encrypted data
	EncryptedData []byte

	// HMAC for integrity verification
	HMAC []byte

	// Access control list with permissions
	Permissions map[string]Permission
}

// EnhancedAuthManager handles user authentication, key management, and permissions
type EnhancedAuthManager struct {
	// Store user credentials (username -> password hash)
	userCredentials map[string]string

	// Store user-specific encryption keys (username -> encryption key)
	userKeys map[string][]byte

	// Store data (namespace:dataID -> DataEntry)
	dataStore map[string]*DataEntry

	// Store data keys (namespace:dataID -> map[username]encryptedDataKey)
	dataKeys map[string]map[string][]byte

	mu sync.RWMutex
}

// NewEnhancedAuthManager creates a new enhanced authentication manager
func NewEnhancedAuthManager() *EnhancedAuthManager {
	return &EnhancedAuthManager{
		userCredentials: make(map[string]string),
		userKeys:        make(map[string][]byte),
		dataStore:       make(map[string]*DataEntry),
		dataKeys:        make(map[string]map[string][]byte),
	}
}

// RegisterUser registers a new user with a password
func (e *EnhancedAuthManager) RegisterUser(username, password string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.userCredentials[username]; exists {
		return fmt.Errorf("user %s already exists", username)
	}

	// Hash the password (in a real system, use bcrypt)
	passwordHash := hashPassword(password)

	// Generate a user-specific encryption key
	userKey := make([]byte, 32) // 256-bit key
	if _, err := rand.Read(userKey); err != nil {
		return fmt.Errorf("failed to generate user key: %w", err)
	}

	// Store user credentials and key
	e.userCredentials[username] = passwordHash
	e.userKeys[username] = userKey

	return nil
}

// AuthenticateUser authenticates a user with their password
func (e *EnhancedAuthManager) AuthenticateUser(username, password string) (bool, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	storedHash, exists := e.userCredentials[username]
	if !exists {
		return false, fmt.Errorf("user %s not found", username)
	}

	providedHash := hashPassword(password)
	return storedHash == providedHash, nil
}

// CreateData creates a new data item with permissions
func (e *EnhancedAuthManager) CreateData(creator, username, password, key string, value []byte,
	allowedUsers []UserPermission) error {

	// Authenticate user
	authenticated, err := e.AuthenticateUser(username, password)
	if err != nil {
		return err
	}
	if !authenticated {
		return fmt.Errorf("authentication failed for user %s", username)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// Create namespace key (username:key)
	namespacedKey := e.namespaceKey(username, key)

	// Check if data already exists
	if _, exists := e.dataStore[namespacedKey]; exists {
		return fmt.Errorf("data with key %s already exists for user %s", key, username)
	}

	// Create a new data key for this data
	dataKey := make([]byte, 32) // 256-bit key
	if _, err := rand.Read(dataKey); err != nil {
		return fmt.Errorf("failed to generate data key: %w", err)
	}

	// Encrypt the value with the data key
	keyManager, err := NewKeyManagerFromKeys(dataKey, dataKey)
	if err != nil {
		return fmt.Errorf("failed to create key manager: %w", err)
	}

	secureData, err := keyManager.EncryptAndAuthenticate(value)
	if err != nil {
		return fmt.Errorf("failed to encrypt data: %w", err)
	}

	// Initialize permissions
	permissions := make(map[string]Permission)

	// Creator gets all permissions
	permissions[username] = PermissionAll

	// Add allowed users with their permissions
	for _, userPerm := range allowedUsers {
		// Verify user exists
		if _, exists := e.userCredentials[userPerm.UserID]; !exists {
			return fmt.Errorf("user %s not found", userPerm.UserID)
		}

		permissions[userPerm.UserID] = userPerm.Permission
	}

	// Create data entry
	dataEntry := &DataEntry{
		Owner:         username,
		EncryptedData: secureData.Ciphertext,
		HMAC:          secureData.HMAC,
		Permissions:   permissions,
	}

	// Store data entry
	e.dataStore[namespacedKey] = dataEntry

	// Initialize data keys map
	e.dataKeys[namespacedKey] = make(map[string][]byte)

	// Encrypt data key for each allowed user and store
	for userID := range permissions {
		userKey := e.userKeys[userID]
		encryptedDataKey, err := encryptKeyWithKey(dataKey, userKey)
		if err != nil {
			return fmt.Errorf("failed to encrypt data key for user %s: %w", userID, err)
		}
		e.dataKeys[namespacedKey][userID] = encryptedDataKey
	}

	return nil
}

// ReadData reads a data item if the user has permission
func (e *EnhancedAuthManager) ReadData(username, password, ownerName, key string) ([]byte, error) {
	// Authenticate user
	authenticated, err := e.AuthenticateUser(username, password)
	if err != nil {
		return nil, err
	}
	if !authenticated {
		return nil, fmt.Errorf("authentication failed for user %s", username)
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	// Get namespaced key
	namespacedKey := e.namespaceKey(ownerName, key)

	// Check if data exists
	dataEntry, exists := e.dataStore[namespacedKey]
	if !exists {
		return nil, fmt.Errorf("data with key %s not found for user %s", key, ownerName)
	}

	// Check if user has read permission
	permission, hasAccess := dataEntry.Permissions[username]
	if !hasAccess || (permission&PermissionRead) == 0 {
		return nil, fmt.Errorf("user %s does not have read permission for this data", username)
	}

	// Get user's key
	userKey := e.userKeys[username]

	// Get encrypted data key for this user
	encryptedDataKey, exists := e.dataKeys[namespacedKey][username]
	if !exists {
		return nil, fmt.Errorf("no data key found for user %s", username)
	}

	// Decrypt the data key
	dataKey, err := decryptKeyWithKey(encryptedDataKey, userKey)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt data key: %w", err)
	}

	// Create key manager
	keyManager, err := NewKeyManagerFromKeys(dataKey, dataKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create key manager: %w", err)
	}

	// Create secure data structure
	secureData := &SecureData{
		Ciphertext: dataEntry.EncryptedData,
		HMAC:       dataEntry.HMAC,
	}

	// Decrypt and verify
	plaintext, err := keyManager.DecryptAndVerify(secureData)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt data: %w", err)
	}

	return plaintext, nil
}

// UpdateData updates a data item if the user has permission
func (e *EnhancedAuthManager) UpdateData(username, password, ownerName, key string,
	newValue []byte) error {

	// Authenticate user
	authenticated, err := e.AuthenticateUser(username, password)
	if err != nil {
		return err
	}
	if !authenticated {
		return fmt.Errorf("authentication failed for user %s", username)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// Get namespaced key
	namespacedKey := e.namespaceKey(ownerName, key)

	// Check if data exists
	dataEntry, exists := e.dataStore[namespacedKey]
	if !exists {
		return fmt.Errorf("data with key %s not found for user %s", key, ownerName)
	}

	// Check if user has update permission
	permission, hasAccess := dataEntry.Permissions[username]
	if !hasAccess || (permission&PermissionUpdate) == 0 {
		return fmt.Errorf("user %s does not have update permission for this data", username)
	}

	// Get user's key
	userKey := e.userKeys[username]

	// Get encrypted data key for this user
	encryptedDataKey, exists := e.dataKeys[namespacedKey][username]
	if !exists {
		return fmt.Errorf("no data key found for user %s", username)
	}

	// Decrypt the data key
	dataKey, err := decryptKeyWithKey(encryptedDataKey, userKey)
	if err != nil {
		return fmt.Errorf("failed to decrypt data key: %w", err)
	}

	// Create key manager
	keyManager, err := NewKeyManagerFromKeys(dataKey, dataKey)
	if err != nil {
		return fmt.Errorf("failed to create key manager: %w", err)
	}

	// Encrypt the new value
	secureData, err := keyManager.EncryptAndAuthenticate(newValue)
	if err != nil {
		return fmt.Errorf("failed to encrypt data: %w", err)
	}

	// Update data entry
	dataEntry.EncryptedData = secureData.Ciphertext
	dataEntry.HMAC = secureData.HMAC

	return nil
}

// DeleteData deletes a data item if the user has permission
func (e *EnhancedAuthManager) DeleteData(username, password, ownerName, key string) error {
	// Authenticate user
	authenticated, err := e.AuthenticateUser(username, password)
	if err != nil {
		return err
	}
	if !authenticated {
		return fmt.Errorf("authentication failed for user %s", username)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// Get namespaced key
	namespacedKey := e.namespaceKey(ownerName, key)

	// Check if data exists
	dataEntry, exists := e.dataStore[namespacedKey]
	if !exists {
		return fmt.Errorf("data with key %s not found for user %s", key, ownerName)
	}

	// Check if user has delete permission
	permission, hasAccess := dataEntry.Permissions[username]
	if !hasAccess || (permission&PermissionDelete) == 0 {
		return fmt.Errorf("user %s does not have delete permission for this data", username)
	}

	// Delete data
	delete(e.dataStore, namespacedKey)
	delete(e.dataKeys, namespacedKey)

	return nil
}

// CreateOwnData is a simplified method for creating data that a user owns
func (e *EnhancedAuthManager) CreateOwnData(username, password, key string, value []byte) error {
	return e.CreateData(username, username, password, key, value, nil)
}

// ReadOwnData is a simplified method for reading data that a user owns
func (e *EnhancedAuthManager) ReadOwnData(username, password, key string) ([]byte, error) {
	return e.ReadData(username, password, username, key)
}

// UpdateOwnData is a simplified method for updating data that a user owns
func (e *EnhancedAuthManager) UpdateOwnData(username, password, key string, newValue []byte) error {
	return e.UpdateData(username, password, username, key, newValue)
}

// DeleteOwnData is a simplified method for deleting data that a user owns
func (e *EnhancedAuthManager) DeleteOwnData(username, password, key string) error {
	return e.DeleteData(username, password, username, key)
}

// ModifyPermissions modifies a user's permissions for a data item
func (e *EnhancedAuthManager) ModifyPermissions(adminUser, adminPassword, targetUser string,
	ownerName, key string, newPermission Permission) error {

	// Authenticate admin
	authenticated, err := e.AuthenticateUser(adminUser, adminPassword)
	if err != nil {
		return err
	}
	if !authenticated {
		return fmt.Errorf("authentication failed for user %s", adminUser)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// Get namespaced key
	namespacedKey := e.namespaceKey(ownerName, key)

	// Check if data exists
	dataEntry, exists := e.dataStore[namespacedKey]
	if !exists {
		return fmt.Errorf("data with key %s not found for user %s", key, ownerName)
	}

	// Check if admin user has admin permission
	adminPermission, hasAccess := dataEntry.Permissions[adminUser]
	if !hasAccess || (adminPermission&PermissionAdmin) == 0 {
		return fmt.Errorf("user %s does not have admin permission for this data", adminUser)
	}

	// Check if target user exists
	if _, exists := e.userCredentials[targetUser]; !exists {
		return fmt.Errorf("target user %s not found", targetUser)
	}

	// Get current permissions
	_, targetExists := dataEntry.Permissions[targetUser]

	// If user is getting new permissions and didn't have them before
	if newPermission != PermissionNone && !targetExists {
		// Get the data key from admin's encrypted version
		adminKey := e.userKeys[adminUser]
		encryptedDataKey := e.dataKeys[namespacedKey][adminUser]
		dataKey, err := decryptKeyWithKey(encryptedDataKey, adminKey)
		if err != nil {
			return fmt.Errorf("failed to decrypt data key: %w", err)
		}

		// Encrypt data key for target user
		targetUserKey := e.userKeys[targetUser]
		encryptedTargetKey, err := encryptKeyWithKey(dataKey, targetUserKey)
		if err != nil {
			return fmt.Errorf("failed to encrypt data key for target user: %w", err)
		}

		// Store encrypted data key for target user
		e.dataKeys[namespacedKey][targetUser] = encryptedTargetKey
	}

	if newPermission == PermissionNone {
		// Remove permissions completely
		delete(dataEntry.Permissions, targetUser)
		delete(e.dataKeys[namespacedKey], targetUser)
	} else {
		// Update permissions
		dataEntry.Permissions[targetUser] = newPermission
	}

	return nil
}

// ListDataKeys lists all data keys for a specified username
func (e *EnhancedAuthManager) ListDataKeys(username, password string) ([]string, error) {
	// Authenticate user
	authenticated, err := e.AuthenticateUser(username, password)
	if err != nil {
		return nil, err
	}
	if !authenticated {
		return nil, fmt.Errorf("authentication failed for user %s", username)
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	// Find all keys where user has at least read permission
	var keys []string
	prefix := username + ":"
	ownPrefix := true

	for namespacedKey, dataEntry := range e.dataStore {
		// Check if user has permission to access this data
		if permission, hasAccess := dataEntry.Permissions[username]; hasAccess && (permission&PermissionRead) != 0 {
			// Check if this is user's own data
			if strings.HasPrefix(namespacedKey, prefix) && ownPrefix {
				// Extract the original key (remove username: prefix)
				key := namespacedKey[len(prefix):]
				keys = append(keys, key)
			} else {
				// This is someone else's data the user has access to
				// In this case, include owner information in the key
				parts := strings.SplitN(namespacedKey, ":", 2)
				if len(parts) == 2 {
					keys = append(keys, parts[0]+"/"+parts[1]) // format: owner/key
				}
			}
		}
	}

	return keys, nil
}

// namespaceKey creates a namespaced key
func (e *EnhancedAuthManager) namespaceKey(username, key string) string {
	return username + ":" + key
}

// GetPermission gets a user's permissions for a data item
func (e *EnhancedAuthManager) GetPermission(username, password, ownerName, key string) (Permission, error) {
	// Authenticate user
	authenticated, err := e.AuthenticateUser(username, password)
	if err != nil {
		return PermissionNone, err
	}
	if !authenticated {
		return PermissionNone, fmt.Errorf("authentication failed for user %s", username)
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	// Get namespaced key
	namespacedKey := e.namespaceKey(ownerName, key)

	// Check if data exists
	dataEntry, exists := e.dataStore[namespacedKey]
	if !exists {
		return PermissionNone, fmt.Errorf("data with key %s not found for user %s", key, ownerName)
	}

	// Get user's permissions
	permission, hasAccess := dataEntry.Permissions[username]
	if !hasAccess {
		return PermissionNone, nil
	}

	return permission, nil
}

// hashPassword creates a simple hash of a password
// Note: In a production system, use a proper password hashing algorithm like bcrypt
func hashPassword(password string) string {
	// This is a very simple hash for demonstration purposes only
	// In a real system, use a proper password hashing algorithm like bcrypt
	hashedBytes := make([]byte, len(password))
	for i := 0; i < len(password); i++ {
		hashedBytes[i] = password[i] + 1
	}
	return string(hashedBytes)
}

// Helper functions for key encryption
func encryptKeyWithKey(dataKey, userKey []byte) ([]byte, error) {
	// For demo purposes, this is a simple XOR encryption
	// In a real system, use a proper encryption algorithm
	if len(userKey) < len(dataKey) {
		return nil, fmt.Errorf("user key too short")
	}

	encryptedKey := make([]byte, len(dataKey))
	for i := 0; i < len(dataKey); i++ {
		encryptedKey[i] = dataKey[i] ^ userKey[i]
	}
	return encryptedKey, nil
}

func decryptKeyWithKey(encryptedKey, userKey []byte) ([]byte, error) {
	// For demo purposes, just use the same XOR function
	return encryptKeyWithKey(encryptedKey, userKey)
}
