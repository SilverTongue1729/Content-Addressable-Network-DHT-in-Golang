package security

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Permission represents a specific action that can be performed
type Permission string

// Common permissions
const (
	PermissionRead  Permission = "read"  // GET operations
	PermissionWrite Permission = "write" // PUT operations
	PermissionAdmin Permission = "admin" // Admin operations (topology changes, etc)
)

// Role represents a set of permissions that can be assigned to an API key
type Role struct {
	Name        string
	Permissions map[Permission]bool
}

// CommonRoles defines standard roles
var CommonRoles = map[string]Role{
	"reader": {
		Name: "reader",
		Permissions: map[Permission]bool{
			PermissionRead: true,
		},
	},
	"writer": {
		Name: "writer",
		Permissions: map[Permission]bool{
			PermissionRead:  true,
			PermissionWrite: true,
		},
	},
	"admin": {
		Name: "admin",
		Permissions: map[Permission]bool{
			PermissionRead:  true,
			PermissionWrite: true,
			PermissionAdmin: true,
		},
	},
}

// APIKey represents an authentication key with associated permissions
type APIKey struct {
	Key         string
	Role        string
	Description string
	Created     time.Time
	LastUsed    time.Time
}

// AccessController manages API key authentication and authorization
type AccessController struct {
	apiKeys map[string]APIKey
	roles   map[string]Role
	mu      sync.RWMutex
}

// NewAccessController creates a new access controller
func NewAccessController() *AccessController {
	ac := &AccessController{
		apiKeys: make(map[string]APIKey),
		roles:   make(map[string]Role),
	}
	
	// Add common roles
	for name, role := range CommonRoles {
		ac.roles[name] = role
	}
	
	// Add a default admin key for initial setup
	defaultKey := "admin-" + fmt.Sprintf("%d", time.Now().UnixNano())
	ac.AddAPIKey(defaultKey, "admin", "Default admin key")
	
	return ac
}

// AddAPIKey adds a new API key with the specified role
func (ac *AccessController) AddAPIKey(key, roleName, description string) error {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	
	role, exists := ac.roles[roleName]
	if !exists {
		return fmt.Errorf("role not found: %s", roleName)
	}
	
	ac.apiKeys[key] = APIKey{
		Key:         key,
		Role:        roleName,
		Description: description,
		Created:     time.Now(),
		LastUsed:    time.Now(),
	}
	
	return nil
}

// RevokeAPIKey revokes an API key
func (ac *AccessController) RevokeAPIKey(key string) {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	
	delete(ac.apiKeys, key)
}

// AddRole adds a custom role
func (ac *AccessController) AddRole(name string, permissions []Permission) {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	
	permMap := make(map[Permission]bool)
	for _, perm := range permissions {
		permMap[perm] = true
	}
	
	ac.roles[name] = Role{
		Name:        name,
		Permissions: permMap,
	}
}

// CheckPermission checks if the API key has the specified permission
func (ac *AccessController) CheckPermission(key string, permission Permission) bool {
	ac.mu.RLock()
	defer ac.mu.RUnlock()
	
	apiKey, exists := ac.apiKeys[key]
	if !exists {
		return false
	}
	
	// Update last used time
	apiKey.LastUsed = time.Now()
	ac.apiKeys[key] = apiKey
	
	role, exists := ac.roles[apiKey.Role]
	if !exists {
		return false
	}
	
	return role.Permissions[permission]
}

// AuthContext is the key type for auth data in contexts
type AuthContext struct{}

// WithAPIKey adds API key to context
func WithAPIKey(ctx context.Context, key string) context.Context {
	return context.WithValue(ctx, AuthContext{}, key)
}

// GetAPIKey gets API key from context
func GetAPIKey(ctx context.Context) string {
	value := ctx.Value(AuthContext{})
	if value == nil {
		return ""
	}
	return value.(string)
} 