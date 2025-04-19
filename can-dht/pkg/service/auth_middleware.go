package service

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/can-dht/pkg/crypto"
	"github.com/gin-gonic/gin"
)

// AuthMiddleware provides authentication and authorization middleware for Gin routes
type AuthMiddleware struct {
	authManager *crypto.EnhancedAuthManager
}

// NewAuthMiddleware creates a new authentication middleware
func NewAuthMiddleware(authManager *crypto.EnhancedAuthManager) *AuthMiddleware {
	return &AuthMiddleware{
		authManager: authManager,
	}
}

// Authenticate verifies user credentials from Basic Auth header
func (am *AuthMiddleware) Authenticate() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Get Basic Auth credentials
		username, password, hasAuth := c.Request.BasicAuth()
		if !hasAuth {
			c.Header("WWW-Authenticate", "Basic realm=CAN-DHT")
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "authentication required",
			})
			return
		}

		// Authenticate the user
		authenticated, err := am.authManager.AuthenticateUser(username, password)
		if err != nil || !authenticated {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "invalid credentials",
			})
			return
		}

		// Store user info in the context for later use
		c.Set("username", username)
		c.Next()
	}
}

// RequirePermission checks if the user has the required permission for the resource
func (am *AuthMiddleware) RequirePermission(resourceOwner, resourceKey string, requiredPerm crypto.Permission) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Get username from context (set by Authenticate middleware)
		username, exists := c.Get("username")
		if !exists {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "authentication required",
			})
			return
		}

		// If resourceOwner or resourceKey contains path parameters, extract them
		if strings.HasPrefix(resourceOwner, ":") {
			resourceOwner = c.Param(resourceOwner[1:])
		}
		if strings.HasPrefix(resourceKey, ":") {
			resourceKey = c.Param(resourceKey[1:])
		}

		// Get user's password from Basic Auth
		_, password, _ := c.Request.BasicAuth()

		// Check permission
		permission, err := am.authManager.GetPermission(username.(string), password, resourceOwner, resourceKey)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
				"error": fmt.Sprintf("permission check error: %v", err),
			})
			return
		}

		// Verify permission
		if permission&requiredPerm == 0 {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
				"error": "insufficient permissions",
			})
			return
		}

		c.Next()
	}
}

// AdminOnly middleware ensures only users with admin permission can access
func (am *AuthMiddleware) AdminOnly() gin.HandlerFunc {
	return func(c *gin.Context) {
		username, exists := c.Get("username")
		if !exists {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "authentication required",
			})
			return
		}

		// In a real system, you would check against the database
		// This is simplified to check only specific admin users
		isAdmin := false
		adminUsers := []string{"admin"}
		for _, admin := range adminUsers {
			if username.(string) == admin {
				isAdmin = true
				break
			}
		}

		if !isAdmin {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
				"error": "admin privileges required",
			})
			return
		}

		c.Next()
	}
} 