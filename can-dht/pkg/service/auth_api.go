package service

import (
	"net/http"

	"github.com/can-dht/pkg/crypto"
	"github.com/gin-gonic/gin"
)

// RegisterAuthAPI registers authentication-related API endpoints with a Gin router
func (s *CANServer) RegisterAuthAPI(router *gin.Engine) {
	// Setup authentication middleware
	authMiddleware := NewAuthMiddleware(s.AuthManager)

	// Create auth API group
	authGroup := router.Group("/auth")
	{
		// Register a new user (public endpoint)
		authGroup.POST("/register", s.registerUser)

		// Auth-required endpoints
		secureGroup := authGroup.Group("/")
		secureGroup.Use(authMiddleware.Authenticate())
		{
			// Get current user info
			secureGroup.GET("/me", s.getUserInfo)

			// Change password
			secureGroup.POST("/change-password", s.changePassword)
		}

		// Admin-only endpoints
		adminGroup := authGroup.Group("/admin")
		adminGroup.Use(authMiddleware.Authenticate(), authMiddleware.AdminOnly())
		{
			// List all users
			adminGroup.GET("/users", s.listUsers)

			// Delete a user
			adminGroup.DELETE("/users/:username", s.deleteUser)
		}
	}

	// Setup data API routes with authentication
	dataGroup := router.Group("/data")
	dataGroup.Use(authMiddleware.Authenticate())
	{
		// List all data accessible to the user
		dataGroup.GET("/", s.listUserData)

		// User's own data operations
		dataGroup.POST("/", s.createData)
		dataGroup.GET("/:key", s.getData)
		dataGroup.PUT("/:key", s.updateData)
		dataGroup.DELETE("/:key", s.deleteData)

		// Shared data operations
		sharedGroup := dataGroup.Group("/shared")
		{
			// Get data from another user
			sharedGroup.GET("/:owner/:key", s.getSharedData)

			// Update data belonging to another user (if allowed)
			sharedGroup.PUT("/:owner/:key", s.updateSharedData)

			// Delete data belonging to another user (if allowed)
			sharedGroup.DELETE("/:owner/:key", s.deleteSharedData)
		}

		// Permissions management
		permissionsGroup := dataGroup.Group("/permissions")
		{
			// Get permissions for a data key
			permissionsGroup.GET("/:key", s.getPermissions)

			// Grant permissions to another user
			permissionsGroup.POST("/:key/:username", s.grantPermissions)

			// Revoke permissions from another user
			permissionsGroup.DELETE("/:key/:username", s.revokePermissions)
		}
	}
}

// registerUser handles user registration requests
func (s *CANServer) registerUser(c *gin.Context) {
	// Check if authentication is enabled
	if s.AuthManager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "authentication is not enabled",
		})
		return
	}

	// Parse request body
	var req struct {
		Username string `json:"username" binding:"required"`
		Password string `json:"password" binding:"required"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "invalid request format",
		})
		return
	}

	// Register the user
	err := s.AuthManager.RegisterUser(req.Username, req.Password)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"message": "user registered successfully",
		"username": req.Username,
	})
}

// getUserInfo returns information about the current user
func (s *CANServer) getUserInfo(c *gin.Context) {
	username, _ := c.Get("username")

	// In a real implementation, we might fetch more user information here
	c.JSON(http.StatusOK, gin.H{
		"username": username,
	})
}

// changePassword handles password change requests
func (s *CANServer) changePassword(c *gin.Context) {
	username, _ := c.Get("username")
	usernameStr := username.(string)

	var req struct {
		OldPassword string `json:"old_password" binding:"required"`
		NewPassword string `json:"new_password" binding:"required"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "invalid request format",
		})
		return
	}

	// Verify old password
	authenticated, err := s.AuthManager.AuthenticateUser(usernameStr, req.OldPassword)
	if err != nil || !authenticated {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "current password is incorrect",
		})
		return
	}

	// In a real implementation, we would update the password here
	// For now, respond with a not implemented message
	c.JSON(http.StatusNotImplemented, gin.H{
		"message": "password change not implemented yet",
	})
}

// listUsers returns a list of all users (admin only)
func (s *CANServer) listUsers(c *gin.Context) {
	// In a real implementation, we would fetch the user list from the auth manager
	// For now, respond with a not implemented message
	c.JSON(http.StatusNotImplemented, gin.H{
		"message": "user listing not implemented yet",
	})
}

// deleteUser deletes a user (admin only)
func (s *CANServer) deleteUser(c *gin.Context) {
	username := c.Param("username")

	// In a real implementation, we would delete the user here
	// For now, respond with a not implemented message
	c.JSON(http.StatusNotImplemented, gin.H{
		"message": "user deletion not implemented yet",
		"username": username,
	})
}

// listUserData returns a list of all data accessible to the user
func (s *CANServer) listUserData(c *gin.Context) {
	username, _ := c.Get("username")
	usernameStr := username.(string)

	// Get user's password from Basic Auth
	_, password, _ := c.Request.BasicAuth()

	// List user's data
	keys, err := s.ListUserData(c, usernameStr, password)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"keys": keys,
	})
}

// createData creates a new data item
func (s *CANServer) createData(c *gin.Context) {
	username, _ := c.Get("username")
	usernameStr := username.(string)

	// Get user's password from Basic Auth
	_, password, _ := c.Request.BasicAuth()

	var req struct {
		Key   string `json:"key" binding:"required"`
		Value string `json:"value" binding:"required"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "invalid request format",
		})
		return
	}

	// Create data
	err := s.SecurePutWithAuth(c, usernameStr, password, req.Key, []byte(req.Value))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"message": "data created successfully",
		"key": req.Key,
	})
}

// getData retrieves a data item
func (s *CANServer) getData(c *gin.Context) {
	username, _ := c.Get("username")
	usernameStr := username.(string)

	// Get user's password from Basic Auth
	_, password, _ := c.Request.BasicAuth()

	key := c.Param("key")

	// Get data
	value, err := s.SecureGetWithAuth(c, usernameStr, password, "", key)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}

	if value == nil {
		c.JSON(http.StatusNotFound, gin.H{
			"error": "data not found",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"key": key,
		"value": string(value),
	})
}

// updateData updates a data item
func (s *CANServer) updateData(c *gin.Context) {
	username, _ := c.Get("username")
	usernameStr := username.(string)

	// Get user's password from Basic Auth
	_, password, _ := c.Request.BasicAuth()

	key := c.Param("key")

	var req struct {
		Value string `json:"value" binding:"required"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "invalid request format",
		})
		return
	}

	// In a real implementation, we would update the data here
	// For this example, we'll delete the old data and create new data
	err := s.SecureDeleteWithAuth(c, usernameStr, password, "", key)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "failed to delete existing data: " + err.Error(),
		})
		return
	}

	err = s.SecurePutWithAuth(c, usernameStr, password, key, []byte(req.Value))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "failed to update data: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "data updated successfully",
		"key": key,
	})
}

// deleteData deletes a data item
func (s *CANServer) deleteData(c *gin.Context) {
	username, _ := c.Get("username")
	usernameStr := username.(string)

	// Get user's password from Basic Auth
	_, password, _ := c.Request.BasicAuth()

	key := c.Param("key")

	// Delete data
	err := s.SecureDeleteWithAuth(c, usernameStr, password, "", key)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "data deleted successfully",
		"key": key,
	})
}

// getSharedData retrieves a data item shared by another user
func (s *CANServer) getSharedData(c *gin.Context) {
	username, _ := c.Get("username")
	usernameStr := username.(string)

	// Get user's password from Basic Auth
	_, password, _ := c.Request.BasicAuth()

	owner := c.Param("owner")
	key := c.Param("key")

	// Get shared data
	value, err := s.SecureGetWithAuth(c, usernameStr, password, owner, key)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}

	if value == nil {
		c.JSON(http.StatusNotFound, gin.H{
			"error": "data not found",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"owner": owner,
		"key": key,
		"value": string(value),
	})
}

// updateSharedData updates a data item shared by another user
func (s *CANServer) updateSharedData(c *gin.Context) {
	username, _ := c.Get("username")
	usernameStr := username.(string)

	// Get user's password from Basic Auth
	_, password, _ := c.Request.BasicAuth()

	owner := c.Param("owner")
	key := c.Param("key")

	var req struct {
		Value string `json:"value" binding:"required"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "invalid request format",
		})
		return
	}

	// Get current value to verify access
	_, err := s.SecureGetWithAuth(c, usernameStr, password, owner, key)
	if err != nil {
		c.JSON(http.StatusForbidden, gin.H{
			"error": "access denied: " + err.Error(),
		})
		return
	}

	// In a real implementation, we would need a proper UpdateData method
	// For now, return not implemented
	c.JSON(http.StatusNotImplemented, gin.H{
		"message": "shared data update not implemented yet",
	})
}

// deleteSharedData deletes a data item shared by another user
func (s *CANServer) deleteSharedData(c *gin.Context) {
	username, _ := c.Get("username")
	usernameStr := username.(string)

	// Get user's password from Basic Auth
	_, password, _ := c.Request.BasicAuth()

	owner := c.Param("owner")
	key := c.Param("key")

	// Delete shared data
	err := s.SecureDeleteWithAuth(c, usernameStr, password, owner, key)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "shared data deleted successfully",
		"owner": owner,
		"key": key,
	})
}

// getPermissions gets the permissions for a data item
func (s *CANServer) getPermissions(c *gin.Context) {
	username, _ := c.Get("username")
	usernameStr := username.(string)

	// Get user's password from Basic Auth
	_, password, _ := c.Request.BasicAuth()

	key := c.Param("key")

	// This would require a new method in the EnhancedAuthManager to list all permissions
	// For now, return not implemented
	c.JSON(http.StatusNotImplemented, gin.H{
		"message": "permission listing not implemented yet",
		"key": key,
	})
}

// grantPermissions grants permissions to another user
func (s *CANServer) grantPermissions(c *gin.Context) {
	username, _ := c.Get("username")
	usernameStr := username.(string)

	// Get user's password from Basic Auth
	_, password, _ := c.Request.BasicAuth()

	key := c.Param("key")
	targetUser := c.Param("username")

	var req struct {
		Permission uint8 `json:"permission" binding:"required"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "invalid request format",
		})
		return
	}

	// Grant permissions
	err := s.ModifyPermissions(c, usernameStr, password, targetUser, usernameStr, key, crypto.Permission(req.Permission))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "permissions granted successfully",
		"key": key,
		"user": targetUser,
		"permission": req.Permission,
	})
}

// revokePermissions revokes permissions from another user
func (s *CANServer) revokePermissions(c *gin.Context) {
	username, _ := c.Get("username")
	usernameStr := username.(string)

	// Get user's password from Basic Auth
	_, password, _ := c.Request.BasicAuth()

	key := c.Param("key")
	targetUser := c.Param("username")

	// Revoke permissions by setting to PermissionNone
	err := s.ModifyPermissions(c, usernameStr, password, targetUser, usernameStr, key, crypto.PermissionNone)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "permissions revoked successfully",
		"key": key,
		"user": targetUser,
	})
} 