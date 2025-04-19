package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// WebSocketManager manages WebSocket connections and broadcasts
type WebSocketManager struct {
	// Clients holds all active WebSocket connections
	clients map[*websocket.Conn]bool
	
	// Broadcast channel for sending messages to all clients
	broadcast chan []byte
	
	// Register channel for new client connections
	register chan *websocket.Conn
	
	// Unregister channel for client disconnections
	unregister chan *websocket.Conn
	
	// Mutex for concurrent access to clients map
	mu sync.Mutex
	
	// WebSocket upgrader
	upgrader websocket.Upgrader
}

// NewWebSocketManager creates a new WebSocket manager
func NewWebSocketManager() *WebSocketManager {
	return &WebSocketManager{
		clients:    make(map[*websocket.Conn]bool),
		broadcast:  make(chan []byte),
		register:   make(chan *websocket.Conn),
		unregister: make(chan *websocket.Conn),
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			// Allow all origins
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		},
	}
}

// Start starts the WebSocket manager
func (manager *WebSocketManager) Start() {
	// Start the manager goroutine
	go func() {
		for {
			select {
			case client := <-manager.register:
				// Register new client
				manager.mu.Lock()
				manager.clients[client] = true
				manager.mu.Unlock()
				log.Printf("WebSocket client connected, total: %d", len(manager.clients))
				
			case client := <-manager.unregister:
				// Unregister client
				manager.mu.Lock()
				if _, ok := manager.clients[client]; ok {
					delete(manager.clients, client)
					client.Close()
				}
				manager.mu.Unlock()
				log.Printf("WebSocket client disconnected, total: %d", len(manager.clients))
				
			case message := <-manager.broadcast:
				// Broadcast message to all clients
				manager.mu.Lock()
				for client := range manager.clients {
					err := client.WriteMessage(websocket.TextMessage, message)
					if err != nil {
						log.Printf("WebSocket write error: %v", err)
						client.Close()
						delete(manager.clients, client)
					}
				}
				manager.mu.Unlock()
			}
		}
	}()
}

// HandleWebSocket handles WebSocket connections
func (manager *WebSocketManager) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	// Upgrade the HTTP connection to a WebSocket connection
	conn, err := manager.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket upgrade error: %v", err)
		return
	}
	
	// Register the client
	manager.register <- conn
	
	// Start client goroutine
	go func() {
		defer func() {
			manager.unregister <- conn
		}()
		
		// Simple ping/pong to keep connection alive
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					log.Printf("WebSocket read error: %v", err)
				}
				break
			}
		}
	}()
}

// BroadcastNetworkState broadcasts the current network state to all clients
func (manager *WebSocketManager) BroadcastNetworkState(state *NetworkState) {
	// Marshal the state to JSON
	data, err := json.Marshal(state)
	if err != nil {
		log.Printf("Error marshaling network state: %v", err)
		return
	}
	
	// Broadcast the state
	manager.broadcast <- data
}

// BroadcastEvent broadcasts a specific event to all clients
func (manager *WebSocketManager) BroadcastEvent(eventType string, payload interface{}) {
	// Create event object
	event := struct {
		Type      string      `json:"type"`
		Timestamp time.Time   `json:"timestamp"`
		Payload   interface{} `json:"payload"`
	}{
		Type:      eventType,
		Timestamp: time.Now(),
		Payload:   payload,
	}
	
	// Marshal the event to JSON
	data, err := json.Marshal(event)
	if err != nil {
		log.Printf("Error marshaling event: %v", err)
		return
	}
	
	// Broadcast the event
	manager.broadcast <- data
}

// StartPeriodicUpdates starts sending periodic updates to clients
func (manager *WebSocketManager) StartPeriodicUpdates() {
	// Start goroutine to send updates every 1 second
	ticker := time.NewTicker(1 * time.Second)
	
	go func() {
		for range ticker.C {
			// Lock the state to get a consistent snapshot
			stateMutex.RLock()
			state := networkState
			stateMutex.RUnlock()
			
			// Broadcast the current state
			manager.BroadcastNetworkState(&state)
		}
	}()
} 