package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/onix-air/contacts/internal/model"
	"github.com/onix-air/contacts/internal/observability"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // In production, implement proper origin check
	},
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

const (
	QoSAtLeastOnce = 1
	AccessRead     = "READ"
	AccessWrite    = "WRITE"
)

type WebSocketService struct {
	redis          analyticsPublisher
	mqtt           mqttGateway
	access         accessChecker
	requestMgr     requestManager
	subsMgr        subscriptionManager
	logger         *log.Logger
	connections    sync.Map // map[string]*websocket.Conn
	connectionInfo sync.Map // map[string]connectionInfo
}

type connectionInfo struct {
	ClientID string
	Mode     string
}

type analyticsPublisher interface {
	PublishAnalyticsEvent(context.Context, *model.AnalyticsEvent) error
}

type mqttGateway interface {
	PublishCommand(interface{}, string) error
	GetClient() mqtt.Client
}

type accessChecker interface {
	Check(context.Context, string, string, string, string) (bool, error)
}

type requestManager interface {
	StoreRequest(context.Context, string, *model.RequestMetadata) error
	GetRequest(context.Context, string) (*model.RequestMetadata, error)
	DeleteRequest(context.Context, string) error
}

type subscriptionManager interface {
	AddSubscription(context.Context, string, string, string) error
	RemoveSubscription(context.Context, string, string, string) error
	GetSubscribers(context.Context, string, string) ([]string, error)
	SaveConnectionMetadata(context.Context, string, *model.SubscriptionMetadata) error
	GetConnectionMetadata(context.Context, string) (*model.SubscriptionMetadata, error)
	RemoveConnectionMetadata(context.Context, string) error
	RemoveAllConnectionSubscriptions(context.Context, string) error
}

var commandResponseTimeout = 10 * time.Second

func NewWebSocketService(
	redis analyticsPublisher,
	mqtt mqttGateway,
	access accessChecker,
	requestMgr requestManager,
	subsMgr subscriptionManager,
	logger *log.Logger,
) *WebSocketService {
	return &WebSocketService{
		redis:      redis,
		mqtt:       mqtt,
		access:     access,
		requestMgr: requestMgr,
		subsMgr:    subsMgr,
		logger:     logger,
	}
}

// UpgradeConnection upgrades HTTP connection to WebSocket
func (ws *WebSocketService) UpgradeConnection(w http.ResponseWriter, r *http.Request) (*websocket.Conn, error) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// StoreConnection stores connection in memory
func (ws *WebSocketService) StoreConnection(connectionID string, conn *websocket.Conn) {
	ws.connections.Store(connectionID, conn)
}

// StoreConnectionInfo stores analytics metadata for a websocket connection.
func (ws *WebSocketService) StoreConnectionInfo(connectionID, clientID, mode string) {
	ws.connectionInfo.Store(connectionID, connectionInfo{ClientID: clientID, Mode: mode})
}

// GetConnection retrieves connection from memory
func (ws *WebSocketService) GetConnection(connectionID string) (*websocket.Conn, bool) {
	val, ok := ws.connections.Load(connectionID)
	if !ok {
		return nil, false
	}
	return val.(*websocket.Conn), true
}

// RemoveConnection removes connection from memory and clean up subscriptions
func (ws *WebSocketService) RemoveConnection(ctx context.Context, connectionID string) {
	if value, ok := ws.connectionInfo.Load(connectionID); ok {
		info := value.(connectionInfo)
		ws.publishAnalytics(ctx, &model.AnalyticsEvent{
			EventID:      uuid.NewString(),
			Source:       "contacts",
			Action:       "connection_closed",
			OccurredAt:   time.Now().UTC().Format(time.RFC3339Nano),
			ClientID:     info.ClientID,
			ConnectionID: connectionID,
			Status:       "ok",
			Metadata: map[string]interface{}{
				"mode": info.Mode,
			},
		})
		ws.connectionInfo.Delete(connectionID)
	}

	ws.connections.Delete(connectionID)

	// Clean up subscriptions
	if err := ws.subsMgr.RemoveAllConnectionSubscriptions(ctx, connectionID); err != nil {
		ws.logger.Printf("%s Failed to remove subscriptions for %s: %v\n", observability.TraceFields(ctx), connectionID, err)
	}
	if err := ws.subsMgr.RemoveConnectionMetadata(ctx, connectionID); err != nil {
		ws.logger.Printf("%s Failed to remove metadata for %s: %v\n", observability.TraceFields(ctx), connectionID, err)
	}
}

// SendMessage sends message to connection
func (ws *WebSocketService) SendMessage(conn *websocket.Conn, message interface{}) error {
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	return conn.WriteJSON(message)
}

// ExecuteWrite handles write requests (via unified /ws)
func (ws *WebSocketService) ExecuteWrite(ctx context.Context, clientID string, req *model.WriteRequest, connectionID string) (*model.WSResponse, error) {
	// Validate request
	if req.RequestID == "" || req.ConsumerID == "" || req.ContractName == "" {
		ws.publishAnalytics(ctx, analyticsEvent(
			"invalid_message",
			clientID,
			connectionID,
			req.RequestID,
			req.ConsumerID,
			req.ContractName,
			"error",
			"INVALID_MESSAGE",
			"Invalid write request",
			req.Payload,
			nil,
		))
		return nil, fmt.Errorf("INVALID_MESSAGE")
	}

	ws.publishAnalytics(ctx, analyticsEvent(
		"write_request_received",
		clientID,
		connectionID,
		req.RequestID,
		req.ConsumerID,
		req.ContractName,
		"ok",
		"",
		"",
		req.Payload,
		nil,
	))

	// Check access
	allowed, err := ws.access.Check(ctx, clientID, req.ConsumerID, req.ContractName, AccessWrite)
	if err != nil {
		ws.logger.Printf("%s AccessService error: %v\n", observability.TraceFields(ctx), err)
		ws.publishAnalytics(ctx, analyticsEvent(
			"access_error",
			clientID,
			connectionID,
			req.RequestID,
			req.ConsumerID,
			req.ContractName,
			"error",
			"INTERNAL_ERROR",
			err.Error(),
			nil,
			nil,
		))
		return nil, fmt.Errorf("INTERNAL_ERROR")
	}
	if !allowed {
		ws.publishAnalytics(ctx, analyticsEvent(
			"access_denied",
			clientID,
			connectionID,
			req.RequestID,
			req.ConsumerID,
			req.ContractName,
			"denied",
			"ACCESS_DENIED",
			"Access denied",
			req.Payload,
			nil,
		))
		return nil, fmt.Errorf("ACCESS_DENIED")
	}

	// Store request in Redis with TTL
	metadata := &model.RequestMetadata{
		ClientID:     clientID,
		ConsumerID:   req.ConsumerID,
		ContractName: req.ContractName,
		ConnectionID: connectionID,
	}
	if err := ws.requestMgr.StoreRequest(ctx, req.RequestID, metadata); err != nil {
		ws.logger.Printf("%s Failed to store request: %v\n", observability.TraceFields(ctx), err)
		ws.publishAnalytics(ctx, analyticsEvent(
			"request_store_failed",
			clientID,
			connectionID,
			req.RequestID,
			req.ConsumerID,
			req.ContractName,
			"error",
			"INTERNAL_ERROR",
			err.Error(),
			req.Payload,
			nil,
		))
		return nil, fmt.Errorf("INTERNAL_ERROR")
	}

	// Publish command to MQTT
	cmdPayload := &model.MQTTCommandPayload{
		RequestID:    req.RequestID,
		ConsumerID:   req.ConsumerID,
		ContractName: req.ContractName,
		Payload:      req.Payload,
		Timestamp:    time.Now().UTC().Format(time.RFC3339),
	}

	if err := ws.mqtt.PublishCommand(cmdPayload, req.ConsumerID); err != nil {
		ws.logger.Printf("%s Failed to publish MQTT command: %v\n", observability.TraceFields(ctx), err)
		ws.requestMgr.DeleteRequest(ctx, req.RequestID)
		ws.publishAnalytics(ctx, analyticsEvent(
			"mqtt_publish_failed",
			clientID,
			connectionID,
			req.RequestID,
			req.ConsumerID,
			req.ContractName,
			"error",
			"MQTT_PUBLISH_FAILED",
			err.Error(),
			req.Payload,
			nil,
		))
		return nil, fmt.Errorf("MQTT_PUBLISH_FAILED")
	}

	ws.publishAnalytics(ctx, analyticsEvent(
		"mqtt_command_published",
		clientID,
		connectionID,
		req.RequestID,
		req.ConsumerID,
		req.ContractName,
		"ok",
		"",
		"",
		req.Payload,
		nil,
	))

	// Schedule timeout check
	time.AfterFunc(commandResponseTimeout, func() {
		bgCtx := context.Background()
		_, err := ws.requestMgr.GetRequest(bgCtx, req.RequestID)
		if err == nil {
			// Request still exists, meaning no response came back
			ws.requestMgr.DeleteRequest(bgCtx, req.RequestID)

			ws.logger.Printf("Command %s timed out waiting for device %s", req.RequestID, req.ConsumerID)

			ws.publishAnalytics(bgCtx, analyticsEvent(
				"command_timeout",
				clientID,
				connectionID,
				req.RequestID,
				req.ConsumerID,
				req.ContractName,
				"error",
				"TIMEOUT",
				"Device did not respond in time",
				req.Payload,
				nil,
			))

			if conn, ok := ws.GetConnection(connectionID); ok {
				wsResp := &model.WSResponse{
					Type:         "error",
					RequestID:    req.RequestID,
					ConsumerID:   req.ConsumerID,
					ContractName: req.ContractName,
					Code:         "TIMEOUT",
					Message:      "Device did not respond in time",
				}
				_ = ws.SendMessage(conn, wsResp)
			}
		}
	})

	// Response will be sent when MQTT response arrives
	return nil, nil
}

// SubscribeToEvents handles a subscribe message on /ws.
func (ws *WebSocketService) SubscribeToEvents(ctx context.Context, clientID string, req *model.ReadSubscribeRequest, connectionID string) (*model.SubscriptionResult, error) {
	if req.ConsumerID == "" || len(req.Contracts) == 0 {
		ws.publishAnalytics(ctx, analyticsEvent(
			"invalid_message",
			clientID,
			connectionID,
			"",
			req.ConsumerID,
			"",
			"error",
			"INVALID_MESSAGE",
			"Invalid read subscription request",
			nil,
			map[string]interface{}{"contracts": req.Contracts},
		))
		return nil, fmt.Errorf("INVALID_MESSAGE")
	}

	ws.publishAnalytics(ctx, analyticsEvent(
		"read_subscribe_requested",
		clientID,
		connectionID,
		"",
		req.ConsumerID,
		"",
		"ok",
		"",
		"",
		nil,
		map[string]interface{}{"contracts": req.Contracts},
	))

	// Check access for each contract
	allowedContracts := []string{}
	deniedContracts := []string{}
	for _, contract := range req.Contracts {
		allowed, err := ws.access.Check(ctx, clientID, req.ConsumerID, contract, AccessRead)
		if err != nil {
			ws.logger.Printf("%s AccessService error: %v\n", observability.TraceFields(ctx), err)
			ws.publishAnalytics(ctx, analyticsEvent(
				"access_error",
				clientID,
				connectionID,
				"",
				req.ConsumerID,
				contract,
				"error",
				"INTERNAL_ERROR",
				err.Error(),
				nil,
				nil,
			))
			deniedContracts = append(deniedContracts, contract)
			continue
		}
		if allowed {
			allowedContracts = append(allowedContracts, contract)
		} else {
			deniedContracts = append(deniedContracts, contract)
		}
	}

	if len(allowedContracts) == 0 {
		ws.publishAnalytics(ctx, analyticsEvent(
			"access_denied",
			clientID,
			connectionID,
			"",
			req.ConsumerID,
			"",
			"denied",
			"ACCESS_DENIED",
			"Access denied to requested contracts",
			nil,
			map[string]interface{}{"contracts": req.Contracts},
		))
		return &model.SubscriptionResult{
			ConsumerID: req.ConsumerID,
			Accepted:   allowedContracts,
			Denied:     deniedContracts,
		}, nil
	}

	// Save connection metadata and subscriptions
	metadata := &model.SubscriptionMetadata{
		ClientID:   clientID,
		ConsumerID: req.ConsumerID,
		Contracts:  allowedContracts,
	}
	if err := ws.subsMgr.SaveConnectionMetadata(ctx, connectionID, metadata); err != nil {
		ws.publishAnalytics(ctx, analyticsEvent(
			"subscription_store_failed",
			clientID,
			connectionID,
			"",
			req.ConsumerID,
			"",
			"error",
			"INTERNAL_ERROR",
			err.Error(),
			nil,
			map[string]interface{}{"contracts": allowedContracts},
		))
		return nil, err
	}

	// Add subscriptions for each allowed contract
	for _, contract := range allowedContracts {
		if err := ws.subsMgr.AddSubscription(ctx, req.ConsumerID, contract, connectionID); err != nil {
			ws.logger.Printf("%s Failed to add subscription: %v\n", observability.TraceFields(ctx), err)
			ws.publishAnalytics(ctx, analyticsEvent(
				"subscription_store_failed",
				clientID,
				connectionID,
				"",
				req.ConsumerID,
				contract,
				"error",
				"INTERNAL_ERROR",
				err.Error(),
				nil,
				nil,
			))
			return nil, fmt.Errorf("INTERNAL_ERROR")
		}
	}

	ws.publishAnalytics(ctx, analyticsEvent(
		"read_subscribed",
		clientID,
		connectionID,
		"",
		req.ConsumerID,
		"",
		"ok",
		"",
		"",
		nil,
		map[string]interface{}{
			"requested_contracts": req.Contracts,
			"allowed_contracts":   allowedContracts,
		},
	))

	return &model.SubscriptionResult{
		ConsumerID: req.ConsumerID,
		Accepted:   allowedContracts,
		Denied:     deniedContracts,
	}, nil
}

// UnsubscribeFromEvents removes matching subscriptions for a connection.
func (ws *WebSocketService) UnsubscribeFromEvents(ctx context.Context, clientID string, req *model.ReadSubscribeRequest, connectionID string) (*model.SubscriptionResult, error) {
	metadata, err := ws.subsMgr.GetConnectionMetadata(ctx, connectionID)
	if err != nil {
		return nil, fmt.Errorf("INVALID_MESSAGE")
	}

	consumerID := req.ConsumerID
	if consumerID == "" {
		consumerID = metadata.ConsumerID
	}
	if consumerID == "" {
		return nil, fmt.Errorf("INVALID_MESSAGE")
	}

	requested := req.Contracts
	if len(requested) == 0 {
		requested = metadata.Contracts
	}

	removed := make([]string, 0, len(requested))
	remaining := make([]string, 0, len(metadata.Contracts))
	removeSet := map[string]struct{}{}
	for _, contract := range requested {
		removeSet[contract] = struct{}{}
	}
	for _, contract := range metadata.Contracts {
		if _, shouldRemove := removeSet[contract]; shouldRemove {
			if err := ws.subsMgr.RemoveSubscription(ctx, consumerID, contract, connectionID); err != nil {
				return nil, fmt.Errorf("INTERNAL_ERROR")
			}
			removed = append(removed, contract)
			continue
		}
		remaining = append(remaining, contract)
	}

	if len(remaining) == 0 {
		_ = ws.subsMgr.RemoveConnectionMetadata(ctx, connectionID)
	} else {
		metadata.Contracts = remaining
		metadata.ConsumerID = consumerID
		if err := ws.subsMgr.SaveConnectionMetadata(ctx, connectionID, metadata); err != nil {
			return nil, fmt.Errorf("INTERNAL_ERROR")
		}
	}

	ws.publishAnalytics(ctx, analyticsEvent(
		"read_unsubscribed",
		clientID,
		connectionID,
		"",
		consumerID,
		"",
		"ok",
		"",
		"",
		nil,
		map[string]interface{}{"contracts": removed},
	))

	return &model.SubscriptionResult{
		ConsumerID: consumerID,
		Accepted:   removed,
	}, nil
}

// ListenMQTTResponses listens for device responses and matches them with requests
func (ws *WebSocketService) ListenMQTTResponses(ctx context.Context) error {
	// We need to subscribe to all possible response topics
	// For a scalable solution, we'd use MQTT wildcards or a pattern subscription
	// For now, we'll use a global subscription mechanism

	handler := func(c mqtt.Client, msg mqtt.Message) {
		ws.handleMQTTResponse(ctx, msg.Payload())
	}

	// Subscribe to wildcard topic for all responses if supported
	// Otherwise, we'll need to subscribe to specific consumers as they connect
	token := ws.mqtt.GetClient().Subscribe("devices/+/responses", byte(QoSAtLeastOnce), handler)
	if token.Wait() && token.Error() != nil {
		return token.Error()
	}

	ws.logger.Println("Subscribed to MQTT response topic: devices/+/responses")
	<-ctx.Done()
	return ctx.Err()
}

// ListenMQTTEvents listens for device events and broadcasts to subscribers
func (ws *WebSocketService) ListenMQTTEvents(ctx context.Context) error {
	handler := func(c mqtt.Client, msg mqtt.Message) {
		ws.handleMQTTEvent(ctx, msg.Payload())
	}

	// Subscribe to wildcard topic for all events
	token := ws.mqtt.GetClient().Subscribe("devices/+/events", byte(QoSAtLeastOnce), handler)
	if token.Wait() && token.Error() != nil {
		return token.Error()
	}

	ws.logger.Println("Subscribed to MQTT event topic: devices/+/events")
	<-ctx.Done()
	return ctx.Err()
}

func (ws *WebSocketService) handleMQTTResponse(ctx context.Context, msg []byte) {
	// Parse MQTT response
	var response model.MQTTResponsePayload
	if err := json.Unmarshal(msg, &response); err != nil {
		ws.logger.Printf("%s Failed to parse MQTT response: %v\n", observability.TraceFields(ctx), err)
		ws.publishAnalytics(ctx, analyticsEvent(
			"mqtt_response_parse_failed",
			"",
			"",
			"",
			"",
			"",
			"error",
			"INVALID_MESSAGE",
			err.Error(),
			json.RawMessage(msg),
			nil,
		))
		return
	}

	// Get request metadata from Redis
	metadata, err := ws.requestMgr.GetRequest(ctx, response.RequestID)
	if err != nil {
		ws.logger.Printf("%s Request not found for ID %s: %v\n", observability.TraceFields(ctx), response.RequestID, err)
		ws.publishAnalytics(ctx, analyticsEvent(
			"mqtt_response_orphan",
			"",
			"",
			response.RequestID,
			response.ConsumerID,
			response.ContractName,
			"error",
			"REQUEST_NOT_FOUND",
			err.Error(),
			response.Payload,
			map[string]interface{}{"device_status": response.Status},
		))
		return
	}

	ws.publishAnalytics(ctx, analyticsEvent(
		"mqtt_response_received",
		metadata.ClientID,
		metadata.ConnectionID,
		response.RequestID,
		response.ConsumerID,
		response.ContractName,
		response.Status,
		"",
		"",
		response.Payload,
		map[string]interface{}{"device_status": response.Status},
	))

	// Get connection
	conn, ok := ws.GetConnection(metadata.ConnectionID)
	if !ok {
		ws.logger.Printf("%s Connection not found for ID %s\n", observability.TraceFields(ctx), metadata.ConnectionID)
		ws.publishAnalytics(ctx, analyticsEvent(
			"connection_not_found",
			metadata.ClientID,
			metadata.ConnectionID,
			response.RequestID,
			response.ConsumerID,
			response.ContractName,
			"error",
			"CONNECTION_NOT_FOUND",
			"Connection not found",
			response.Payload,
			nil,
		))
		return
	}

	// Send response to user
	wsResp := &model.WSResponse{
		Type:         "success",
		RequestID:    response.RequestID,
		ConsumerID:   response.ConsumerID,
		ContractName: response.ContractName,
		Status:       response.Status,
		Payload:      response.Payload,
		Timestamp:    response.Timestamp,
	}

	if err := ws.SendMessage(conn, wsResp); err != nil {
		ws.logger.Printf("%s Failed to send response: %v\n", observability.TraceFields(ctx), err)
		ws.publishAnalytics(ctx, analyticsEvent(
			"websocket_send_failed",
			metadata.ClientID,
			metadata.ConnectionID,
			response.RequestID,
			response.ConsumerID,
			response.ContractName,
			"error",
			"INTERNAL_ERROR",
			err.Error(),
			response.Payload,
			nil,
		))
	}

	// Clean up request from Redis
	ws.requestMgr.DeleteRequest(ctx, response.RequestID)
}

func (ws *WebSocketService) handleMQTTEvent(ctx context.Context, msg []byte) {
	// Parse MQTT event
	var event model.MQTTEventPayload
	if err := json.Unmarshal(msg, &event); err != nil {
		ws.logger.Printf("%s Failed to parse MQTT event: %v\n", observability.TraceFields(ctx), err)
		ws.publishAnalytics(ctx, analyticsEvent(
			"device_event_parse_failed",
			"",
			"",
			"",
			"",
			"",
			"error",
			"INVALID_MESSAGE",
			err.Error(),
			json.RawMessage(msg),
			nil,
		))
		return
	}

	deviceEvent := model.NewAnalyticsEvent("device_event")
	deviceEvent.Source = "device"
	deviceEvent.ConsumerID = event.ConsumerID
	deviceEvent.ContractName = event.ContractName
	deviceEvent.Status = "ok"
	deviceEvent.Payload = event.Payload
	deviceEvent.Metadata = map[string]interface{}{"device_ts": event.Timestamp}
	ws.publishAnalytics(ctx, deviceEvent)

	// Get all subscribers for this contract
	subscribers, err := ws.subsMgr.GetSubscribers(ctx, event.ConsumerID, event.ContractName)
	if err != nil {
		ws.logger.Printf("%s Failed to get subscribers: %v\n", observability.TraceFields(ctx), err)
		ws.publishAnalytics(ctx, analyticsEvent(
			"subscribers_lookup_failed",
			"",
			"",
			"",
			event.ConsumerID,
			event.ContractName,
			"error",
			"INTERNAL_ERROR",
			err.Error(),
			event.Payload,
			nil,
		))
		return
	}

	// Send event to all subscribed connections
	wsEvent := &model.WSResponse{
		Type:         "event",
		ConsumerID:   event.ConsumerID,
		ContractName: event.ContractName,
		Payload:      event.Payload,
		Timestamp:    event.Timestamp,
	}

	for _, connID := range subscribers {
		if conn, ok := ws.GetConnection(connID); ok {
			if err := ws.SendMessage(conn, wsEvent); err != nil {
				ws.logger.Printf("%s Failed to send event to connection %s: %v\n", observability.TraceFields(ctx), connID, err)
				ws.publishAnalytics(ctx, analyticsEvent(
					"websocket_send_failed",
					"",
					connID,
					"",
					event.ConsumerID,
					event.ContractName,
					"error",
					"INTERNAL_ERROR",
					err.Error(),
					event.Payload,
					nil,
				))
			}
		}
	}
}

// PublishAnalyticsEvent publishes an analytics event from HTTP handlers.
func (ws *WebSocketService) PublishAnalyticsEvent(ctx context.Context, event *model.AnalyticsEvent) {
	ws.publishAnalytics(ctx, event)
}

func (ws *WebSocketService) publishAnalytics(ctx context.Context, event *model.AnalyticsEvent) {
	if event == nil {
		return
	}
	if err := ws.redis.PublishAnalyticsEvent(ctx, event); err != nil {
		ws.logger.Printf("%s Failed to publish analytics event %s: %v\n", observability.TraceFields(ctx), event.Action, err)
	}
}

func analyticsEvent(
	action string,
	clientID string,
	connectionID string,
	requestID string,
	consumerID string,
	contractName string,
	status string,
	errorCode string,
	errorMessage string,
	payload json.RawMessage,
	metadata map[string]interface{},
) *model.AnalyticsEvent {
	event := model.NewAnalyticsEvent(action)
	event.ClientID = clientID
	event.ConnectionID = connectionID
	event.RequestID = requestID
	event.ConsumerID = consumerID
	event.ContractName = contractName
	event.Status = status
	event.ErrorCode = errorCode
	event.ErrorMessage = errorMessage
	event.Payload = payload
	event.Metadata = metadata
	return event
}

// GenerateConnectionID generates unique connection ID
func GenerateConnectionID() string {
	return uuid.New().String()
}
