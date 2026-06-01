package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/onix-air/contacts/internal/client"
	"github.com/onix-air/contacts/internal/errorcatalog"
	"github.com/onix-air/contacts/internal/model"
	"github.com/onix-air/contacts/internal/observability"
	pb "github.com/onix-air/contacts/internal/proto"
)

var upgrader = websocket.Upgrader{
	CheckOrigin:     isAllowedWebSocketOrigin,
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

const (
	QoSAtLeastOnce = 1
	maxMessageSize = 64 * 1024
)

type WebSocketService struct {
	redis          *client.RedisClient
	mqtt           *client.MQTTClient
	access         *client.AccessServiceClient
	requestMgr     *RequestManager
	subsMgr        *SubscriptionManager
	logger         *log.Logger
	connections    sync.Map // map[string]*websocket.Conn
	connectionInfo sync.Map // map[string]connectionInfo
}

type connectionInfo struct {
	ClientID string
	Mode     string
}

func NewWebSocketService(
	redis *client.RedisClient,
	mqtt *client.MQTTClient,
	access *client.AccessServiceClient,
	requestMgr *RequestManager,
	subsMgr *SubscriptionManager,
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
	conn.SetReadLimit(maxMessageSize)
	return conn, nil
}

func isAllowedWebSocketOrigin(r *http.Request) bool {
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	if origin == "" {
		return false
	}

	parsed, err := url.Parse(origin)
	if err != nil ||
		parsed.Scheme == "" ||
		parsed.Host == "" ||
		parsed.User != nil ||
		parsed.Path != "" ||
		parsed.RawQuery != "" ||
		parsed.Fragment != "" ||
		parsed.ForceQuery ||
		strings.HasSuffix(parsed.Host, ":") {
		return false
	}
	scheme := strings.ToLower(parsed.Scheme)
	host := strings.ToLower(parsed.Hostname())
	if host == "localhost" || host == "127.0.0.1" {
		return scheme == "http" || scheme == "https"
	}

	baseDomain := strings.ToLower(strings.TrimSpace(os.Getenv("SPARROW_TRUSTED_BASE_DOMAIN")))
	if baseDomain == "" || scheme != "https" {
		return false
	}
	return host == baseDomain || strings.HasSuffix(host, "."+baseDomain)
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
	if req.RequestID == "" || req.ConsumerID == "" || req.MethodName == "" {
		ws.publishAnalytics(ctx, analyticsEvent(
			"invalid_message",
			clientID,
			connectionID,
			req.RequestID,
			req.ConsumerID,
			req.MethodName,
			"error",
			"INVALID_MESSAGE",
			"Invalid write request",
			req.Input,
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
		req.MethodName,
		"ok",
		"",
		"",
		req.Input,
		nil,
	))

	// Check access
	allowed, err := ws.access.Check(ctx, clientID, req.ConsumerID, req.MethodName, pb.ResourceType_METHOD)
	if err != nil {
		ws.logger.Printf("%s AccessService error: %v\n", observability.TraceFields(ctx), err)
		ws.publishAnalytics(ctx, analyticsEvent(
			"access_error",
			clientID,
			connectionID,
			req.RequestID,
			req.ConsumerID,
			req.MethodName,
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
			req.MethodName,
			"denied",
			"ACCESS_DENIED",
			"Access denied",
			req.Input,
			nil,
		))
		return nil, fmt.Errorf("ACCESS_DENIED")
	}

	// Store request in Redis with TTL
	metadata := &model.RequestMetadata{
		ClientID:     clientID,
		ConsumerID:   req.ConsumerID,
		MethodName:   req.MethodName,
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
			req.MethodName,
			"error",
			"INTERNAL_ERROR",
			err.Error(),
			req.Input,
			nil,
		))
		return nil, fmt.Errorf("INTERNAL_ERROR")
	}

	// Publish command to MQTT
	cmdPayload := &model.MQTTCommandPayload{
		RequestID:  req.RequestID,
		ConsumerID: req.ConsumerID,
		MethodName: req.MethodName,
		Input:      req.Input,
		Timestamp:  time.Now().UTC().Format(time.RFC3339),
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
			req.MethodName,
			"error",
			"MQTT_PUBLISH_FAILED",
			err.Error(),
			req.Input,
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
		req.MethodName,
		"ok",
		"",
		"",
		req.Input,
		nil,
	))

	// Schedule timeout check
	time.AfterFunc(10*time.Second, func() {
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
				req.MethodName,
				"error",
				"TIMEOUT",
				"Device did not respond in time",
				req.Input,
				nil,
			))

			if conn, ok := ws.GetConnection(connectionID); ok {
				entry := errorcatalog.ByStatus(408)
				wsResp := &model.WSResponse{Type: "error", RequestID: req.RequestID, StatusCode: entry.StatusCode, Code: entry.Code, Message: entry.Message}
				_ = ws.SendMessage(conn, wsResp)
			}
		}
	})

	// Response will be sent when MQTT response arrives
	return nil, nil
}

// SubscribeToEvents handles a subscribe message on /ws.
func (ws *WebSocketService) SubscribeToEvents(ctx context.Context, clientID string, req *model.ReadSubscribeRequest, connectionID string) (*model.SubscriptionResult, error) {
	if req.ConsumerID == "" || len(req.Variables) == 0 {
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
			map[string]interface{}{"variables": req.Variables},
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
		map[string]interface{}{"variables": req.Variables},
	))

	// Check access for each variable
	allowedVariables := []string{}
	deniedVariables := []string{}
	for _, variable := range req.Variables {
		allowed, err := ws.access.Check(ctx, clientID, req.ConsumerID, variable, pb.ResourceType_VARIABLE)
		if err != nil {
			ws.logger.Printf("%s AccessService error: %v\n", observability.TraceFields(ctx), err)
			ws.publishAnalytics(ctx, analyticsVariableEvent(
				"access_error",
				clientID,
				connectionID,
				"",
				req.ConsumerID,
				variable,
				"error",
				"INTERNAL_ERROR",
				err.Error(),
				nil,
				nil,
			))
			deniedVariables = append(deniedVariables, variable)
			continue
		}
		if allowed {
			allowedVariables = append(allowedVariables, variable)
		} else {
			deniedVariables = append(deniedVariables, variable)
		}
	}

	if len(allowedVariables) == 0 {
		ws.publishAnalytics(ctx, analyticsEvent(
			"access_denied",
			clientID,
			connectionID,
			"",
			req.ConsumerID,
			"",
			"denied",
			"ACCESS_DENIED",
			"Access denied to requested variables",
			nil,
			map[string]interface{}{"variables": req.Variables},
		))
		return &model.SubscriptionResult{
			ConsumerID: req.ConsumerID,
			Accepted:   allowedVariables,
			Denied:     deniedVariables,
		}, nil
	}

	// Save connection metadata and subscriptions
	metadata := &model.SubscriptionMetadata{
		ClientID:   clientID,
		ConsumerID: req.ConsumerID,
		Variables:  allowedVariables,
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
			map[string]interface{}{"variables": allowedVariables},
		))
		return nil, err
	}

	// Add subscriptions for each allowed variable
	for _, variable := range allowedVariables {
		if err := ws.subsMgr.AddSubscription(ctx, req.ConsumerID, variable, connectionID); err != nil {
			ws.logger.Printf("%s Failed to add subscription: %v\n", observability.TraceFields(ctx), err)
			ws.publishAnalytics(ctx, analyticsVariableEvent(
				"subscription_store_failed",
				clientID,
				connectionID,
				"",
				req.ConsumerID,
				variable,
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
			"requested_variables": req.Variables,
			"allowed_variables":   allowedVariables,
		},
	))

	return &model.SubscriptionResult{
		ConsumerID: req.ConsumerID,
		Accepted:   allowedVariables,
		Denied:     deniedVariables,
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

	requested := req.Variables
	if len(requested) == 0 {
		requested = metadata.Variables
	}

	removed := make([]string, 0, len(requested))
	remaining := make([]string, 0, len(metadata.Variables))
	removeSet := map[string]struct{}{}
	for _, variable := range requested {
		removeSet[variable] = struct{}{}
	}
	for _, variable := range metadata.Variables {
		if _, shouldRemove := removeSet[variable]; shouldRemove {
			if err := ws.subsMgr.RemoveSubscription(ctx, consumerID, variable, connectionID); err != nil {
				return nil, fmt.Errorf("INTERNAL_ERROR")
			}
			removed = append(removed, variable)
			continue
		}
		remaining = append(remaining, variable)
	}

	if len(remaining) == 0 {
		_ = ws.subsMgr.RemoveConnectionMetadata(ctx, connectionID)
	} else {
		metadata.Variables = remaining
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
		map[string]interface{}{"variables": removed},
	))

	return &model.SubscriptionResult{
		ConsumerID: consumerID,
		Accepted:   removed,
	}, nil
}

// ListenMQTTResponses listens for device responses and matches them with requests
func (ws *WebSocketService) ListenMQTTResponses(ctx context.Context) error {
	handler := func(c mqtt.Client, msg mqtt.Message) {
		ws.handleMQTTResponse(ctx, msg.Payload())
	}

	if err := ws.mqtt.SubscribePersistent("devices/+/responses", handler); err != nil {
		return err
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

	if err := ws.mqtt.SubscribePersistent("devices/+/events", handler); err != nil {
		return err
	}

	ws.logger.Println("Subscribed to MQTT event topic: devices/+/events")
	<-ctx.Done()
	return ctx.Err()
}

func (ws *WebSocketService) handleMQTTResponse(ctx context.Context, msg []byte) {
	var response model.MQTTResponsePayload
	if err := json.Unmarshal(msg, &response); err != nil || response.RequestID == "" || response.StatusCode == 0 {
		detail := "missing request_id or status_code"
		if err != nil {
			detail = err.Error()
		}
		ws.logger.Printf("%s Failed to parse MQTT response: %s\n", observability.TraceFields(ctx), detail)
		ws.publishAnalytics(ctx, analyticsEvent(
			"mqtt_response_parse_failed",
			"",
			"",
			"",
			"",
			"",
			"error",
			"INVALID_MESSAGE",
			detail,
			json.RawMessage(msg),
			nil,
		))
		return
	}

	metadata, err := ws.requestMgr.GetRequest(ctx, response.RequestID)
	if err != nil {
		ws.logger.Printf("%s Request not found for ID %s: %v\n", observability.TraceFields(ctx), response.RequestID, err)
		ws.publishAnalytics(ctx, analyticsEvent(
			"mqtt_response_orphan",
			"",
			"",
			response.RequestID,
			"",
			"",
			"error",
			"REQUEST_NOT_FOUND",
			err.Error(),
			nil,
			map[string]interface{}{"device_status_code": response.StatusCode},
		))
		return
	}
	defer ws.requestMgr.DeleteRequest(ctx, response.RequestID)

	statusCode := response.StatusCode
	entry := errorcatalog.ByStatus(statusCode)

	responseEvent := analyticsEvent(
		"mqtt_response_received",
		metadata.ClientID,
		metadata.ConnectionID,
		response.RequestID,
		metadata.ConsumerID,
		metadata.MethodName,
		"",
		entry.Code,
		entry.Message,
		nil,
		nil,
	)
	responseEvent.StatusCode = &statusCode
	ws.publishAnalytics(ctx, responseEvent)

	conn, ok := ws.GetConnection(metadata.ConnectionID)
	if !ok {
		ws.logger.Printf("%s Connection not found for ID %s\n", observability.TraceFields(ctx), metadata.ConnectionID)
		ws.publishAnalytics(ctx, analyticsVariableEvent(
			"connection_not_found",
			metadata.ClientID,
			metadata.ConnectionID,
			response.RequestID,
			metadata.ConsumerID,
			metadata.MethodName,
			"error",
			"CONNECTION_NOT_FOUND",
			"Connection not found",
			nil,
			nil,
		))
		return
	}

	wsResp := &model.WSResponse{
		Type:       "success",
		RequestID:  response.RequestID,
		StatusCode: statusCode,
	}
	if statusCode != 200 {
		wsResp.Type = "error"
		wsResp.Code = entry.Code
		wsResp.Message = entry.Message
	}

	if err := ws.SendMessage(conn, wsResp); err != nil {
		ws.logger.Printf("%s Failed to send response: %v\n", observability.TraceFields(ctx), err)
		ws.publishAnalytics(ctx, analyticsEvent(
			"websocket_send_failed",
			metadata.ClientID,
			metadata.ConnectionID,
			response.RequestID,
			metadata.ConsumerID,
			metadata.MethodName,
			"error",
			"INTERNAL_ERROR",
			err.Error(),
			nil,
			nil,
		))
	}
}

func (ws *WebSocketService) handleMQTTEvent(ctx context.Context, msg []byte) {
	// Parse MQTT event
	var event model.MQTTEventPayload
	if err := json.Unmarshal(msg, &event); err != nil || event.ConsumerID == "" || event.VariableName == "" {
		detail := "missing consumer_id or variable_name"
		if err != nil {
			detail = err.Error()
		}
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
			detail,
			json.RawMessage(msg),
			nil,
		))
		return
	}

	deviceEvent := model.NewAnalyticsEvent("device_event")
	deviceEvent.Source = "device"
	deviceEvent.ConsumerID = event.ConsumerID
	deviceEvent.VariableName = event.VariableName
	deviceEvent.Status = "ok"
	deviceEvent.Payload = event.Payload
	deviceEvent.Metadata = map[string]interface{}{"device_ts": event.Timestamp}
	ws.publishAnalytics(ctx, deviceEvent)

	// Get all subscribers for this variable
	subscribers, err := ws.subsMgr.GetSubscribers(ctx, event.ConsumerID, event.VariableName)
	if err != nil {
		ws.logger.Printf("%s Failed to get subscribers: %v\n", observability.TraceFields(ctx), err)
		ws.publishAnalytics(ctx, analyticsVariableEvent(
			"subscribers_lookup_failed",
			"",
			"",
			"",
			event.ConsumerID,
			event.VariableName,
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
		VariableName: event.VariableName,
		Payload:      event.Payload,
		Timestamp:    event.Timestamp,
	}

	for _, connID := range subscribers {
		if conn, ok := ws.GetConnection(connID); ok {
			if err := ws.SendMessage(conn, wsEvent); err != nil {
				ws.logger.Printf("%s Failed to send event to connection %s: %v\n", observability.TraceFields(ctx), connID, err)
				ws.publishAnalytics(ctx, analyticsVariableEvent(
					"websocket_send_failed",
					"",
					connID,
					"",
					event.ConsumerID,
					event.VariableName,
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
	methodName string,
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
	event.MethodName = methodName
	event.Status = status
	event.ErrorCode = errorCode
	event.ErrorMessage = errorMessage
	event.Payload = payload
	event.Metadata = metadata
	return event
}

func analyticsVariableEvent(
	action string,
	clientID string,
	connectionID string,
	requestID string,
	consumerID string,
	variableName string,
	status string,
	errorCode string,
	errorMessage string,
	payload json.RawMessage,
	metadata map[string]interface{},
) *model.AnalyticsEvent {
	event := analyticsEvent(action, clientID, connectionID, requestID, consumerID, "", status, errorCode, errorMessage, payload, metadata)
	event.VariableName = variableName
	return event
}

// GenerateConnectionID generates unique connection ID
func GenerateConnectionID() string {
	return uuid.New().String()
}
