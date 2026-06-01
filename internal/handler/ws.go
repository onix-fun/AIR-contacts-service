package handler

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/onix-air/contacts/internal/errorcatalog"
	"github.com/onix-air/contacts/internal/model"
	"github.com/onix-air/contacts/internal/observability"
	"github.com/onix-air/contacts/internal/service"
)

type SocketHandler struct {
	service *service.WebSocketService
	logger  *log.Logger
}

func NewSocketHandler(service *service.WebSocketService, logger *log.Logger) *SocketHandler {
	return &SocketHandler{service: service, logger: logger}
}

func (h *SocketHandler) Handle(w http.ResponseWriter, r *http.Request) {
	clientID := r.Header.Get("X-Client-ID")
	if clientID == "" {
		h.reject(r.Context(), w, "", "missing_client_id", "UNAUTHORIZED", "Missing X-Client-ID header")
		return
	}
	if _, err := uuid.Parse(clientID); err != nil {
		h.reject(r.Context(), w, clientID, "invalid_client_id", "BAD_REQUEST", "Invalid X-Client-ID format")
		return
	}
	expiresAt, err := accessExpiry(r)
	if err != nil {
		h.reject(r.Context(), w, clientID, "invalid_access_expiry", "UNAUTHORIZED", err.Error())
		return
	}

	conn, err := h.service.UpgradeConnection(w, r)
	if err != nil {
		h.logger.Printf("%s WebSocket upgrade error: %v\n", observability.TraceFields(r.Context()), err)
		return
	}
	defer conn.Close()
	expiryTimer := closeConnectionAtExpiry(conn, expiresAt)
	defer expiryTimer.Stop()

	connectionID := service.GenerateConnectionID()
	h.service.StoreConnection(connectionID, conn)
	h.service.StoreConnectionInfo(connectionID, clientID, "unified")
	defer h.service.RemoveConnection(context.Background(), connectionID)

	h.service.PublishAnalyticsEvent(r.Context(), &model.AnalyticsEvent{
		EventID:      uuid.NewString(),
		Source:       "contacts",
		Action:       "connection_opened",
		OccurredAt:   time.Now().UTC().Format(time.RFC3339Nano),
		ClientID:     clientID,
		ConnectionID: connectionID,
		Status:       "ok",
		Metadata: map[string]interface{}{
			"mode": "unified",
			"path": "/ws",
		},
	})

	for {
		var message model.WSClientMessage
		if err := conn.ReadJSON(&message); err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				h.logger.Printf("%s WebSocket error: %v\n", observability.TraceFields(r.Context()), err)
			}
			return
		}

		switch message.Type {
		case "command":
			h.handleCommand(r.Context(), clientID, connectionID, message)
		case "subscribe":
			h.handleSubscribe(r.Context(), clientID, connectionID, message)
		case "unsubscribe":
			h.handleUnsubscribe(r.Context(), clientID, connectionID, message)
		case "ping":
			_ = h.service.SendMessage(conn, &model.WSResponse{Type: "pong", Timestamp: time.Now().UTC().Format(time.RFC3339)})
		default:
			h.sendError(connectionID, message.RequestID, "INVALID_MESSAGE")
		}
	}
}

func closeConnectionAtExpiry(conn *websocket.Conn, expiresAt time.Time) *time.Timer {
	return time.AfterFunc(time.Until(expiresAt), func() {
		deadline := time.Now().Add(time.Second)
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "Session expired"), deadline)
		_ = conn.Close()
	})
}

func accessExpiry(r *http.Request) (time.Time, error) {
	raw := r.Header.Get("X-Access-Expires-At")
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("Missing or invalid access token expiry")
	}
	expiresAt := time.Unix(value, 0)
	if !expiresAt.After(time.Now()) {
		return time.Time{}, fmt.Errorf("Access token expired")
	}
	return expiresAt, nil
}

func (h *SocketHandler) reject(ctx context.Context, w http.ResponseWriter, clientID, action, code, message string) {
	h.service.PublishAnalyticsEvent(ctx, &model.AnalyticsEvent{
		EventID:      uuid.NewString(),
		Source:       "contacts",
		Action:       action,
		OccurredAt:   time.Now().UTC().Format(time.RFC3339Nano),
		ClientID:     clientID,
		Status:       "error",
		ErrorCode:    code,
		ErrorMessage: message,
		Metadata: map[string]interface{}{
			"path": "/ws",
		},
	})
	http.Error(w, message, http.StatusUnauthorized)
}

func (h *SocketHandler) handleCommand(ctx context.Context, clientID, connectionID string, message model.WSClientMessage) {
	if message.RequestID == "" {
		message.RequestID = uuid.NewString()
	}
	req := &model.WriteRequest{
		RequestID:  message.RequestID,
		ConsumerID: message.ConsumerID,
		MethodName: message.MethodName,
		Input:      message.Input,
	}

	reqCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if _, err := h.service.ExecuteWrite(reqCtx, clientID, req, connectionID); err != nil {
		h.sendError(connectionID, req.RequestID, err.Error())
	}
}

func (h *SocketHandler) handleSubscribe(ctx context.Context, clientID, connectionID string, message model.WSClientMessage) {
	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	result, err := h.service.SubscribeToEvents(reqCtx, clientID, &model.ReadSubscribeRequest{
		ConsumerID: message.ConsumerID,
		Variables:  message.Variables,
	}, connectionID)
	if err != nil {
		h.sendError(connectionID, message.RequestID, err.Error())
		return
	}
	if conn, ok := h.service.GetConnection(connectionID); ok {
		_ = h.service.SendMessage(conn, &model.WSResponse{
			Type:       "subscription",
			ConsumerID: result.ConsumerID,
			Accepted:   result.Accepted,
			Denied:     result.Denied,
			Status:     "subscribed",
		})
	}
}

func (h *SocketHandler) handleUnsubscribe(ctx context.Context, clientID, connectionID string, message model.WSClientMessage) {
	result, err := h.service.UnsubscribeFromEvents(ctx, clientID, &model.ReadSubscribeRequest{
		ConsumerID: message.ConsumerID,
		Variables:  message.Variables,
	}, connectionID)
	if err != nil {
		h.sendError(connectionID, message.RequestID, err.Error())
		return
	}
	if conn, ok := h.service.GetConnection(connectionID); ok {
		_ = h.service.SendMessage(conn, &model.WSResponse{
			Type:       "subscription",
			ConsumerID: result.ConsumerID,
			Accepted:   result.Accepted,
			Status:     "unsubscribed",
		})
	}
}

func (h *SocketHandler) sendError(connectionID, requestID, code string) {
	entry := errorcatalog.ByCode(code)
	if conn, ok := h.service.GetConnection(connectionID); ok {
		_ = h.service.SendMessage(conn, &model.WSResponse{
			Type:       "error",
			RequestID:  requestID,
			StatusCode: entry.StatusCode,
			Code:       entry.Code,
			Message:    entry.Message,
		})
	}
}
