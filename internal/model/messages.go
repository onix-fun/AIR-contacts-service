package model

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// WriteRequest - message from user for command execution
type WriteRequest struct {
	RequestID  string          `json:"request_id"`
	ConsumerID string          `json:"consumer_id"`
	MethodName string          `json:"method_name"`
	Input      json.RawMessage `json:"input,omitempty"`
}

// ReadSubscribeRequest - message from user for event subscription
type ReadSubscribeRequest struct {
	ConsumerID string   `json:"consumer_id"`
	Variables  []string `json:"variables"`
}

// WSClientMessage is the single WebSocket command envelope.
type WSClientMessage struct {
	Type       string          `json:"type"`
	RequestID  string          `json:"request_id,omitempty"`
	ConsumerID string          `json:"consumer_id,omitempty"`
	MethodName string          `json:"method_name,omitempty"`
	Variables  []string        `json:"variables,omitempty"`
	Input      json.RawMessage `json:"input,omitempty"`
}

// MQTTCommandPayload - payload for MQTT command topic
type MQTTCommandPayload struct {
	RequestID  string          `json:"request_id"`
	ConsumerID string          `json:"consumer_id"`
	MethodName string          `json:"method_name"`
	Input      json.RawMessage `json:"input,omitempty"`
	Timestamp  string          `json:"ts"`
}

// MQTTResponsePayload - payload from MQTT response topic
type MQTTResponsePayload struct {
	RequestID  string `json:"request_id"`
	StatusCode int    `json:"status_code"`
}

// MQTTEventPayload - payload from MQTT event topic
type MQTTEventPayload struct {
	ConsumerID   string          `json:"consumer_id"`
	VariableName string          `json:"variable_name"`
	Payload      json.RawMessage `json:"payload"`
	Timestamp    string          `json:"ts"`
}

// WSResponse - response to user via WebSocket
type WSResponse struct {
	Type         string          `json:"type"` // "success", "event", "error"
	RequestID    string          `json:"request_id,omitempty"`
	ConsumerID   string          `json:"consumer_id,omitempty"`
	VariableName string          `json:"variable_name,omitempty"`
	Status       string          `json:"status,omitempty"`
	StatusCode   int             `json:"status_code,omitempty"`
	Code         string          `json:"code,omitempty"`
	Message      string          `json:"message,omitempty"`
	Payload      json.RawMessage `json:"payload,omitempty"`
	Timestamp    string          `json:"ts,omitempty"`
	Accepted     []string        `json:"accepted,omitempty"`
	Denied       []string        `json:"denied,omitempty"`
}

type SubscriptionResult struct {
	ConsumerID string
	Accepted   []string
	Denied     []string
}

// ErrorResponse - error response
type ErrorResponse struct {
	Type      string `json:"type"`
	RequestID string `json:"request_id,omitempty"`
	Code      string `json:"code"`
	Message   string `json:"message"`
}

// RequestMetadata - metadata stored in Redis
type RequestMetadata struct {
	ClientID     string
	ConsumerID   string
	MethodName   string
	ConnectionID string
}

// SubscriptionMetadata - subscription metadata stored in Redis
type SubscriptionMetadata struct {
	ClientID   string
	ConsumerID string
	Variables  []string
}

// AnalyticsEvent is written to Redis Streams and consumed by the analytics service.
type AnalyticsEvent struct {
	EventID      string                 `json:"event_id"`
	Source       string                 `json:"source"`
	Action       string                 `json:"action"`
	OccurredAt   string                 `json:"occurred_at"`
	ClientID     string                 `json:"client_id,omitempty"`
	ConnectionID string                 `json:"connection_id,omitempty"`
	RequestID    string                 `json:"request_id,omitempty"`
	ConsumerID   string                 `json:"consumer_id,omitempty"`
	MethodName   string                 `json:"method_name,omitempty"`
	VariableName string                 `json:"variable_name,omitempty"`
	Status       string                 `json:"status,omitempty"`
	StatusCode   *int                   `json:"status_code,omitempty"`
	ErrorCode    string                 `json:"error_code,omitempty"`
	ErrorMessage string                 `json:"error_message,omitempty"`
	Payload      json.RawMessage        `json:"payload,omitempty"`
	Metadata     map[string]interface{} `json:"metadata,omitempty"`
}

func NewAnalyticsEvent(action string) *AnalyticsEvent {
	return &AnalyticsEvent{
		EventID:    uuid.NewString(),
		Source:     "contacts",
		Action:     action,
		OccurredAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
}
