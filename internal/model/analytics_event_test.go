package model

import (
	"encoding/json"
	"testing"
	"time"
)

func TestNewAnalyticsEvent(t *testing.T) {
	event := NewAnalyticsEvent("device_event_received")

	if event.EventID == "" {
		t.Fatal("expected event id")
	}
	if event.Source != "contacts" {
		t.Fatalf("expected source contacts, got %s", event.Source)
	}
	if event.Action != "device_event_received" {
		t.Fatalf("unexpected action %s", event.Action)
	}
	if _, err := time.Parse(time.RFC3339Nano, event.OccurredAt); err != nil {
		t.Fatalf("occurred_at is not RFC3339Nano: %v", err)
	}
}

func TestAnalyticsEventJSONShape(t *testing.T) {
	event := NewAnalyticsEvent("write_request_received")
	event.ClientID = "00000000-0000-0000-0000-000000000001"
	event.ConsumerID = "00000000-0000-0000-0000-000000000002"
	event.MethodName = "relay.set"
	event.Payload = json.RawMessage(`{"state":true}`)
	event.Metadata = map[string]interface{}{"mode": "write"}

	data, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal analytics event: %v", err)
	}

	var decoded map[string]interface{}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal analytics event: %v", err)
	}

	if decoded["event_id"] == "" {
		t.Fatal("expected event_id field")
	}
	if decoded["client_id"] != event.ClientID {
		t.Fatalf("expected client_id %s, got %v", event.ClientID, decoded["client_id"])
	}
	payload := decoded["payload"].(map[string]interface{})
	if payload["state"] != true {
		t.Fatalf("expected payload.state=true, got %v", payload["state"])
	}
}
