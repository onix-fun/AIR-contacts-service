package service

import (
	"encoding/json"
	"testing"
)

func TestAnalyticsEventHelper(t *testing.T) {
	event := analyticsEvent(
		"mqtt_command_published",
		"client-1",
		"connection-1",
		"request-1",
		"consumer-1",
		"relay.set",
		"ok",
		"",
		"",
		json.RawMessage(`{"state":true}`),
		map[string]interface{}{"mode": "write"},
	)

	if event.EventID == "" {
		t.Fatal("expected event id")
	}
	if event.Source != "contacts" {
		t.Fatalf("expected source contacts, got %s", event.Source)
	}
	if event.Action != "mqtt_command_published" {
		t.Fatalf("unexpected action %s", event.Action)
	}
	if event.ClientID != "client-1" || event.ConnectionID != "connection-1" || event.RequestID != "request-1" {
		t.Fatalf("unexpected identifiers: %+v", event)
	}
	if string(event.Payload) != `{"state":true}` {
		t.Fatalf("unexpected payload %s", string(event.Payload))
	}
	if event.Metadata["mode"] != "write" {
		t.Fatalf("unexpected metadata %+v", event.Metadata)
	}
}
