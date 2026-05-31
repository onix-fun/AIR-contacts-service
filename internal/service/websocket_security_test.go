package service

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestIsAllowedWebSocketOrigin(t *testing.T) {
	original := os.Getenv("SPARROW_TRUSTED_BASE_DOMAIN")
	t.Cleanup(func() { _ = os.Setenv("SPARROW_TRUSTED_BASE_DOMAIN", original) })
	if err := os.Setenv("SPARROW_TRUSTED_BASE_DOMAIN", "example.com"); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		origin  string
		allowed bool
	}{
		{origin: "https://example.com", allowed: true},
		{origin: "https://app.example.com", allowed: true},
		{origin: "https://example.com.evil.test", allowed: false},
		{origin: "http://app.example.com", allowed: false},
		{origin: "https://app.example.com/path", allowed: false},
		{origin: "https://app.example.com:", allowed: false},
		{origin: "https://app.example.com:invalid", allowed: false},
		{origin: "https://user@app.example.com", allowed: false},
		{origin: "http://localhost:5173", allowed: true},
		{origin: "http://127.0.0.1:5174", allowed: true},
		{origin: "", allowed: false},
	}

	for _, tt := range tests {
		request, err := http.NewRequest(http.MethodGet, "http://localhost/ws", nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Origin", tt.origin)
		if actual := isAllowedWebSocketOrigin(request); actual != tt.allowed {
			t.Fatalf("origin %q allowed=%v, want %v", tt.origin, actual, tt.allowed)
		}
	}
}

func TestUpgradeConnectionRejectsOversizedMessage(t *testing.T) {
	result := make(chan error, 1)
	service := &WebSocketService{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := service.UpgradeConnection(w, r)
		if err != nil {
			result <- err
			return
		}
		defer conn.Close()
		_, _, err = conn.ReadMessage()
		result <- err
	}))
	defer server.Close()

	headers := http.Header{}
	headers.Set("Origin", "http://localhost")
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), headers)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := conn.WriteMessage(websocket.TextMessage, []byte(strings.Repeat("x", maxMessageSize+1))); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-result:
		if err == nil || !strings.Contains(err.Error(), "read limit") {
			t.Fatalf("expected read limit error, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for oversized message rejection")
	}
}
