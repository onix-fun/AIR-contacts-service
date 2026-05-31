package handler

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestAccessExpiry(t *testing.T) {
	request, err := http.NewRequest(http.MethodGet, "http://localhost/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("X-Access-Expires-At", strconv.FormatInt(time.Now().Add(time.Minute).Unix(), 10))
	if _, err := accessExpiry(request); err != nil {
		t.Fatalf("expected future expiry to pass: %v", err)
	}

	request.Header.Set("X-Access-Expires-At", strconv.FormatInt(time.Now().Add(-time.Minute).Unix(), 10))
	if _, err := accessExpiry(request); err == nil {
		t.Fatal("expected expired token to fail")
	}

	request.Header.Del("X-Access-Expires-At")
	if _, err := accessExpiry(request); err == nil {
		t.Fatal("expected missing expiry to fail")
	}
}

func TestCloseConnectionAtExpiry(t *testing.T) {
	acceptTestOrigin := func(*http.Request) bool { return true }
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{CheckOrigin: acceptTestOrigin}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		timer := closeConnectionAtExpiry(conn, time.Now().Add(50*time.Millisecond))
		defer timer.Stop()
		time.Sleep(150 * time.Millisecond)
	}))
	defer server.Close()

	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if _, _, err := conn.ReadMessage(); !websocket.IsCloseError(err, websocket.ClosePolicyViolation) {
		t.Fatalf("expected policy violation close, got %v", err)
	}
}
