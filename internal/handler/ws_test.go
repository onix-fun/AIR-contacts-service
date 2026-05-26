package handler

import (
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/gorilla/websocket"
	"github.com/onix-air/contacts/internal/model"
	"github.com/onix-air/contacts/internal/service"
)

const handlerClientID = "00000000-0000-0000-0000-000000000001"

type handlerAnalytics struct {
	events []*model.AnalyticsEvent
	closed chan struct{}
	once   sync.Once
}

func (a *handlerAnalytics) PublishAnalyticsEvent(_ context.Context, event *model.AnalyticsEvent) error {
	a.events = append(a.events, event)
	if event.Action == "connection_closed" {
		a.once.Do(func() { close(a.closed) })
	}
	return nil
}

type handlerMQTT struct{}

func (*handlerMQTT) PublishCommand(interface{}, string) error { return nil }
func (*handlerMQTT) GetClient() mqtt.Client                   { return nil }

type handlerAccess struct{}

func (*handlerAccess) Check(context.Context, string, string, string, string) (bool, error) {
	return true, nil
}

type handlerRequests struct{}

func (*handlerRequests) StoreRequest(context.Context, string, *model.RequestMetadata) error {
	return nil
}
func (*handlerRequests) GetRequest(context.Context, string) (*model.RequestMetadata, error) {
	return nil, errors.New("missing")
}
func (*handlerRequests) DeleteRequest(context.Context, string) error { return nil }

type handlerSubscriptions struct {
	metadata *model.SubscriptionMetadata
	getErr   error
}

func (*handlerSubscriptions) AddSubscription(context.Context, string, string, string) error {
	return nil
}
func (*handlerSubscriptions) RemoveSubscription(context.Context, string, string, string) error {
	return nil
}
func (*handlerSubscriptions) GetSubscribers(context.Context, string, string) ([]string, error) {
	return nil, nil
}
func (s *handlerSubscriptions) SaveConnectionMetadata(_ context.Context, _ string, metadata *model.SubscriptionMetadata) error {
	s.metadata = metadata
	return nil
}
func (s *handlerSubscriptions) GetConnectionMetadata(context.Context, string) (*model.SubscriptionMetadata, error) {
	if s.getErr != nil {
		return nil, s.getErr
	}
	return s.metadata, nil
}
func (*handlerSubscriptions) RemoveConnectionMetadata(context.Context, string) error { return nil }
func (*handlerSubscriptions) RemoveAllConnectionSubscriptions(context.Context, string) error {
	return nil
}

func newTestHandler() (*SocketHandler, *handlerAnalytics, *handlerSubscriptions) {
	analytics := &handlerAnalytics{closed: make(chan struct{})}
	subscriptions := &handlerSubscriptions{}
	ws := service.NewWebSocketService(
		analytics,
		&handlerMQTT{},
		&handlerAccess{},
		&handlerRequests{},
		subscriptions,
		log.New(io.Discard, "", 0),
	)
	return NewSocketHandler(ws, log.New(io.Discard, "", 0)), analytics, subscriptions
}

func dialHandler(t *testing.T, handler *SocketHandler) (*websocket.Conn, func()) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(handler.Handle))
	header := http.Header{"X-Client-ID": []string{handlerClientID}}
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), header)
	if err != nil {
		server.Close()
		t.Fatalf("dial handler: %v", err)
	}
	return conn, func() {
		_ = conn.Close()
		server.Close()
	}
}

func readResponse(t *testing.T, conn *websocket.Conn) model.WSResponse {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	var response model.WSResponse
	if err := conn.ReadJSON(&response); err != nil {
		t.Fatalf("read websocket response: %v", err)
	}
	return response
}

func TestHandleRejectsInvalidRequests(t *testing.T) {
	handler, analytics, _ := newTestHandler()
	recorder := httptest.NewRecorder()
	handler.Handle(recorder, httptest.NewRequest(http.MethodGet, "/ws", nil))
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("missing client status %d", recorder.Code)
	}

	recorder = httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/ws", nil)
	request.Header.Set("X-Client-ID", "invalid")
	handler.Handle(recorder, request)
	if recorder.Code != http.StatusUnauthorized || len(analytics.events) != 2 {
		t.Fatalf("invalid client status=%d events=%d", recorder.Code, len(analytics.events))
	}

	recorder = httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodGet, "/ws", nil)
	request.Header.Set("X-Client-ID", handlerClientID)
	handler.Handle(recorder, request)
	if recorder.Code == http.StatusSwitchingProtocols {
		t.Fatal("expected failed HTTP upgrade")
	}
}

func TestHandleWebSocketMessages(t *testing.T) {
	handler, analytics, subscriptions := newTestHandler()
	conn, cleanup := dialHandler(t, handler)
	defer cleanup()

	if err := conn.WriteJSON(model.WSClientMessage{Type: "ping"}); err != nil {
		t.Fatalf("write ping: %v", err)
	}
	if response := readResponse(t, conn); response.Type != "pong" {
		t.Fatalf("unexpected ping response: %+v", response)
	}
	if err := conn.WriteJSON(model.WSClientMessage{Type: "unknown", RequestID: "unknown"}); err != nil {
		t.Fatalf("write unknown: %v", err)
	}
	if response := readResponse(t, conn); response.Code != "INVALID_MESSAGE" {
		t.Fatalf("unexpected invalid response: %+v", response)
	}
	if err := conn.WriteJSON(model.WSClientMessage{Type: "command"}); err != nil {
		t.Fatalf("write command: %v", err)
	}
	if response := readResponse(t, conn); response.Code != "INVALID_MESSAGE" || response.RequestID == "" {
		t.Fatalf("unexpected command response: %+v", response)
	}
	if err := conn.WriteJSON(model.WSClientMessage{Type: "subscribe", RequestID: "sub"}); err != nil {
		t.Fatalf("write invalid subscribe: %v", err)
	}
	if response := readResponse(t, conn); response.Code != "INVALID_MESSAGE" {
		t.Fatalf("unexpected subscribe error: %+v", response)
	}

	subscriptions.getErr = errors.New("missing metadata")
	if err := conn.WriteJSON(model.WSClientMessage{Type: "unsubscribe", RequestID: "unsub"}); err != nil {
		t.Fatalf("write invalid unsubscribe: %v", err)
	}
	if response := readResponse(t, conn); response.Code != "INVALID_MESSAGE" {
		t.Fatalf("unexpected unsubscribe error: %+v", response)
	}
	subscriptions.getErr = nil

	if err := conn.WriteJSON(model.WSClientMessage{Type: "subscribe", ConsumerID: "consumer", Contracts: []string{"events"}}); err != nil {
		t.Fatalf("write subscribe: %v", err)
	}
	if response := readResponse(t, conn); response.Status != "subscribed" {
		t.Fatalf("unexpected subscription response: %+v", response)
	}
	if err := conn.WriteJSON(model.WSClientMessage{Type: "unsubscribe"}); err != nil {
		t.Fatalf("write unsubscribe: %v", err)
	}
	if response := readResponse(t, conn); response.Status != "unsubscribed" {
		t.Fatalf("unexpected unsubscribe response: %+v", response)
	}

	if err := conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "done"), time.Now().Add(time.Second)); err != nil {
		t.Fatalf("write close: %v", err)
	}
	select {
	case <-analytics.closed:
	case <-time.After(time.Second):
		t.Fatal("handler did not finish after close")
	}
	handler.sendError("missing", "request", "INTERNAL_ERROR", "error")
	if errorMessage("ACCESS_DENIED") != "Access denied" || errorMessage("OTHER") != "Unknown error" {
		t.Fatal("unexpected error message mapping")
	}
}
