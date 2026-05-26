package service

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/gorilla/websocket"
	"github.com/onix-air/contacts/internal/model"
)

type analyticsPublisherStub struct {
	events []*model.AnalyticsEvent
	err    error
}

func (s *analyticsPublisherStub) PublishAnalyticsEvent(_ context.Context, event *model.AnalyticsEvent) error {
	s.events = append(s.events, event)
	return s.err
}

type accessCheckerStub struct {
	check func(string) (bool, error)
}

func (s *accessCheckerStub) Check(_ context.Context, _, _, contract, _ string) (bool, error) {
	if s.check != nil {
		return s.check(contract)
	}
	return true, nil
}

type requestManagerStub struct {
	items     map[string]*model.RequestMetadata
	storeErr  error
	getErr    error
	deleteErr error
	deleted   []string
}

func newRequestManagerStub() *requestManagerStub {
	return &requestManagerStub{items: make(map[string]*model.RequestMetadata)}
}

func (s *requestManagerStub) StoreRequest(_ context.Context, id string, metadata *model.RequestMetadata) error {
	if s.storeErr != nil {
		return s.storeErr
	}
	s.items[id] = metadata
	return nil
}

func (s *requestManagerStub) GetRequest(_ context.Context, id string) (*model.RequestMetadata, error) {
	if s.getErr != nil {
		return nil, s.getErr
	}
	metadata, ok := s.items[id]
	if !ok {
		return nil, errors.New("missing request")
	}
	return metadata, nil
}

func (s *requestManagerStub) DeleteRequest(_ context.Context, id string) error {
	s.deleted = append(s.deleted, id)
	delete(s.items, id)
	return s.deleteErr
}

type subscriptionManagerStub struct {
	metadata      *model.SubscriptionMetadata
	subscribers   []string
	saveErr       error
	getMetaErr    error
	addErr        error
	removeErr     error
	removeMetaErr error
	removeAllErr  error
	getSubsErr    error
}

func (s *subscriptionManagerStub) AddSubscription(context.Context, string, string, string) error {
	return s.addErr
}
func (s *subscriptionManagerStub) RemoveSubscription(context.Context, string, string, string) error {
	return s.removeErr
}
func (s *subscriptionManagerStub) GetSubscribers(context.Context, string, string) ([]string, error) {
	return s.subscribers, s.getSubsErr
}
func (s *subscriptionManagerStub) SaveConnectionMetadata(_ context.Context, _ string, metadata *model.SubscriptionMetadata) error {
	if s.saveErr == nil {
		s.metadata = metadata
	}
	return s.saveErr
}
func (s *subscriptionManagerStub) GetConnectionMetadata(context.Context, string) (*model.SubscriptionMetadata, error) {
	if s.getMetaErr != nil {
		return nil, s.getMetaErr
	}
	return s.metadata, nil
}
func (s *subscriptionManagerStub) RemoveConnectionMetadata(context.Context, string) error {
	return s.removeMetaErr
}
func (s *subscriptionManagerStub) RemoveAllConnectionSubscriptions(context.Context, string) error {
	return s.removeAllErr
}

type tokenStub struct {
	err error
}

func (t *tokenStub) Wait() bool                     { return true }
func (t *tokenStub) WaitTimeout(time.Duration) bool { return true }
func (t *tokenStub) Done() <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}
func (t *tokenStub) Error() error { return t.err }

type mqttMessageStub struct {
	payload []byte
}

func (*mqttMessageStub) Duplicate() bool   { return false }
func (*mqttMessageStub) Qos() byte         { return 1 }
func (*mqttMessageStub) Retained() bool    { return false }
func (*mqttMessageStub) Topic() string     { return "topic" }
func (*mqttMessageStub) MessageID() uint16 { return 1 }
func (m *mqttMessageStub) Payload() []byte { return m.payload }
func (*mqttMessageStub) Ack()              {}

type mqttUnderlyingStub struct {
	token       mqtt.Token
	onSubscribe func(string, mqtt.MessageHandler)
}

func (*mqttUnderlyingStub) IsConnected() bool      { return true }
func (*mqttUnderlyingStub) IsConnectionOpen() bool { return true }
func (m *mqttUnderlyingStub) Connect() mqtt.Token  { return m.token }
func (*mqttUnderlyingStub) Disconnect(uint)        {}
func (m *mqttUnderlyingStub) Publish(string, byte, bool, interface{}) mqtt.Token {
	return m.token
}
func (m *mqttUnderlyingStub) Subscribe(topic string, _ byte, handler mqtt.MessageHandler) mqtt.Token {
	if m.onSubscribe != nil {
		m.onSubscribe(topic, handler)
	}
	return m.token
}
func (m *mqttUnderlyingStub) SubscribeMultiple(map[string]byte, mqtt.MessageHandler) mqtt.Token {
	return m.token
}
func (m *mqttUnderlyingStub) Unsubscribe(...string) mqtt.Token   { return m.token }
func (*mqttUnderlyingStub) AddRoute(string, mqtt.MessageHandler) {}
func (*mqttUnderlyingStub) OptionsReader() mqtt.ClientOptionsReader {
	return mqtt.NewOptionsReader(mqtt.NewClientOptions())
}

type mqttGatewayStub struct {
	underlying mqtt.Client
	publishErr error
	published  interface{}
	consumerID string
}

func (s *mqttGatewayStub) PublishCommand(payload interface{}, consumerID string) error {
	s.published, s.consumerID = payload, consumerID
	return s.publishErr
}

func (s *mqttGatewayStub) GetClient() mqtt.Client { return s.underlying }

func newWebSocketServiceStub() (*WebSocketService, *analyticsPublisherStub, *mqttGatewayStub, *accessCheckerStub, *requestManagerStub, *subscriptionManagerStub) {
	analytics := &analyticsPublisherStub{}
	mqttClient := &mqttGatewayStub{underlying: &mqttUnderlyingStub{token: &tokenStub{}}}
	access := &accessCheckerStub{}
	requests := newRequestManagerStub()
	subscriptions := &subscriptionManagerStub{}
	service := NewWebSocketService(analytics, mqttClient, access, requests, subscriptions, log.New(io.Discard, "", 0))
	return service, analytics, mqttClient, access, requests, subscriptions
}

func webSocketPair(t *testing.T, service *WebSocketService) (*websocket.Conn, *websocket.Conn) {
	t.Helper()
	serverConnection := make(chan *websocket.Conn, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := service.UpgradeConnection(w, r)
		if err != nil {
			t.Errorf("upgrade connection: %v", err)
			return
		}
		serverConnection <- conn
	}))
	t.Cleanup(server.Close)
	headers := http.Header{"Origin": []string{"http://example.test"}}
	client, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), headers)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	serverConn := <-serverConnection
	t.Cleanup(func() {
		_ = client.Close()
		_ = serverConn.Close()
	})
	return serverConn, client
}

func TestWebSocketConnectionLifecycle(t *testing.T) {
	service, analytics, _, _, _, subscriptions := newWebSocketServiceStub()
	if _, err := service.UpgradeConnection(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil)); err == nil {
		t.Fatal("expected invalid websocket upgrade error")
	}
	serverConn, clientConn := webSocketPair(t, service)
	service.StoreConnection("connection", serverConn)
	service.StoreConnectionInfo("connection", "client", "unified")
	if conn, ok := service.GetConnection("connection"); !ok || conn != serverConn {
		t.Fatal("expected stored websocket")
	}
	if err := service.SendMessage(serverConn, &model.WSResponse{Type: "pong"}); err != nil {
		t.Fatalf("send message: %v", err)
	}
	var response model.WSResponse
	if err := clientConn.ReadJSON(&response); err != nil || response.Type != "pong" {
		t.Fatalf("read response: %+v err=%v", response, err)
	}
	service.RemoveConnection(context.Background(), "connection")
	if _, ok := service.GetConnection("connection"); ok {
		t.Fatal("expected removed connection")
	}
	if len(analytics.events) == 0 || analytics.events[len(analytics.events)-1].Action != "connection_closed" {
		t.Fatal("expected connection closed analytics event")
	}

	subscriptions.removeAllErr = errors.New("remove all failed")
	subscriptions.removeMetaErr = errors.New("remove metadata failed")
	service.RemoveConnection(context.Background(), "missing")
	analytics.err = errors.New("publish failed")
	service.PublishAnalyticsEvent(context.Background(), model.NewAnalyticsEvent("failed"))
	service.PublishAnalyticsEvent(context.Background(), nil)
	if GenerateConnectionID() == "" {
		t.Fatal("expected generated connection ID")
	}
}

func TestExecuteWrite(t *testing.T) {
	service, _, mqttClient, access, requests, _ := newWebSocketServiceStub()
	ctx := context.Background()
	request := &model.WriteRequest{RequestID: "request", ConsumerID: "consumer", ContractName: "contract", Payload: json.RawMessage(`{"on":true}`)}
	if _, err := service.ExecuteWrite(ctx, "client", &model.WriteRequest{}, "connection"); err == nil {
		t.Fatal("expected invalid request error")
	}
	access.check = func(string) (bool, error) { return false, errors.New("access failed") }
	if _, err := service.ExecuteWrite(ctx, "client", request, "connection"); err == nil {
		t.Fatal("expected access service error")
	}
	access.check = func(string) (bool, error) { return false, nil }
	if _, err := service.ExecuteWrite(ctx, "client", request, "connection"); err == nil {
		t.Fatal("expected access denied error")
	}
	access.check = nil
	requests.storeErr = errors.New("store failed")
	if _, err := service.ExecuteWrite(ctx, "client", request, "connection"); err == nil {
		t.Fatal("expected store error")
	}
	requests.storeErr = nil
	mqttClient.publishErr = errors.New("publish failed")
	if _, err := service.ExecuteWrite(ctx, "client", request, "connection"); err == nil {
		t.Fatal("expected MQTT publish error")
	}
	mqttClient.publishErr = nil

	originalTimeout := commandResponseTimeout
	commandResponseTimeout = time.Millisecond
	defer func() { commandResponseTimeout = originalTimeout }()
	serverConn, clientConn := webSocketPair(t, service)
	service.StoreConnection("connection", serverConn)
	if response, err := service.ExecuteWrite(ctx, "client", request, "connection"); err != nil || response != nil {
		t.Fatalf("execute successful write: response=%v err=%v", response, err)
	}
	_ = clientConn.SetReadDeadline(time.Now().Add(time.Second))
	var timeoutResponse model.WSResponse
	if err := clientConn.ReadJSON(&timeoutResponse); err != nil || timeoutResponse.Code != "TIMEOUT" {
		t.Fatalf("timeout response: %+v err=%v", timeoutResponse, err)
	}
	if len(requests.deleted) == 0 {
		t.Fatal("expected deleted request")
	}
}

func TestSubscriptions(t *testing.T) {
	service, _, _, access, _, subscriptions := newWebSocketServiceStub()
	ctx := context.Background()
	if _, err := service.SubscribeToEvents(ctx, "client", &model.ReadSubscribeRequest{}, "connection"); err == nil {
		t.Fatal("expected invalid subscription")
	}
	access.check = func(contract string) (bool, error) {
		if contract == "error" {
			return false, errors.New("access failed")
		}
		return false, nil
	}
	result, err := service.SubscribeToEvents(ctx, "client", &model.ReadSubscribeRequest{ConsumerID: "consumer", Contracts: []string{"error", "denied"}}, "connection")
	if err != nil || len(result.Denied) != 2 {
		t.Fatalf("denied subscription: %+v err=%v", result, err)
	}
	access.check = nil
	subscriptions.saveErr = errors.New("save failed")
	if _, err := service.SubscribeToEvents(ctx, "client", &model.ReadSubscribeRequest{ConsumerID: "consumer", Contracts: []string{"allowed"}}, "connection"); err == nil {
		t.Fatal("expected metadata store failure")
	}
	subscriptions.saveErr = nil
	subscriptions.addErr = errors.New("add failed")
	if _, err := service.SubscribeToEvents(ctx, "client", &model.ReadSubscribeRequest{ConsumerID: "consumer", Contracts: []string{"allowed"}}, "connection"); err == nil {
		t.Fatal("expected subscription store failure")
	}
	subscriptions.addErr = nil
	access.check = func(contract string) (bool, error) { return contract == "allowed", nil }
	result, err = service.SubscribeToEvents(ctx, "client", &model.ReadSubscribeRequest{ConsumerID: "consumer", Contracts: []string{"allowed", "denied"}}, "connection")
	if err != nil || len(result.Accepted) != 1 || len(result.Denied) != 1 {
		t.Fatalf("successful subscription: %+v err=%v", result, err)
	}
}

func TestUnsubscribe(t *testing.T) {
	service, _, _, _, _, subscriptions := newWebSocketServiceStub()
	ctx := context.Background()
	subscriptions.getMetaErr = errors.New("missing")
	if _, err := service.UnsubscribeFromEvents(ctx, "client", &model.ReadSubscribeRequest{}, "connection"); err == nil {
		t.Fatal("expected missing metadata error")
	}
	subscriptions.getMetaErr = nil
	subscriptions.metadata = &model.SubscriptionMetadata{}
	if _, err := service.UnsubscribeFromEvents(ctx, "client", &model.ReadSubscribeRequest{}, "connection"); err == nil {
		t.Fatal("expected missing consumer error")
	}
	subscriptions.metadata = &model.SubscriptionMetadata{ConsumerID: "consumer", Contracts: []string{"a", "b"}}
	subscriptions.removeErr = errors.New("remove failed")
	if _, err := service.UnsubscribeFromEvents(ctx, "client", &model.ReadSubscribeRequest{Contracts: []string{"a"}}, "connection"); err == nil {
		t.Fatal("expected remove failure")
	}
	subscriptions.removeErr = nil
	subscriptions.saveErr = errors.New("save failed")
	if _, err := service.UnsubscribeFromEvents(ctx, "client", &model.ReadSubscribeRequest{ConsumerID: "other", Contracts: []string{"a"}}, "connection"); err == nil {
		t.Fatal("expected remaining metadata failure")
	}
	subscriptions.saveErr = nil
	subscriptions.metadata = &model.SubscriptionMetadata{ConsumerID: "consumer", Contracts: []string{"a", "b"}}
	result, err := service.UnsubscribeFromEvents(ctx, "client", &model.ReadSubscribeRequest{Contracts: []string{"a"}}, "connection")
	if err != nil || len(result.Accepted) != 1 {
		t.Fatalf("partial unsubscribe: %+v err=%v", result, err)
	}
	subscriptions.metadata = &model.SubscriptionMetadata{ConsumerID: "consumer", Contracts: []string{"a"}}
	result, err = service.UnsubscribeFromEvents(ctx, "client", &model.ReadSubscribeRequest{}, "connection")
	if err != nil || len(result.Accepted) != 1 {
		t.Fatalf("complete unsubscribe: %+v err=%v", result, err)
	}
}

func TestMQTTResponseHandling(t *testing.T) {
	service, _, _, _, requests, _ := newWebSocketServiceStub()
	service.handleMQTTResponse(context.Background(), []byte("{"))
	requests.getErr = errors.New("missing request")
	service.handleMQTTResponse(context.Background(), []byte(`{"request_id":"request","consumer_id":"consumer","contract_name":"contract"}`))
	requests.getErr = nil
	requests.items["request"] = &model.RequestMetadata{ClientID: "client", ConnectionID: "missing"}
	service.handleMQTTResponse(context.Background(), []byte(`{"request_id":"request","consumer_id":"consumer","contract_name":"contract"}`))

	serverConn, clientConn := webSocketPair(t, service)
	service.StoreConnection("connection", serverConn)
	requests.items["success"] = &model.RequestMetadata{ClientID: "client", ConnectionID: "connection"}
	service.handleMQTTResponse(context.Background(), []byte(`{"request_id":"success","consumer_id":"consumer","contract_name":"contract","status":"ok","payload":{"value":1}}`))
	var response model.WSResponse
	if err := clientConn.ReadJSON(&response); err != nil || response.Type != "success" {
		t.Fatalf("MQTT response: %+v err=%v", response, err)
	}

	closedServer, _ := webSocketPair(t, service)
	service.StoreConnection("closed", closedServer)
	_ = closedServer.Close()
	requests.items["closed"] = &model.RequestMetadata{ClientID: "client", ConnectionID: "closed"}
	service.handleMQTTResponse(context.Background(), []byte(`{"request_id":"closed","consumer_id":"consumer","contract_name":"contract"}`))
}

func TestMQTTEventHandling(t *testing.T) {
	service, _, _, _, _, subscriptions := newWebSocketServiceStub()
	service.handleMQTTEvent(context.Background(), []byte("{"))
	subscriptions.getSubsErr = errors.New("lookup failed")
	service.handleMQTTEvent(context.Background(), []byte(`{"consumer_id":"consumer","contract_name":"contract"}`))
	subscriptions.getSubsErr = nil
	subscriptions.subscribers = []string{"missing"}
	service.handleMQTTEvent(context.Background(), []byte(`{"consumer_id":"consumer","contract_name":"contract"}`))

	serverConn, clientConn := webSocketPair(t, service)
	service.StoreConnection("connected", serverConn)
	subscriptions.subscribers = []string{"connected"}
	service.handleMQTTEvent(context.Background(), []byte(`{"consumer_id":"consumer","contract_name":"contract","payload":{"value":1}}`))
	var event model.WSResponse
	if err := clientConn.ReadJSON(&event); err != nil || event.Type != "event" {
		t.Fatalf("MQTT event: %+v err=%v", event, err)
	}

	closedServer, _ := webSocketPair(t, service)
	service.StoreConnection("closed", closedServer)
	_ = closedServer.Close()
	subscriptions.subscribers = []string{"closed"}
	service.handleMQTTEvent(context.Background(), []byte(`{"consumer_id":"consumer","contract_name":"contract"}`))
}

func TestMQTTListeners(t *testing.T) {
	service, _, gateway, _, requests, subscriptions := newWebSocketServiceStub()
	underlying := gateway.underlying.(*mqttUnderlyingStub)
	requests.getErr = errors.New("orphan")
	underlying.onSubscribe = func(_ string, handler mqtt.MessageHandler) {
		handler(underlying, &mqttMessageStub{payload: []byte(`{"request_id":"request"}`)})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := service.ListenMQTTResponses(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("listen responses: %v", err)
	}
	underlying.token = &tokenStub{err: errors.New("subscribe failed")}
	if err := service.ListenMQTTResponses(context.Background()); err == nil {
		t.Fatal("expected response subscribe error")
	}

	underlying.token = &tokenStub{}
	subscriptions.subscribers = nil
	underlying.onSubscribe = func(_ string, handler mqtt.MessageHandler) {
		handler(underlying, &mqttMessageStub{payload: []byte(`{"consumer_id":"consumer","contract_name":"contract"}`)})
	}
	if err := service.ListenMQTTEvents(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("listen events: %v", err)
	}
	underlying.token = &tokenStub{err: errors.New("subscribe failed")}
	if err := service.ListenMQTTEvents(context.Background()); err == nil {
		t.Fatal("expected event subscribe error")
	}
}
