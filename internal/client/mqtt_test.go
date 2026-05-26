package client

import (
	"context"
	"errors"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type mqttTokenStub struct {
	err error
}

func (t *mqttTokenStub) Wait() bool                     { return true }
func (t *mqttTokenStub) WaitTimeout(time.Duration) bool { return true }
func (t *mqttTokenStub) Done() <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}
func (t *mqttTokenStub) Error() error { return t.err }

type mqttClientStub struct {
	connected      bool
	connectToken   mqtt.Token
	operationToken mqtt.Token
	publishedTopic string
	payload        interface{}
	subscribed     []string
	unsubscribed   []string
	disconnected   uint
}

func (c *mqttClientStub) IsConnected() bool      { return c.connected }
func (c *mqttClientStub) IsConnectionOpen() bool { return c.connected }
func (c *mqttClientStub) Connect() mqtt.Token    { return c.connectToken }
func (c *mqttClientStub) Disconnect(q uint)      { c.disconnected = q }
func (c *mqttClientStub) Publish(topic string, _ byte, _ bool, payload interface{}) mqtt.Token {
	c.publishedTopic, c.payload = topic, payload
	return c.operationToken
}
func (c *mqttClientStub) Subscribe(topic string, _ byte, _ mqtt.MessageHandler) mqtt.Token {
	c.subscribed = append(c.subscribed, topic)
	return c.operationToken
}
func (c *mqttClientStub) SubscribeMultiple(map[string]byte, mqtt.MessageHandler) mqtt.Token {
	return c.operationToken
}
func (c *mqttClientStub) Unsubscribe(topics ...string) mqtt.Token {
	c.unsubscribed = append(c.unsubscribed, topics...)
	return c.operationToken
}
func (c *mqttClientStub) AddRoute(string, mqtt.MessageHandler) {}
func (c *mqttClientStub) OptionsReader() mqtt.ClientOptionsReader {
	return mqtt.NewOptionsReader(mqtt.NewClientOptions())
}

func TestNewMQTTClient(t *testing.T) {
	original := newMQTTClient
	defer func() { newMQTTClient = original }()

	success := &mqttClientStub{connectToken: &mqttTokenStub{}, operationToken: &mqttTokenStub{}}
	newMQTTClient = func(*mqtt.ClientOptions) mqtt.Client { return success }
	mc, err := NewMQTTClient("tcp://broker:1883", "user", "secret")
	if err != nil || mc.GetClient() != success {
		t.Fatalf("new MQTT client: client=%v err=%v", mc, err)
	}

	expected := errors.New("connect failed")
	newMQTTClient = func(*mqtt.ClientOptions) mqtt.Client {
		return &mqttClientStub{connectToken: &mqttTokenStub{err: expected}}
	}
	if _, err := NewMQTTClient("tcp://broker:1883", "", ""); !errors.Is(err, expected) {
		t.Fatalf("expected connection error, got %v", err)
	}
}

func TestMQTTClientOperations(t *testing.T) {
	expected := errors.New("operation failed")
	stub := &mqttClientStub{connected: true, operationToken: &mqttTokenStub{}}
	mc := &MQTTClient{client: stub}

	if err := mc.PublishCommand(map[string]bool{"on": true}, "consumer"); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if stub.publishedTopic != "devices/consumer/commands" {
		t.Fatalf("unexpected publish topic %q", stub.publishedTopic)
	}
	if err := mc.PublishCommand(func() {}, "consumer"); err == nil {
		t.Fatal("expected marshal error")
	}
	if err := mc.SubscribeToResponses("consumer", nil); err != nil {
		t.Fatalf("subscribe responses: %v", err)
	}
	if err := mc.SubscribeToEvents("consumer", nil); err != nil {
		t.Fatalf("subscribe events: %v", err)
	}
	if err := mc.UnsubscribeFromResponses("consumer"); err != nil {
		t.Fatalf("unsubscribe responses: %v", err)
	}
	if err := mc.UnsubscribeFromEvents("consumer"); err != nil {
		t.Fatalf("unsubscribe events: %v", err)
	}
	if !mc.IsConnected() || mc.GetClient() != stub {
		t.Fatal("expected connected underlying MQTT client")
	}
	mc.Disconnect(50)
	if stub.disconnected != 50 {
		t.Fatalf("unexpected quiesce %d", stub.disconnected)
	}

	stub.operationToken = &mqttTokenStub{err: expected}
	if err := mc.PublishCommand("payload", "consumer"); !errors.Is(err, expected) {
		t.Fatalf("expected publish error, got %v", err)
	}
	if err := mc.SubscribeToResponses("consumer", nil); !errors.Is(err, expected) {
		t.Fatalf("expected response subscription error, got %v", err)
	}
	if err := mc.SubscribeToEvents("consumer", nil); !errors.Is(err, expected) {
		t.Fatalf("expected event subscription error, got %v", err)
	}
	if err := mc.UnsubscribeFromResponses("consumer"); !errors.Is(err, expected) {
		t.Fatalf("expected response unsubscribe error, got %v", err)
	}
	if err := mc.UnsubscribeFromEvents("consumer"); !errors.Is(err, expected) {
		t.Fatalf("expected event unsubscribe error, got %v", err)
	}
}

func TestMQTTClientWaitForConnection(t *testing.T) {
	mc := &MQTTClient{client: &mqttClientStub{connected: true}}
	if err := mc.WaitForConnection(context.Background(), time.Second); err != nil {
		t.Fatalf("wait for connected client: %v", err)
	}

	mc.client = &mqttClientStub{}
	if err := mc.WaitForConnection(context.Background(), time.Nanosecond); err == nil {
		t.Fatal("expected connection timeout")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := mc.WaitForConnection(ctx, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
}
