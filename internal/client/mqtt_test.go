package client

import (
	"errors"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func TestSubscribePersistentSubscribesImmediatelyWhenConnected(t *testing.T) {
	fake := &fakeMQTTConnection{connected: true}
	mc := &MQTTClient{client: fake, subscriptions: make(map[string]mqtt.MessageHandler)}
	handler := func(mqtt.Client, mqtt.Message) {}

	if err := mc.SubscribePersistent("devices/+/events", handler); err != nil {
		t.Fatalf("SubscribePersistent returned error: %v", err)
	}

	if len(fake.subscribedTopics) != 1 || fake.subscribedTopics[0] != "devices/+/events" {
		t.Fatalf("unexpected subscriptions: %+v", fake.subscribedTopics)
	}
	if _, ok := mc.subscriptions["devices/+/events"]; !ok {
		t.Fatal("expected subscription to be registered for reconnect")
	}
}

func TestSubscribePersistentRestoresRegisteredTopicsAfterReconnect(t *testing.T) {
	fake := &fakeMQTTConnection{connected: false}
	mc := &MQTTClient{client: fake, subscriptions: make(map[string]mqtt.MessageHandler)}
	handler := func(mqtt.Client, mqtt.Message) {}

	if err := mc.SubscribePersistent("devices/+/responses", handler); err != nil {
		t.Fatalf("SubscribePersistent returned error: %v", err)
	}
	if len(fake.subscribedTopics) != 0 {
		t.Fatalf("expected no broker subscribe while disconnected, got %+v", fake.subscribedTopics)
	}

	fake.connected = true
	mc.resubscribe(fake)

	if len(fake.subscribedTopics) != 1 || fake.subscribedTopics[0] != "devices/+/responses" {
		t.Fatalf("unexpected resubscriptions: %+v", fake.subscribedTopics)
	}
}

func TestSubscribePersistentReturnsSubscribeError(t *testing.T) {
	expected := errors.New("subscribe failed")
	fake := &fakeMQTTConnection{connected: true, subscribeErr: expected}
	mc := &MQTTClient{client: fake, subscriptions: make(map[string]mqtt.MessageHandler)}
	handler := func(mqtt.Client, mqtt.Message) {}

	if err := mc.SubscribePersistent("devices/+/events", handler); !errors.Is(err, expected) {
		t.Fatalf("expected subscribe error %v, got %v", expected, err)
	}
	if _, ok := mc.subscriptions["devices/+/events"]; !ok {
		t.Fatal("expected failed subscription to remain registered for reconnect retry")
	}
}

type fakeMQTTConnection struct {
	connected        bool
	subscribeErr     error
	subscribedTopics []string
	unsubscribed     []string
}

func (f *fakeMQTTConnection) Disconnect(uint) {}

func (f *fakeMQTTConnection) IsConnected() bool {
	return f.connected
}

func (f *fakeMQTTConnection) Publish(string, byte, bool, interface{}) mqtt.Token {
	return fakeToken{}
}

func (f *fakeMQTTConnection) Subscribe(topic string, _ byte, _ mqtt.MessageHandler) mqtt.Token {
	f.subscribedTopics = append(f.subscribedTopics, topic)
	return fakeToken{err: f.subscribeErr}
}

func (f *fakeMQTTConnection) Unsubscribe(topics ...string) mqtt.Token {
	f.unsubscribed = append(f.unsubscribed, topics...)
	return fakeToken{}
}

type fakeToken struct {
	err error
}

func (t fakeToken) Wait() bool {
	return true
}

func (t fakeToken) WaitTimeout(time.Duration) bool {
	return true
}

func (t fakeToken) Done() <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}

func (t fakeToken) Error() error {
	return t.err
}
