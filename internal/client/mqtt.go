package client

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const (
	QoSAtLeastOnce = 1
)

type mqttConnection interface {
	Disconnect(quiesce uint)
	IsConnected() bool
	Publish(topic string, qos byte, retained bool, payload interface{}) mqtt.Token
	Subscribe(topic string, qos byte, callback mqtt.MessageHandler) mqtt.Token
	Unsubscribe(topics ...string) mqtt.Token
}

type MQTTClient struct {
	client        mqttConnection
	pahoClient    mqtt.Client
	subscriptions map[string]mqtt.MessageHandler
	mu            sync.RWMutex
}

func NewMQTTClient(brokerURL, username, password string) (*MQTTClient, error) {
	mc := &MQTTClient{
		subscriptions: make(map[string]mqtt.MessageHandler),
	}

	opts := mqtt.NewClientOptions()
	opts.AddBroker(brokerURL)
	opts.SetClientID("device-ws-service-" + fmt.Sprintf("%d", time.Now().UnixNano()))
	if username != "" {
		opts.SetUsername(username)
	}
	if password != "" {
		opts.SetPassword(password)
	}
	opts.SetConnectRetry(true)
	opts.SetAutoReconnect(true)
	opts.SetMaxReconnectInterval(10 * time.Second)
	opts.SetConnectionLostHandler(func(_ mqtt.Client, err error) {
		log.Printf("MQTT connection lost: %v", err)
	})
	opts.SetReconnectingHandler(func(_ mqtt.Client, _ *mqtt.ClientOptions) {
		log.Println("MQTT reconnecting to broker")
	})
	opts.SetOnConnectHandler(func(client mqtt.Client) {
		log.Println("MQTT connected to broker")
		go mc.resubscribe(client)
	})
	opts.ConnectTimeout = 5 * time.Second

	pahoClient := mqtt.NewClient(opts)
	mc.client = pahoClient
	mc.pahoClient = pahoClient
	if token := pahoClient.Connect(); token.Wait() && token.Error() != nil {
		return nil, token.Error()
	}

	return mc, nil
}

func (mc *MQTTClient) Disconnect(quiesce uint) {
	mc.client.Disconnect(quiesce)
}

// GetClient returns the underlying MQTT client
func (mc *MQTTClient) GetClient() mqtt.Client {
	return mc.pahoClient
}

// PublishCommand publishes command to device
func (mc *MQTTClient) PublishCommand(payload interface{}, consumerID string) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	topic := fmt.Sprintf("devices/%s/commands", consumerID)
	token := mc.client.Publish(topic, byte(QoSAtLeastOnce), false, data)
	token.Wait()
	return token.Error()
}

// SubscribePersistent registers a subscription and restores it after MQTT reconnects.
func (mc *MQTTClient) SubscribePersistent(topic string, handler mqtt.MessageHandler) error {
	mc.mu.Lock()
	if mc.subscriptions == nil {
		mc.subscriptions = make(map[string]mqtt.MessageHandler)
	}
	mc.subscriptions[topic] = handler
	mc.mu.Unlock()

	if !mc.client.IsConnected() {
		return nil
	}

	return subscribe(mc.client, topic, handler)
}

// SubscribeToResponses subscribes to device responses
func (mc *MQTTClient) SubscribeToResponses(consumerID string, handler mqtt.MessageHandler) error {
	topic := fmt.Sprintf("devices/%s/responses", consumerID)
	return mc.SubscribePersistent(topic, handler)
}

// SubscribeToEvents subscribes to device events
func (mc *MQTTClient) SubscribeToEvents(consumerID string, handler mqtt.MessageHandler) error {
	topic := fmt.Sprintf("devices/%s/events", consumerID)
	return mc.SubscribePersistent(topic, handler)
}

// UnsubscribeFromResponses unsubscribes from device responses
func (mc *MQTTClient) UnsubscribeFromResponses(consumerID string) error {
	topic := fmt.Sprintf("devices/%s/responses", consumerID)
	mc.forgetSubscription(topic)
	token := mc.client.Unsubscribe(topic)
	token.Wait()
	return token.Error()
}

// UnsubscribeFromEvents unsubscribes from device events
func (mc *MQTTClient) UnsubscribeFromEvents(consumerID string) error {
	topic := fmt.Sprintf("devices/%s/events", consumerID)
	mc.forgetSubscription(topic)
	token := mc.client.Unsubscribe(topic)
	token.Wait()
	return token.Error()
}

// IsConnected checks if MQTT client is connected
func (mc *MQTTClient) IsConnected() bool {
	return mc.client.IsConnected()
}

// WaitForConnection waits for MQTT connection
func (mc *MQTTClient) WaitForConnection(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if mc.IsConnected() {
				return nil
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("MQTT connection timeout")
			}
		}
	}
}

func (mc *MQTTClient) resubscribe(client mqttConnection) {
	mc.mu.RLock()
	subscriptions := make(map[string]mqtt.MessageHandler, len(mc.subscriptions))
	for topic, handler := range mc.subscriptions {
		subscriptions[topic] = handler
	}
	mc.mu.RUnlock()

	for topic, handler := range subscriptions {
		if err := subscribe(client, topic, handler); err != nil {
			log.Printf("MQTT resubscribe failed topic=%s: %v", topic, err)
			continue
		}
		log.Printf("MQTT subscribed topic=%s", topic)
	}
}

func (mc *MQTTClient) forgetSubscription(topic string) {
	mc.mu.Lock()
	delete(mc.subscriptions, topic)
	mc.mu.Unlock()
}

func subscribe(client mqttConnection, topic string, handler mqtt.MessageHandler) error {
	token := client.Subscribe(topic, byte(QoSAtLeastOnce), handler)
	token.Wait()
	return token.Error()
}
