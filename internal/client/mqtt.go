package client

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const (
	QoSAtLeastOnce = 1
)

type MQTTClient struct {
	client mqtt.Client
}

var newMQTTClient = mqtt.NewClient

func NewMQTTClient(brokerURL, username, password string) (*MQTTClient, error) {
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
	opts.ConnectTimeout = 5 * time.Second

	client := newMQTTClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return nil, token.Error()
	}

	return &MQTTClient{client: client}, nil
}

func (mc *MQTTClient) Disconnect(quiesce uint) {
	mc.client.Disconnect(quiesce)
}

// GetClient returns the underlying MQTT client
func (mc *MQTTClient) GetClient() mqtt.Client {
	return mc.client
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

// SubscribeToResponses subscribes to device responses
func (mc *MQTTClient) SubscribeToResponses(consumerID string, handler mqtt.MessageHandler) error {
	topic := fmt.Sprintf("devices/%s/responses", consumerID)
	token := mc.client.Subscribe(topic, byte(QoSAtLeastOnce), handler)
	token.Wait()
	return token.Error()
}

// SubscribeToEvents subscribes to device events
func (mc *MQTTClient) SubscribeToEvents(consumerID string, handler mqtt.MessageHandler) error {
	topic := fmt.Sprintf("devices/%s/events", consumerID)
	token := mc.client.Subscribe(topic, byte(QoSAtLeastOnce), handler)
	token.Wait()
	return token.Error()
}

// UnsubscribeFromResponses unsubscribes from device responses
func (mc *MQTTClient) UnsubscribeFromResponses(consumerID string) error {
	topic := fmt.Sprintf("devices/%s/responses", consumerID)
	token := mc.client.Unsubscribe(topic)
	token.Wait()
	return token.Error()
}

// UnsubscribeFromEvents unsubscribes from device events
func (mc *MQTTClient) UnsubscribeFromEvents(consumerID string) error {
	topic := fmt.Sprintf("devices/%s/events", consumerID)
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
