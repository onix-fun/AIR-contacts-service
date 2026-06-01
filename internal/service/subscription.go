package service

import (
	"context"
	"log"
	"time"

	"github.com/onix-air/contacts/internal/client"
	"github.com/onix-air/contacts/internal/model"
)

type SubscriptionManager struct {
	redis  *client.RedisClient
	ttl    time.Duration
	logger *log.Logger
}

const DefaultSubscriptionTTL = 1 * time.Hour

func NewSubscriptionManager(redis *client.RedisClient) *SubscriptionManager {
	return &SubscriptionManager{
		redis:  redis,
		ttl:    DefaultSubscriptionTTL,
		logger: log.New(nil, "[subs-mgr] ", 0),
	}
}

// AddSubscription adds connection to subscription set
func (sm *SubscriptionManager) AddSubscription(ctx context.Context, consumerID, variableName, connectionID string) error {
	if err := sm.redis.AddSubscription(ctx, consumerID, variableName, connectionID, sm.ttl); err != nil {
		return err
	}
	return sm.redis.SetSubscriptionExpiry(ctx, consumerID, variableName, sm.ttl)
}

// RemoveSubscription removes connection from subscription set
func (sm *SubscriptionManager) RemoveSubscription(ctx context.Context, consumerID, variableName, connectionID string) error {
	return sm.redis.RemoveSubscription(ctx, consumerID, variableName, connectionID)
}

// GetSubscribers returns all connection IDs subscribed to a variable
func (sm *SubscriptionManager) GetSubscribers(ctx context.Context, consumerID, variableName string) ([]string, error) {
	return sm.redis.GetSubscriptions(ctx, consumerID, variableName)
}

// SaveConnectionMetadata saves connection subscription metadata
func (sm *SubscriptionManager) SaveConnectionMetadata(ctx context.Context, connectionID string, metadata *model.SubscriptionMetadata) error {
	return sm.redis.SetConnectionMetadata(ctx, connectionID, metadata, sm.ttl)
}

// GetConnectionMetadata retrieves connection subscription metadata
func (sm *SubscriptionManager) GetConnectionMetadata(ctx context.Context, connectionID string) (*model.SubscriptionMetadata, error) {
	var metadata model.SubscriptionMetadata
	if err := sm.redis.GetConnectionMetadata(ctx, connectionID, &metadata); err != nil {
		return nil, err
	}
	return &metadata, nil
}

// RemoveConnectionMetadata removes connection metadata
func (sm *SubscriptionManager) RemoveConnectionMetadata(ctx context.Context, connectionID string) error {
	return sm.redis.DeleteConnectionMetadata(ctx, connectionID)
}

// RemoveAllConnectionSubscriptions removes all subscriptions for a connection
func (sm *SubscriptionManager) RemoveAllConnectionSubscriptions(ctx context.Context, connectionID string) error {
	return sm.redis.RemoveAllConnectionSubscriptions(ctx, connectionID)
}
