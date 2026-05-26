package service

import (
	"context"
	"log"
	"time"

	"github.com/onix-air/contacts/internal/model"
)

type SubscriptionManager struct {
	redis  subscriptionStorage
	ttl    time.Duration
	logger *log.Logger
}

const DefaultSubscriptionTTL = 1 * time.Hour

type subscriptionStorage interface {
	AddSubscription(context.Context, string, string, string, time.Duration) error
	SetSubscriptionExpiry(context.Context, string, string, time.Duration) error
	GetSubscriptions(context.Context, string, string) ([]string, error)
	RemoveSubscription(context.Context, string, string, string) error
	SetConnectionMetadata(context.Context, string, interface{}, time.Duration) error
	GetConnectionMetadata(context.Context, string, interface{}) error
	DeleteConnectionMetadata(context.Context, string) error
	RemoveAllConnectionSubscriptions(context.Context, string) error
}

func NewSubscriptionManager(redis subscriptionStorage) *SubscriptionManager {
	return &SubscriptionManager{
		redis:  redis,
		ttl:    DefaultSubscriptionTTL,
		logger: log.New(nil, "[subs-mgr] ", 0),
	}
}

// AddSubscription adds connection to subscription set
func (sm *SubscriptionManager) AddSubscription(ctx context.Context, consumerID, contractName, connectionID string) error {
	if err := sm.redis.AddSubscription(ctx, consumerID, contractName, connectionID, sm.ttl); err != nil {
		return err
	}
	return sm.redis.SetSubscriptionExpiry(ctx, consumerID, contractName, sm.ttl)
}

// RemoveSubscription removes connection from subscription set
func (sm *SubscriptionManager) RemoveSubscription(ctx context.Context, consumerID, contractName, connectionID string) error {
	return sm.redis.RemoveSubscription(ctx, consumerID, contractName, connectionID)
}

// GetSubscribers returns all connection IDs subscribed to a contract
func (sm *SubscriptionManager) GetSubscribers(ctx context.Context, consumerID, contractName string) ([]string, error) {
	return sm.redis.GetSubscriptions(ctx, consumerID, contractName)
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
