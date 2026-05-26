package client

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/onix-air/contacts/internal/model"
	"github.com/redis/go-redis/v9"
)

type RedisClient struct {
	client          *redis.Client
	analyticsStream string
	logger          *log.Logger
}

var newGoRedisClient = redis.NewClient

func NewRedisClient(ctx context.Context, addr string, analyticsStream ...string) (*RedisClient, error) {
	client := newGoRedisClient(&redis.Options{
		Addr:         addr,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})

	// Test connection
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	stream := "analytics.events"
	if len(analyticsStream) > 0 && analyticsStream[0] != "" {
		stream = analyticsStream[0]
	}

	return &RedisClient{
		client:          client,
		analyticsStream: stream,
		logger:          log.New(nil, "[redis] ", 0),
	}, nil
}

func (rc *RedisClient) Close() error {
	return rc.client.Close()
}

// SetRequest stores request metadata in Redis with 30s TTL
func (rc *RedisClient) SetRequest(ctx context.Context, requestID string, metadata interface{}, ttl time.Duration) error {
	data, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	return rc.client.Set(ctx, "ws:req:"+requestID, string(data), ttl).Err()
}

// GetRequest retrieves request metadata from Redis
func (rc *RedisClient) GetRequest(ctx context.Context, requestID string, v interface{}) error {
	val, err := rc.client.Get(ctx, "ws:req:"+requestID).Result()
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(val), v)
}

// DeleteRequest removes request from Redis
func (rc *RedisClient) DeleteRequest(ctx context.Context, requestID string) error {
	return rc.client.Del(ctx, "ws:req:"+requestID).Err()
}

// AddSubscription adds connection to subscription set
func (rc *RedisClient) AddSubscription(ctx context.Context, consumerID, contractName, connectionID string, ttl time.Duration) error {
	key := "ws:subs:" + consumerID + ":" + contractName
	return rc.client.SAdd(ctx, key, connectionID).Err()
}

// SetSubscriptionExpiry sets expiry for subscription key
func (rc *RedisClient) SetSubscriptionExpiry(ctx context.Context, consumerID, contractName string, ttl time.Duration) error {
	key := "ws:subs:" + consumerID + ":" + contractName
	return rc.client.Expire(ctx, key, ttl).Err()
}

// GetSubscriptions gets all connection IDs for a contract
func (rc *RedisClient) GetSubscriptions(ctx context.Context, consumerID, contractName string) ([]string, error) {
	key := "ws:subs:" + consumerID + ":" + contractName
	return rc.client.SMembers(ctx, key).Result()
}

// RemoveSubscription removes connection from subscription set
func (rc *RedisClient) RemoveSubscription(ctx context.Context, consumerID, contractName, connectionID string) error {
	key := "ws:subs:" + consumerID + ":" + contractName
	return rc.client.SRem(ctx, key, connectionID).Err()
}

// SetConnectionMetadata stores connection subscription metadata
func (rc *RedisClient) SetConnectionMetadata(ctx context.Context, connectionID string, metadata interface{}, ttl time.Duration) error {
	data, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	return rc.client.Set(ctx, "ws:conn:"+connectionID, string(data), ttl).Err()
}

// GetConnectionMetadata retrieves connection metadata
func (rc *RedisClient) GetConnectionMetadata(ctx context.Context, connectionID string, v interface{}) error {
	val, err := rc.client.Get(ctx, "ws:conn:"+connectionID).Result()
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(val), v)
}

// DeleteConnectionMetadata removes connection metadata
func (rc *RedisClient) DeleteConnectionMetadata(ctx context.Context, connectionID string) error {
	return rc.client.Del(ctx, "ws:conn:"+connectionID).Err()
}

// PublishAnalyticsEvent writes a durable analytics event to Redis Streams.
func (rc *RedisClient) PublishAnalyticsEvent(ctx context.Context, event *model.AnalyticsEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return rc.client.XAdd(ctx, &redis.XAddArgs{
		Stream: rc.analyticsStream,
		Values: map[string]interface{}{
			"event": string(data),
		},
	}).Err()
}

// GetAllSubscriptionKeys gets all subscription keys for a consumer
func (rc *RedisClient) GetAllSubscriptionKeys(ctx context.Context, consumerID string) ([]string, error) {
	pattern := "ws:subs:" + consumerID + ":*"
	iter := rc.client.Scan(ctx, 0, pattern, 100).Iterator()
	var keys []string
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	return keys, iter.Err()
}

// RemoveAllConnectionSubscriptions removes all subscriptions for a connection
func (rc *RedisClient) RemoveAllConnectionSubscriptions(ctx context.Context, connectionID string) error {
	// Get all subscription keys
	iter := rc.client.Scan(ctx, 0, "ws:subs:*", 0).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		// Remove connection from each subscription set
		if err := rc.client.SRem(ctx, key, connectionID).Err(); err != nil {
			return err
		}
	}
	return iter.Err()
}

// GetClient returns the underlying Redis client
func (rc *RedisClient) GetClient() *redis.Client {
	return rc.client
}
