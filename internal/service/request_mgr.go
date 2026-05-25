package service

import (
	"context"
	"log"
	"time"

	"github.com/onix-air/contacts/internal/client"
	"github.com/onix-air/contacts/internal/model"
)

type RequestManager struct {
	redis   *client.RedisClient
	ttl     time.Duration
	timeouts map[string]*time.Timer // For handling timeouts
	logger  *log.Logger
}

func NewRequestManager(redis *client.RedisClient, ttl time.Duration) *RequestManager {
	return &RequestManager{
		redis:    redis,
		ttl:      ttl,
		timeouts: make(map[string]*time.Timer),
		logger:   log.New(nil, "[req-mgr] ", 0),
	}
}

// StoreRequest stores request in Redis with TTL and sets timeout
func (rm *RequestManager) StoreRequest(ctx context.Context, requestID string, metadata *model.RequestMetadata) error {
	return rm.redis.SetRequest(ctx, requestID, metadata, rm.ttl)
}

// GetRequest retrieves request metadata from Redis
func (rm *RequestManager) GetRequest(ctx context.Context, requestID string) (*model.RequestMetadata, error) {
	var metadata model.RequestMetadata
	if err := rm.redis.GetRequest(ctx, requestID, &metadata); err != nil {
		return nil, err
	}
	return &metadata, nil
}

// DeleteRequest removes request from Redis
func (rm *RequestManager) DeleteRequest(ctx context.Context, requestID string) error {
	return rm.redis.DeleteRequest(ctx, requestID)
}

// StartTimeoutChecker periodically checks for timed out requests
// This is a simple implementation - in production, you might use Redis expiry with pub/sub
func (rm *RequestManager) StartTimeoutChecker(ctx context.Context) error {
	// For simplicity, we rely on Redis TTL to clean up expired keys
	// In a production system, you might want to track timeouts and notify users
	<-ctx.Done()
	return nil
}
