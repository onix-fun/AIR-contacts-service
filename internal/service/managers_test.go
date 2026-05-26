package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/onix-air/contacts/internal/model"
)

type requestStorageStub struct {
	metadata  *model.RequestMetadata
	setErr    error
	getErr    error
	deleteErr error
}

func (s *requestStorageStub) SetRequest(_ context.Context, _ string, value interface{}, _ time.Duration) error {
	if s.setErr != nil {
		return s.setErr
	}
	s.metadata = value.(*model.RequestMetadata)
	return nil
}

func (s *requestStorageStub) GetRequest(_ context.Context, _ string, value interface{}) error {
	if s.getErr != nil {
		return s.getErr
	}
	*value.(*model.RequestMetadata) = *s.metadata
	return nil
}

func (s *requestStorageStub) DeleteRequest(context.Context, string) error { return s.deleteErr }

type subscriptionStorageStub struct {
	metadata      *model.SubscriptionMetadata
	subscribers   []string
	addErr        error
	expireErr     error
	getSubsErr    error
	removeErr     error
	setMetaErr    error
	getMetaErr    error
	deleteMetaErr error
	removeAllErr  error
}

func (s *subscriptionStorageStub) AddSubscription(context.Context, string, string, string, time.Duration) error {
	return s.addErr
}
func (s *subscriptionStorageStub) SetSubscriptionExpiry(context.Context, string, string, time.Duration) error {
	return s.expireErr
}
func (s *subscriptionStorageStub) GetSubscriptions(context.Context, string, string) ([]string, error) {
	return s.subscribers, s.getSubsErr
}
func (s *subscriptionStorageStub) RemoveSubscription(context.Context, string, string, string) error {
	return s.removeErr
}
func (s *subscriptionStorageStub) SetConnectionMetadata(_ context.Context, _ string, value interface{}, _ time.Duration) error {
	if s.setMetaErr == nil {
		s.metadata = value.(*model.SubscriptionMetadata)
	}
	return s.setMetaErr
}
func (s *subscriptionStorageStub) GetConnectionMetadata(_ context.Context, _ string, value interface{}) error {
	if s.getMetaErr != nil {
		return s.getMetaErr
	}
	*value.(*model.SubscriptionMetadata) = *s.metadata
	return nil
}
func (s *subscriptionStorageStub) DeleteConnectionMetadata(context.Context, string) error {
	return s.deleteMetaErr
}
func (s *subscriptionStorageStub) RemoveAllConnectionSubscriptions(context.Context, string) error {
	return s.removeAllErr
}

func TestRequestManager(t *testing.T) {
	storage := &requestStorageStub{}
	manager := NewRequestManager(storage, time.Minute)
	metadata := &model.RequestMetadata{ClientID: "client"}
	if err := manager.StoreRequest(context.Background(), "request", metadata); err != nil {
		t.Fatalf("store request: %v", err)
	}
	got, err := manager.GetRequest(context.Background(), "request")
	if err != nil || got.ClientID != "client" {
		t.Fatalf("get request: %+v err=%v", got, err)
	}
	if err := manager.DeleteRequest(context.Background(), "request"); err != nil {
		t.Fatalf("delete request: %v", err)
	}
	storage.getErr = errors.New("missing")
	if _, err := manager.GetRequest(context.Background(), "missing"); err == nil {
		t.Fatal("expected get request error")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := manager.StartTimeoutChecker(ctx); err != nil {
		t.Fatalf("timeout checker: %v", err)
	}
}

func TestSubscriptionManager(t *testing.T) {
	storage := &subscriptionStorageStub{subscribers: []string{"connection"}}
	manager := NewSubscriptionManager(storage)
	ctx := context.Background()
	metadata := &model.SubscriptionMetadata{ClientID: "client"}

	if err := manager.AddSubscription(ctx, "consumer", "contract", "connection"); err != nil {
		t.Fatalf("add subscription: %v", err)
	}
	storage.addErr = errors.New("add failed")
	if err := manager.AddSubscription(ctx, "consumer", "contract", "connection"); err == nil {
		t.Fatal("expected add error")
	}
	storage.addErr = nil
	storage.expireErr = errors.New("expire failed")
	if err := manager.AddSubscription(ctx, "consumer", "contract", "connection"); err == nil {
		t.Fatal("expected expiry error")
	}
	storage.expireErr = nil

	if err := manager.RemoveSubscription(ctx, "consumer", "contract", "connection"); err != nil {
		t.Fatalf("remove subscription: %v", err)
	}
	subscribers, err := manager.GetSubscribers(ctx, "consumer", "contract")
	if err != nil || len(subscribers) != 1 {
		t.Fatalf("get subscribers: %v err=%v", subscribers, err)
	}
	if err := manager.SaveConnectionMetadata(ctx, "connection", metadata); err != nil {
		t.Fatalf("save metadata: %v", err)
	}
	got, err := manager.GetConnectionMetadata(ctx, "connection")
	if err != nil || got.ClientID != "client" {
		t.Fatalf("get metadata: %+v err=%v", got, err)
	}
	storage.getMetaErr = errors.New("metadata failed")
	if _, err := manager.GetConnectionMetadata(ctx, "connection"); err == nil {
		t.Fatal("expected metadata error")
	}
	if err := manager.RemoveConnectionMetadata(ctx, "connection"); err != nil {
		t.Fatalf("remove metadata: %v", err)
	}
	if err := manager.RemoveAllConnectionSubscriptions(ctx, "connection"); err != nil {
		t.Fatalf("remove all: %v", err)
	}
}
