package client

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/onix-air/contacts/internal/model"
	"github.com/redis/go-redis/v9"
)

type redisMemoryHook struct {
	values   map[string]string
	sets     map[string]map[string]struct{}
	scanKeys []string
	fail     map[string]error
}

func newRedisMemoryHook() *redisMemoryHook {
	return &redisMemoryHook{
		values: make(map[string]string),
		sets:   make(map[string]map[string]struct{}),
		fail:   make(map[string]error),
	}
}

func (h *redisMemoryHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *redisMemoryHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (h *redisMemoryHook) ProcessHook(redis.ProcessHook) redis.ProcessHook {
	return func(_ context.Context, cmd redis.Cmder) error {
		name := cmd.Name()
		if err := h.fail[name]; err != nil {
			return err
		}
		args := cmd.Args()
		switch name {
		case "ping":
			cmd.(*redis.StatusCmd).SetVal("PONG")
		case "set":
			h.values[fmt.Sprint(args[1])] = fmt.Sprint(args[2])
			cmd.(*redis.StatusCmd).SetVal("OK")
		case "get":
			value, ok := h.values[fmt.Sprint(args[1])]
			if !ok {
				return redis.Nil
			}
			cmd.(*redis.StringCmd).SetVal(value)
		case "del":
			delete(h.values, fmt.Sprint(args[1]))
			cmd.(*redis.IntCmd).SetVal(1)
		case "sadd":
			key, value := fmt.Sprint(args[1]), fmt.Sprint(args[2])
			if h.sets[key] == nil {
				h.sets[key] = make(map[string]struct{})
			}
			h.sets[key][value] = struct{}{}
			cmd.(*redis.IntCmd).SetVal(1)
		case "expire":
			cmd.(*redis.BoolCmd).SetVal(true)
		case "smembers":
			var values []string
			for value := range h.sets[fmt.Sprint(args[1])] {
				values = append(values, value)
			}
			cmd.(*redis.StringSliceCmd).SetVal(values)
		case "srem":
			delete(h.sets[fmt.Sprint(args[1])], fmt.Sprint(args[2]))
			cmd.(*redis.IntCmd).SetVal(1)
		case "xadd":
			cmd.(*redis.StringCmd).SetVal("1-0")
		case "scan":
			cmd.(*redis.ScanCmd).SetVal(h.scanKeys, 0)
		}
		return nil
	}
}

func withRedisFactory(t *testing.T, hook *redisMemoryHook) {
	t.Helper()
	original := newGoRedisClient
	newGoRedisClient = func(*redis.Options) *redis.Client {
		client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1})
		client.AddHook(hook)
		return client
	}
	t.Cleanup(func() { newGoRedisClient = original })
}

func TestNewRedisClient(t *testing.T) {
	hook := newRedisMemoryHook()
	withRedisFactory(t, hook)

	rc, err := NewRedisClient(context.Background(), "unused")
	if err != nil || rc.analyticsStream != "analytics.events" {
		t.Fatalf("new default Redis client: stream=%q err=%v", rc.analyticsStream, err)
	}
	if rc.GetClient() == nil {
		t.Fatal("expected underlying Redis client")
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("close client: %v", err)
	}

	rc, err = NewRedisClient(context.Background(), "unused", "custom.events")
	if err != nil || rc.analyticsStream != "custom.events" {
		t.Fatalf("new custom Redis client: stream=%q err=%v", rc.analyticsStream, err)
	}

	expected := redis.Nil
	hook.fail["ping"] = expected
	if _, err := NewRedisClient(context.Background(), "unused"); !errors.Is(err, expected) {
		t.Fatalf("expected ping error, got %v", err)
	}
}

func TestRedisRequestAndConnectionMetadata(t *testing.T) {
	hook := newRedisMemoryHook()
	withRedisFactory(t, hook)
	rc, _ := NewRedisClient(context.Background(), "unused")
	ctx := context.Background()

	request := &model.RequestMetadata{ClientID: "client", ConsumerID: "consumer"}
	if err := rc.SetRequest(ctx, "r1", request, time.Minute); err != nil {
		t.Fatalf("set request: %v", err)
	}
	var gotRequest model.RequestMetadata
	if err := rc.GetRequest(ctx, "r1", &gotRequest); err != nil || gotRequest.ClientID != "client" {
		t.Fatalf("get request: %+v err=%v", gotRequest, err)
	}
	if err := rc.DeleteRequest(ctx, "r1"); err != nil {
		t.Fatalf("delete request: %v", err)
	}
	if err := rc.SetRequest(ctx, "bad", func() {}, time.Minute); err == nil {
		t.Fatal("expected request marshal error")
	}
	hook.values["ws:req:invalid"] = "{"
	if err := rc.GetRequest(ctx, "invalid", &gotRequest); err == nil {
		t.Fatal("expected request unmarshal error")
	}
	hook.fail["get"] = redis.Nil
	if err := rc.GetRequest(ctx, "missing", &gotRequest); err == nil {
		t.Fatal("expected request lookup error")
	}
	delete(hook.fail, "get")

	metadata := &model.SubscriptionMetadata{ClientID: "client", ConsumerID: "consumer"}
	if err := rc.SetConnectionMetadata(ctx, "c1", metadata, time.Minute); err != nil {
		t.Fatalf("set metadata: %v", err)
	}
	var gotMetadata model.SubscriptionMetadata
	if err := rc.GetConnectionMetadata(ctx, "c1", &gotMetadata); err != nil || gotMetadata.ClientID != "client" {
		t.Fatalf("get metadata: %+v err=%v", gotMetadata, err)
	}
	if err := rc.DeleteConnectionMetadata(ctx, "c1"); err != nil {
		t.Fatalf("delete metadata: %v", err)
	}
	if err := rc.SetConnectionMetadata(ctx, "bad", func() {}, time.Minute); err == nil {
		t.Fatal("expected metadata marshal error")
	}
	hook.values["ws:conn:invalid"] = "{"
	if err := rc.GetConnectionMetadata(ctx, "invalid", &gotMetadata); err == nil {
		t.Fatal("expected metadata unmarshal error")
	}
	hook.fail["get"] = redis.Nil
	if err := rc.GetConnectionMetadata(ctx, "missing", &gotMetadata); err == nil {
		t.Fatal("expected metadata lookup error")
	}
}

func TestRedisSubscriptionsAndAnalytics(t *testing.T) {
	hook := newRedisMemoryHook()
	withRedisFactory(t, hook)
	rc, _ := NewRedisClient(context.Background(), "unused", "analytics.custom")
	ctx := context.Background()

	if err := rc.AddSubscription(ctx, "consumer", "temperature", "conn", time.Minute); err != nil {
		t.Fatalf("add subscription: %v", err)
	}
	if err := rc.SetSubscriptionExpiry(ctx, "consumer", "temperature", time.Minute); err != nil {
		t.Fatalf("expire subscription: %v", err)
	}
	subscriptions, err := rc.GetSubscriptions(ctx, "consumer", "temperature")
	if err != nil || len(subscriptions) != 1 {
		t.Fatalf("get subscriptions: %v err=%v", subscriptions, err)
	}
	if err := rc.RemoveSubscription(ctx, "consumer", "temperature", "conn"); err != nil {
		t.Fatalf("remove subscription: %v", err)
	}

	if err := rc.PublishAnalyticsEvent(ctx, model.NewAnalyticsEvent("opened")); err != nil {
		t.Fatalf("publish analytics: %v", err)
	}
	if err := rc.PublishAnalyticsEvent(ctx, &model.AnalyticsEvent{Payload: []byte("{")}); err == nil {
		t.Fatal("expected analytics marshal error")
	}

	hook.scanKeys = []string{"ws:subs:consumer:a", "ws:subs:consumer:b"}
	keys, err := rc.GetAllSubscriptionKeys(ctx, "consumer")
	if err != nil || len(keys) != 2 {
		t.Fatalf("scan subscription keys: %v err=%v", keys, err)
	}
	hook.sets[keys[0]] = map[string]struct{}{"conn": {}}
	hook.sets[keys[1]] = map[string]struct{}{"conn": {}}
	if err := rc.RemoveAllConnectionSubscriptions(ctx, "conn"); err != nil {
		t.Fatalf("remove all subscriptions: %v", err)
	}
	hook.fail["srem"] = redis.Nil
	if err := rc.RemoveAllConnectionSubscriptions(ctx, "conn"); err == nil {
		t.Fatal("expected remove subscription error")
	}
	delete(hook.fail, "srem")
	hook.fail["scan"] = redis.Nil
	if _, err := rc.GetAllSubscriptionKeys(ctx, "consumer"); err == nil {
		t.Fatal("expected scan keys error")
	}
	if err := rc.RemoveAllConnectionSubscriptions(ctx, "conn"); err == nil {
		t.Fatal("expected remove-all scan error")
	}
}
