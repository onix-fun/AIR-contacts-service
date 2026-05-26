package client

import (
	"context"
	"errors"
	"testing"

	"github.com/onix-air/contacts/internal/observability"
	pb "github.com/onix-air/contacts/internal/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	validClientID   = "00000000-0000-0000-0000-000000000001"
	validConsumerID = "00000000-0000-0000-0000-000000000002"
)

type accessRPCStub struct {
	response *pb.CheckResponse
	err      error
	request  *pb.CheckRequest
	md       metadata.MD
}

func (s *accessRPCStub) Check(ctx context.Context, req *pb.CheckRequest, _ ...grpc.CallOption) (*pb.CheckResponse, error) {
	s.request = req
	s.md, _ = metadata.FromOutgoingContext(ctx)
	return s.response, s.err
}

func TestNewAccessServiceClientAndClose(t *testing.T) {
	original := dialGRPC
	defer func() { dialGRPC = original }()

	expected := errors.New("dial failed")
	dialGRPC = func(string, ...grpc.DialOption) (*grpc.ClientConn, error) {
		return nil, expected
	}
	if _, err := NewAccessServiceClient("unused"); !errors.Is(err, expected) {
		t.Fatalf("expected dial error, got %v", err)
	}

	dialGRPC = original
	ac, err := NewAccessServiceClient("passthrough:///unused")
	if err != nil {
		t.Fatalf("new access client: %v", err)
	}
	if err := ac.Close(); err != nil {
		t.Fatalf("close access client: %v", err)
	}
}

func TestAccessServiceClientCheck(t *testing.T) {
	ac := &AccessServiceClient{client: &accessRPCStub{}}
	if _, err := ac.Check(context.Background(), "bad", validConsumerID, "door", "READ"); err == nil {
		t.Fatal("expected invalid client UUID error")
	}
	if _, err := ac.Check(context.Background(), validClientID, "bad", "door", "READ"); err == nil {
		t.Fatal("expected invalid consumer UUID error")
	}

	rpc := &accessRPCStub{response: &pb.CheckResponse{Allowed: true}}
	ac.client = rpc
	ctx := observability.WithCorrelationID(context.Background(), "correlation-1")
	allowed, err := ac.Check(ctx, validClientID, validConsumerID, "door", "WRITE")
	if err != nil || !allowed {
		t.Fatalf("expected allowed response, got allowed=%v err=%v", allowed, err)
	}
	if rpc.request.GetContractName() != "door" || rpc.request.GetDirection() != "WRITE" {
		t.Fatalf("unexpected request: %+v", rpc.request)
	}
	if got := rpc.md.Get("x-correlation-id"); len(got) != 1 || got[0] != "correlation-1" {
		t.Fatalf("missing correlation metadata: %v", rpc.md)
	}

	expected := errors.New("rpc failed")
	ac.client = &accessRPCStub{err: expected}
	if _, err := ac.Check(context.Background(), validClientID, validConsumerID, "door", "READ"); !errors.Is(err, expected) {
		t.Fatalf("expected RPC error, got %v", err)
	}
}
