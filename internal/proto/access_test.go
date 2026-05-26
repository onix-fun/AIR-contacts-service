package access

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc"
)

type connStub struct {
	err error
}

func (c *connStub) Invoke(_ context.Context, method string, _ interface{}, reply interface{}, _ ...grpc.CallOption) error {
	if method != AccessService_Check_FullMethodName {
		return errors.New("unexpected method")
	}
	if c.err != nil {
		return c.err
	}
	reply.(*CheckResponse).Allowed = true
	return nil
}

func (c *connStub) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, errors.New("unused")
}

type serverStub struct {
	UnimplementedAccessServiceServer
	called bool
}

func (s *serverStub) Check(context.Context, *CheckRequest) (*CheckResponse, error) {
	s.called = true
	return &CheckResponse{Allowed: true}, nil
}

type unsafeServerStub struct{}

func (*unsafeServerStub) Check(context.Context, *CheckRequest) (*CheckResponse, error) {
	return &CheckResponse{}, nil
}

func (*unsafeServerStub) mustEmbedUnimplementedAccessServiceServer() {}

func TestCheckMessages(t *testing.T) {
	req := &CheckRequest{
		ClientId:     "client",
		ConsumerId:   "consumer",
		ContractName: "contract",
		Direction:    "READ",
	}
	_ = req.String()
	_ = req.ProtoReflect()
	req.Reset()
	req.ClientId, req.ConsumerId, req.ContractName, req.Direction = "client", "consumer", "contract", "READ"
	if req.GetClientId() != "client" || req.GetConsumerId() != "consumer" ||
		req.GetContractName() != "contract" || req.GetDirection() != "READ" {
		t.Fatalf("unexpected getters: %+v", req)
	}
	descriptor, path := req.Descriptor()
	if len(descriptor) == 0 || path[0] != 0 {
		t.Fatal("expected request descriptor")
	}
	var nilReq *CheckRequest
	_ = nilReq.ProtoReflect()
	if nilReq.GetClientId() != "" || nilReq.GetConsumerId() != "" ||
		nilReq.GetContractName() != "" || nilReq.GetDirection() != "" {
		t.Fatal("expected zero getters on nil request")
	}

	resp := &CheckResponse{Allowed: true}
	_ = resp.String()
	_ = resp.ProtoReflect()
	resp.Reset()
	resp.Allowed = true
	if !resp.GetAllowed() {
		t.Fatal("expected allowed getter")
	}
	descriptor, path = resp.Descriptor()
	if len(descriptor) == 0 || path[0] != 1 {
		t.Fatal("expected response descriptor")
	}
	var nilResp *CheckResponse
	_ = nilResp.ProtoReflect()
	if nilResp.GetAllowed() {
		t.Fatal("expected false getter on nil response")
	}
	file_access_proto_init()
}

func TestAccessServiceClient(t *testing.T) {
	client := NewAccessServiceClient(&connStub{})
	response, err := client.Check(context.Background(), &CheckRequest{})
	if err != nil || !response.Allowed {
		t.Fatalf("check response=%+v err=%v", response, err)
	}

	expected := errors.New("invoke failed")
	client = NewAccessServiceClient(&connStub{err: expected})
	if _, err := client.Check(context.Background(), &CheckRequest{}); !errors.Is(err, expected) {
		t.Fatalf("expected invoke error, got %v", err)
	}
}

func TestAccessServiceServerAndHandler(t *testing.T) {
	unimplemented := UnimplementedAccessServiceServer{}
	if _, err := unimplemented.Check(context.Background(), &CheckRequest{}); err == nil {
		t.Fatal("expected unimplemented error")
	}

	server := &serverStub{}
	RegisterAccessServiceServer(grpc.NewServer(), server)
	RegisterAccessServiceServer(grpc.NewServer(), &unsafeServerStub{})

	expected := errors.New("decode failed")
	if _, err := _AccessService_Check_Handler(server, context.Background(), func(interface{}) error {
		return expected
	}, nil); !errors.Is(err, expected) {
		t.Fatalf("expected decode error, got %v", err)
	}
	if _, err := _AccessService_Check_Handler(server, context.Background(), func(interface{}) error {
		return nil
	}, nil); err != nil || !server.called {
		t.Fatalf("expected direct handler invocation, err=%v", err)
	}

	called := false
	result, err := _AccessService_Check_Handler(server, context.Background(), func(v interface{}) error {
		v.(*CheckRequest).ClientId = "client"
		return nil
	}, func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		called = info.FullMethod == AccessService_Check_FullMethodName && req.(*CheckRequest).ClientId == "client"
		return handler(ctx, req)
	})
	if err != nil || result.(*CheckResponse).Allowed != true || !called {
		t.Fatalf("intercepted result=%v called=%v err=%v", result, called, err)
	}
}
