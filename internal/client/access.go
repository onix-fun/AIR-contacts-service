package client

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/onix-air/contacts/internal/observability"
	pb "github.com/onix-air/contacts/internal/proto"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type AccessServiceClient struct {
	conn   *grpc.ClientConn
	client pb.AccessServiceClient
}

func NewAccessServiceClient(addr string) (*AccessServiceClient, error) {
	conn, err := grpc.Dial(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		return nil, err
	}

	client := pb.NewAccessServiceClient(conn)
	return &AccessServiceClient{
		conn:   conn,
		client: client,
	}, nil
}

func (ac *AccessServiceClient) Close() error {
	return ac.conn.Close()
}

// Check checks if client has access to contract on consumer for READ or WRITE direction.
// clientID and consumerID should be valid UUIDs
func (ac *AccessServiceClient) Check(ctx context.Context, clientID, consumerID, contractName, direction string) (bool, error) {
	// Validate UUIDs
	if _, err := uuid.Parse(clientID); err != nil {
		return false, err
	}
	if _, err := uuid.Parse(consumerID); err != nil {
		return false, err
	}

	req := &pb.CheckRequest{
		ClientId:     clientID,
		ConsumerId:   consumerID,
		ContractName: contractName,
		Direction:    direction,
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if correlationID := observability.CorrelationID(ctx); correlationID != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-correlation-id", correlationID)
	}

	resp, err := ac.client.Check(ctx, req)
	if err != nil {
		return false, err
	}

	return resp.Allowed, nil
}
