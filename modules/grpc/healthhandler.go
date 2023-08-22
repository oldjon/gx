package grpc

import (
	"context"

	healthproto "github.com/oldjon/gx/modules/grpc/proto"
	"google.golang.org/grpc"
)

type healthHandler struct {
	healthproto.UnimplementedHealthServiceServer
}

func (h *healthHandler) RegisterServer(s *grpc.Server) {
	healthproto.RegisterHealthServiceServer(s, h)
}

func (h *healthHandler) Ping(_ context.Context, _ *healthproto.Health_PingRequest) (*healthproto.Health_PingResponse, error) {
	return &healthproto.Health_PingResponse{}, nil
}
