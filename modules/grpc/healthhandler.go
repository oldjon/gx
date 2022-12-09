package grpc

import (
	"context"

	"google.golang.org/grpc"

	healthproto "github.com/oldjon/gx/modules/grpc/proto"
)

type healthHandler struct {
}

func (h *healthHandler) RegisterServer(s *grpc.Server) {
	healthproto.RegisterFxHealthServiceServer(s, h)
}

func (h *healthHandler) Ping(ctx context.Context, req *healthproto.FxHealth_PingRequest) (*healthproto.FxHealth_PingResponse, error) {
	_ = ctx
	_ = req

	return &healthproto.FxHealth_PingResponse{}, nil
}
