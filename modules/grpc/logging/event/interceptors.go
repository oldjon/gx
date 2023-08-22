package event

import (
	"context"

	grpc_tags "github.com/oldjon/gx/modules/grpc/tags"
	"github.com/oldjon/gx/service"
	"google.golang.org/grpc"
)

func UnaryServerInterceptor(logger *service.EventLogger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		tags := grpc_tags.FromContext(ctx)
		// clone global logger to request logger
		newLogger := logger.Clone().SetTags(tags)
		newCtx := WithEventLogger(ctx, newLogger)
		return handler(newCtx, req)
	}
}
