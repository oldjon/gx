package tags

import (
	"context"

	"github.com/grpc-ecosystem/go-grpc-middleware/util/metautils"
	"google.golang.org/grpc"
)

// UnaryClientInterceptor returns a new unary server interceptor for OpenTracing.
func UnaryClientInterceptor(opts ...Option) grpc.UnaryClientInterceptor {
	o := evaluateOptions(opts)
	return func(parentCtx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		newCtx, err := injectHeaderTags(parentCtx, o)
		// use old context if on error, which does not contain tags in metadata
		if err != nil {
			newCtx = parentCtx
		}
		return invoker(newCtx, method, req, reply, cc, opts...)
	}
}

// UnaryServerInterceptor returns a new unary server interceptors that sets the values for request tags.
func UnaryServerInterceptor(opts ...Option) grpc.UnaryServerInterceptor {
	o := evaluateOptions(opts)
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		newCtx, err := extractHeaderTags(ctx, o)
		// use old context if on error, which does not contain tags
		if err != nil {
			newCtx = ctx
		}
		return handler(newCtx, req)
	}
}

// todo: add stream interceptor

func extractHeaderTags(ctx context.Context, o *options) (context.Context, error) {
	headerPropagator := textMapPropagator{o.tagsPrefix}
	md := metautils.ExtractIncoming(ctx)
	tags, err := headerPropagator.Extract(metadataTextMap(md))
	if err != nil {
		return nil, err
	}
	return WithTags(ctx, tags), nil
}

func injectHeaderTags(ctx context.Context, o *options) (context.Context, error) {
	headerPropagator := textMapPropagator{o.tagsPrefix}
	tags := FromContext(ctx)
	md := metautils.ExtractOutgoing(ctx).Clone()
	err := headerPropagator.Inject(tags, metadataTextMap(md))
	if err != nil {
		return nil, err
	}
	return md.ToOutgoing(ctx), nil
}
