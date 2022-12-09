package recover

import (
	"context"
	"fmt"

	"github.com/oldjon/gx/common"

	"github.com/oldjon/gx/modules/grpc/prometheus"
	prom "github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Options struct {
	ServiceName string
	Registerer  prom.Registerer
}

type Middleware struct {
	serviceName        string
	serverPanicCounter *prom.CounterVec
}

// New returns a Middleware instance
func New(opts Options) *Middleware {
	return newMiddleware(opts)
}

func newMiddleware(opts Options) *Middleware {
	if opts.Registerer == nil {
		opts.Registerer = prom.DefaultRegisterer
	}

	serverPanicCounter := prom.NewCounterVec(
		prom.CounterOpts{
			Name: "grpc_panic_total",
			Help: "Total number of RPCs panic on the server.",
		}, []string{"grpc_type", "grpc_service", "grpc_method", "service"})
	serverPanicCounter = registerOrGet(opts.Registerer, serverPanicCounter).(*prom.CounterVec)

	return &Middleware{
		serviceName:        opts.ServiceName,
		serverPanicCounter: serverPanicCounter,
	}
}

func registerOrGet(r prom.Registerer, c prom.Collector) prom.Collector {
	if err := r.Register(c); err != nil {
		if are, ok := err.(prom.AlreadyRegisteredError); ok {
			return are.ExistingCollector
		}
		panic(err)
	}
	return c
}

// UnaryServerInterceptor is a gRPC server-side interceptor that provides Prometheus monitoring for Unary RPCs.
func (m *Middleware) UnaryServerInterceptor(logger *zap.Logger) grpc.UnaryServerInterceptor {
	if common.IsGRPCServerRecoverEnabled() {
		return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (_ interface{}, err error) {
			grpcType := "Unary"
			grpcService, grpcMethod := prometheus.SplitMethodName(info.FullMethod)

			defer func() {
				if r := recover(); r != nil {
					if common.IsGRPCServerRecoverStackTraceEnabled() {
						logger.Error(fmt.Sprintf("recover panic in grpc handler: %s", r),
							zap.String("grpc_type", grpcType),
							zap.String("grpc_service", grpcService),
							zap.String("grpc_method", grpcMethod),
							zap.Stack("stack"),
						)
					}

					err = status.Errorf(codes.Internal, "panic: %s", r)

					// put into metrics , wish the following code will not panic
					m.serverPanicCounter.WithLabelValues(grpcType, grpcService, grpcMethod, m.serviceName).Inc()
				}
			}()

			return handler(ctx, req)
		}
	}

	return nil
}

// StreamServerInterceptor is a gRPC server-side interceptor that provides Prometheus monitoring for Streaming RPCs.
func (m *Middleware) StreamServerInterceptor(logger *zap.Logger) grpc.StreamServerInterceptor {
	if common.IsGRPCServerRecoverEnabled() {
		return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
			grpcType := "Stream"
			grpcService, grpcMethod := prometheus.SplitMethodName(info.FullMethod)

			defer func() {
				if r := recover(); r != nil {
					if common.IsGRPCServerRecoverStackTraceEnabled() {
						logger.Error(fmt.Sprintf("recover panic in grpc handler: %s", r),
							zap.String("grpc_type", grpcType),
							zap.String("grpc_service", grpcService),
							zap.String("grpc_method", grpcMethod),
							zap.Stack("stack"),
						)
					}

					err = status.Errorf(codes.Internal, "panic: %s", r)

					// put into metrics , wish the following code will not panic
					m.serverPanicCounter.WithLabelValues(grpcType, grpcService, grpcMethod, m.serviceName).Inc()
				}
			}()

			return handler(srv, stream)
		}
	}

	return nil
}
