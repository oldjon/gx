package prometheus

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type Options struct {
	ServiceName string
	Registerer  prometheus.Registerer
	ErrorToCode func(err error) codes.Code
}

// New returns a Middleware instance
func New(opts Options) *Middleware {
	return newMiddleware(opts)
}

// Register takes a gRPC server and pre-initializes all counters to 0.
// This allows for easier monitoring in Prometheus (no missing metrics), and should be called *after* all m_services have
// been registered with the server.
// Currently FX will not call this function. It looks unnecessary
// func Register(server *grpc.Server, mid *Middleware) {
// 	serviceInfo := server.GetServiceInfo()
// 	for serviceName, info := range serviceInfo {
// 		for _, mInfo := range info.Methods {
// 			preRegisterMethod(serviceName, &mInfo, mid)
// 		}
// 	}
// }

// UnaryServerInterceptor is a gRPC server-side interceptor that provides Prometheus monitoring for Unary RPCs.
func (m *Middleware) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		monitor := newServerReporter(Unary, info.FullMethod, m)
		monitor.ReceivedMessage(req, nil)

		resp, err := handler(ctx, req)

		monitor.Handled(m.errorToCode(err))
		monitor.SentMessage(resp, err)

		return resp, err
	}
}

// StreamServerInterceptor is a gRPC server-side interceptor that provides Prometheus monitoring for Streaming RPCs.
func (m *Middleware) StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		monitor := newServerReporter(streamRPCType(info), info.FullMethod, m)
		err := handler(srv, &monitoredServerStream{ss, monitor})
		monitor.Handled(m.errorToCode(err))
		return err
	}
}

func streamRPCType(info *grpc.StreamServerInfo) grpcType {
	if info.IsClientStream && !info.IsServerStream {
		return ClientStream
	} else if !info.IsClientStream && info.IsServerStream {
		return ServerStream
	}
	return BidiStream
}

// monitoredStream wraps grpc.ServerStream allowing each Sent/Recv of message to increment counters.
type monitoredServerStream struct {
	grpc.ServerStream
	monitor *serverReporter
}

func (s *monitoredServerStream) SendMsg(m interface{}) error {
	err := s.ServerStream.SendMsg(m)

	s.monitor.SentMessage(m, err)

	return err
}

func (s *monitoredServerStream) RecvMsg(m interface{}) error {
	err := s.ServerStream.RecvMsg(m)

	s.monitor.ReceivedMessage(m, err) //todo, verify this logic is right or not

	return err
}
