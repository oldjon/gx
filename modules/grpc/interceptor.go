package grpc

import (
	"context"
	"time"

	"github.com/google/uuid"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	grpc_tags "github.com/oldjon/gx/modules/grpc/tags"
	"github.com/oldjon/gx/service"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type UnaryServerInterceptor func(options map[string]interface{}) grpc.UnaryServerInterceptor

func (m *module) createServerUnaryInterceptors(middlewares []UnaryServerMiddleware, isOpenLogAddUUID bool, logger *zap.Logger) []grpc.UnaryServerInterceptor {
	interceptors := make([]grpc.UnaryServerInterceptor, 0)
	for _, middleware := range middlewares {
		if isOpenLogAddUUID && middleware.Name == UnaryServerMiddlewarePayloadLogger {
			middleware.Interceptor = PayloadUnaryServerInterceptorWithUUID
		}

		interceptor := m.createServerUnaryInterceptor(middleware)
		if interceptor != nil {
			logger.Info("grpc server unary interceptor is loaded",
				zap.String("middleware", string(middleware.Name)),
				zap.String("module", m.driver.ModuleName()))
			interceptors = append(interceptors, interceptor)
		} else {
			logger.Warn("grpc server unary interceptor is skipped",
				zap.String("middleware", string(middleware.Name)),
				zap.String("module", m.driver.ModuleName()))
		}
	}

	return interceptors
}

func (m *module) createServerUnaryInterceptor(middleware UnaryServerMiddleware) grpc.UnaryServerInterceptor {
	mi := middleware.Interceptor
	if mi == nil {
		return nil
	}

	// build options
	options := make(map[string]interface{})
	options["gx.host"] = m.driver.Host()
	options["gx.module"] = m

	interceptor := mi(options)

	return interceptor
}

func ZapUnaryServerInterceptor(options map[string]interface{}) grpc.UnaryServerInterceptor {
	host := options["gx.host"].(service.Host)
	logger := host.Logger()
	return grpc_zap.UnaryServerInterceptor(logger, grpc_zap.WithCodes(errorToCode))
}

func PayloadUnaryServerInterceptor(options map[string]interface{}) grpc.UnaryServerInterceptor {
	host := options["gx.host"].(service.Host)
	logger := host.Logger()

	decider := func(ctx context.Context, fullMethodName string, servingObject interface{}) bool {
		return logger.Core().Enabled(zap.DebugLevel)
	}
	return grpc_zap.PayloadUnaryServerInterceptor(logger, decider)
}

func PayloadUnaryServerInterceptorWithUUID(options map[string]interface{}) grpc.UnaryServerInterceptor {
	host := options["gx.host"].(service.Host)
	logger := host.Logger()

	decider := func(ctx context.Context, fullMethodName string, servingObject interface{}) bool {
		grpc_ctxtags.Extract(ctx).Set("uuid", uuid.NewString())
		return logger.Core().Enabled(zap.InfoLevel)
	}
	return grpc_zap.PayloadUnaryServerInterceptor(logger, decider)
}

func TagUnaryServerInterceptor(options map[string]interface{}) grpc.UnaryServerInterceptor {
	_ = options
	return grpc_tags.UnaryServerInterceptor()
}

// func EventLoggingUnaryServerInterceptor(options map[string]interface{}) grpc.UnaryServerInterceptor {
// 	host := options["gx.host"].(service.Host)
//
// 	eventLogger := host.EventLogger()
//
// 	if eventLogger != nil {
// 		return event_logging.UnaryServerInterceptor(eventLogger)
// 	}
// 	return nil
// }

func MetricsUnaryServerInterceptor(options map[string]interface{}) grpc.UnaryServerInterceptor {
	m := options["gx.module"].(*module)
	prometheus := m.prom
	return prometheus.UnaryServerInterceptor()
}

// func TracerUnaryServerInterceptor(options map[string]interface{}) grpc.UnaryServerInterceptor {
// 	host := options["gx.host"].(service.Host)
// 	tracer := host.Tracer()
// 	return grpc_ot.UnaryServerInterceptor(grpc_ot.WithTracer(tracer))
// }

func RecoveryUnaryServerInterceptor(options map[string]interface{}) grpc.UnaryServerInterceptor {
	host := options["gx.host"].(service.Host)
	logger := host.Logger()

	m := options["gx.module"].(*module)
	recovery := m.recovery
	return recovery.UnaryServerInterceptor(logger)
}

type UnaryClientInterceptor func(options map[string]interface{}) grpc.UnaryClientInterceptor

func (d *Dialer) createClientUnaryInterceptors(middlewares []UnaryClientMiddleware, logger *zap.Logger) []grpc.UnaryClientInterceptor {
	_ = logger

	interceptors := make([]grpc.UnaryClientInterceptor, 0)
	for _, middleware := range middlewares {
		interceptor := d.createClientUnaryInterceptor(middleware)
		if interceptor != nil {
			// logger.Info("grpc bot unary interceptor is loaded",
			//	zap.Uint32("unary_interceptor_key", uint32(option.Key)))
			interceptors = append(interceptors, interceptor)
			// } else {
			// logger.Warn("grpc bot unary interceptor is skipped",
			//	zap.Uint32("unary_interceptor_key", uint32(option.Key)))
		}
	}

	return interceptors
}

func (d *Dialer) createClientUnaryInterceptor(middleware UnaryClientMiddleware) grpc.UnaryClientInterceptor {
	mi := middleware.Interceptor
	if mi == nil {
		return nil
	}

	// build options
	options := make(map[string]interface{})
	options["gx.dialer"] = d

	interceptor := mi(options)

	return interceptor
}

func ZapUnaryClientInterceptor(options map[string]interface{}) grpc.UnaryClientInterceptor {
	d := options["gx.dialer"].(*Dialer)
	logger := d.Logger

	return grpc_zap.UnaryClientInterceptor(logger)
}

func PayloadUnaryClientInterceptor(options map[string]interface{}) grpc.UnaryClientInterceptor {
	d := options["gx.dialer"].(*Dialer)
	logger := d.Logger

	decider := func(ctx context.Context, fullMethodName string) bool {
		return logger.Core().Enabled(zap.DebugLevel)
	}
	return grpc_zap.PayloadUnaryClientInterceptor(logger, decider)
}

func TagUnaryClientInterceptor(options map[string]interface{}) grpc.UnaryClientInterceptor {
	_ = options
	return grpc_tags.UnaryClientInterceptor()
}

func RetryClientInterceptor(options map[string]interface{}) grpc.UnaryClientInterceptor {
	_ = options

	callopts := []grpc_retry.CallOption{
		grpc_retry.WithMax(3),
		grpc_retry.WithBackoff(grpc_retry.BackoffLinearWithJitter(500*time.Millisecond, 0.2)),
	}
	return grpc_retry.UnaryClientInterceptor(callopts...)
}

// func TracerClientInterceptor(options map[string]interface{}) grpc.UnaryClientInterceptor {
// 	d := options["gx.dialer"].(*Dialer)
// 	tracer := d.Tracer
//
// 	return grpc_ot.UnaryClientInterceptor(grpc_ot.WithTracer(tracer))
// }
