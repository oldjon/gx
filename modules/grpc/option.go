package grpc

type UnaryServerMiddlewareName string
type UnaryClientMiddlewareName string

const (
	UnaryServerMiddlewareZapLog        UnaryServerMiddlewareName = "Zaplog"
	UnaryServerMiddlewarePayloadLogger UnaryServerMiddlewareName = "PayloadLogger"
	UnaryServerMiddlewareTag           UnaryServerMiddlewareName = "Tag"
	UnaryServerMiddlewareEventLogger   UnaryServerMiddlewareName = "EventLogger"
	UnaryServerMiddlewareMetrics       UnaryServerMiddlewareName = "Metrics"
	UnaryServerMiddlewareTracer        UnaryServerMiddlewareName = "Tracer"
	UnaryServerMiddlewareRecovery      UnaryServerMiddlewareName = "Recovery"

	UnaryClientMiddlewareZaplog        UnaryClientMiddlewareName = "Zaplog"
	UnaryClientMiddlewarePayloadLogger UnaryClientMiddlewareName = "PayloadLogger"
	UnaryClientMiddlewareTag           UnaryClientMiddlewareName = "Tag"
	UnaryClientMiddlewareRetry         UnaryClientMiddlewareName = "Retry"
	UnaryClientMiddlewareTracer        UnaryClientMiddlewareName = "Tracer"
)

var (
	// DefaultOptions is the default options used by grpc module
	DefaultOptions = Options{
		UnaryServerMiddlewares: []UnaryServerMiddleware{
			{
				Name:        UnaryServerMiddlewareZapLog,
				Interceptor: ZapUnaryServerInterceptor,
			},
			{
				Name:        UnaryServerMiddlewarePayloadLogger,
				Interceptor: PayloadUnaryServerInterceptor,
			},
			{
				Name:        UnaryServerMiddlewareTag,
				Interceptor: TagUnaryServerInterceptor,
			},
			{
				Name:        UnaryServerMiddlewareEventLogger,
				Interceptor: EventLoggingUnaryServerInterceptor,
			},
			{
				Name:        UnaryServerMiddlewareMetrics,
				Interceptor: MetricsUnaryServerInterceptor,
			},
			{
				Name:        UnaryServerMiddlewareTracer,
				Interceptor: TracerUnaryServerInterceptor,
			},
			{
				Name:        UnaryServerMiddlewareRecovery,
				Interceptor: RecoveryUnaryServerInterceptor,
			},
		},
		UnaryClientMiddlewares: []UnaryClientMiddleware{
			{
				Name:        UnaryClientMiddlewareZaplog,
				Interceptor: ZapUnaryClientInterceptor,
			},
			{
				Name:        UnaryClientMiddlewarePayloadLogger,
				Interceptor: PayloadUnaryClientInterceptor,
			},
			{
				Name:        UnaryClientMiddlewareTag,
				Interceptor: TagUnaryClientInterceptor,
			},
			{
				Name:        UnaryClientMiddlewareRetry,
				Interceptor: RetryClientInterceptor,
			},
			{
				Name:        UnaryClientMiddlewareTracer,
				Interceptor: TracerClientInterceptor,
			},
		},
	}
)

type Options struct {
	// UnaryServerMiddlewares could be used to customize grpc unary server interceptors
	//   grpc server loader will use UnaryServerMiddleware in order
	//   STRONGLY recommended don't change the default order of interceptors, the internal order maybe changed without notification
	UnaryServerMiddlewares []UnaryServerMiddleware

	// UnaryClientMiddlewares could be used to customize grpc unary bot interceptors
	//   grpc bot loader will use UnaryClientMiddleware in order
	//   STRONGLY recommended don't change the default order of interceptors, the internal order maybe changed without notification
	UnaryClientMiddlewares []UnaryClientMiddleware
}

type UnaryServerMiddleware struct {
	// Name is used to define certain server interceptor
	//  just for reference, and for human reading
	Name UnaryServerMiddlewareName
	// Interceptor is the customized version of interceptor
	//   if set to nil , server loader will skip load this interceptor
	Interceptor UnaryServerInterceptor
}

type UnaryServerMiddlewares []UnaryServerMiddleware

// AddUnaryServerMiddlewareBefore add an UnaryServerMiddleware just before named middleware
func (o *Options) AddUnaryServerMiddlewareBefore(name UnaryServerMiddlewareName, middleware UnaryServerMiddleware) {
	// find index
	index := -1
	for i, m := range o.UnaryServerMiddlewares {
		if m.Name == name {
			index = i
			break
		}
	}

	if index >= 0 {
		o.UnaryServerMiddlewares = append(o.UnaryServerMiddlewares, UnaryServerMiddleware{})
		copy(o.UnaryServerMiddlewares[index+1:], o.UnaryServerMiddlewares[index:])
		o.UnaryServerMiddlewares[index] = middleware
	} else {
		// append at last
		o.UnaryServerMiddlewares = append(o.UnaryServerMiddlewares, middleware)
	}
}

type UnaryClientMiddleware struct {
	// Key is used to define certain server interceptor
	//  just for reference , and for human reading
	Name UnaryClientMiddlewareName
	// Interceptor is the customized version of interceptor
	//   if set to nil , bot loader will skip load this interceptor
	Interceptor UnaryClientInterceptor
}

// AddUnaryClientMiddlewareBefore add an UnaryClientMiddleware just before named middleware
func (o *Options) AddUnaryClientMiddlewareBefore(name UnaryClientMiddlewareName, middleware UnaryClientMiddleware) {
	// find index
	index := -1
	for i, m := range o.UnaryClientMiddlewares {
		if m.Name == name {
			index = i
			break
		}
	}

	if index >= 0 {
		o.UnaryClientMiddlewares = append(o.UnaryClientMiddlewares, UnaryClientMiddleware{})
		copy(o.UnaryClientMiddlewares[index+1:], o.UnaryClientMiddlewares[index:])
		o.UnaryClientMiddlewares[index] = middleware
	} else {
		// append at last
		o.UnaryClientMiddlewares = append(o.UnaryClientMiddlewares, middleware)
	}
}
