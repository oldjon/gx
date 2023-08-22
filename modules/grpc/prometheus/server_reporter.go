package prometheus

import (
	"time"

	legacyproto "github.com/golang/protobuf/proto" //nolint staticcheck todo, drop legacyproto support in future
	prom "github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

type grpcType string

const (
	Unary        grpcType = "unary"
	ClientStream grpcType = "client_stream"
	ServerStream grpcType = "server_stream"
	BidiStream   grpcType = "bidi_stream"
)

type Middleware struct {
	serviceName                string
	serverStartedCounter       *prom.CounterVec
	serverHandledCounter       *prom.CounterVec
	serverStreamMsgReceived    *prom.CounterVec
	serverStreamMsgSent        *prom.CounterVec
	serverHandledHistogram     *prom.HistogramVec
	serverSendSizeHistogram    *prom.HistogramVec
	serverReceiveSizeHistogram *prom.HistogramVec
	errorToCode                func(err error) codes.Code
}

func newMiddleware(opts Options) *Middleware {
	if opts.Registerer == nil {
		opts.Registerer = prom.DefaultRegisterer
	}
	if opts.ErrorToCode == nil {
		opts.ErrorToCode = grpc.Code
	}
	serverStartedCounter := prom.NewCounterVec(
		prom.CounterOpts{
			Name: "grpc_started_total",
			Help: "Total number of RPCs started on the server.",
		}, []string{"grpc_type", "grpc_service", "grpc_method", "service"})
	serverStartedCounter = registerOrGet(opts.Registerer, serverStartedCounter).(*prom.CounterVec)

	serverHandledCounter := prom.NewCounterVec(
		prom.CounterOpts{
			Name: "grpc_handled_total",
			Help: "Total number of RPCs completed on the server, regardless of success or failure.",
		}, []string{"grpc_type", "grpc_service", "grpc_method", "grpc_code", "service"})
	serverHandledCounter = registerOrGet(opts.Registerer, serverHandledCounter).(*prom.CounterVec)

	serverStreamMsgReceived := prom.NewCounterVec(
		prom.CounterOpts{
			Name: "grpc_msg_received_total",
			Help: "Total number of RPC stream messages received on the server.",
		}, []string{"grpc_type", "grpc_service", "grpc_method", "service"})
	serverStreamMsgReceived = registerOrGet(opts.Registerer, serverStreamMsgReceived).(*prom.CounterVec)

	serverStreamMsgSent := prom.NewCounterVec(
		prom.CounterOpts{
			Name: "grpc_msg_sent_total",
			Help: "Total number of gRPC stream messages sent by the server.",
		}, []string{"grpc_type", "grpc_service", "grpc_method", "service"})
	serverStreamMsgSent = registerOrGet(opts.Registerer, serverStreamMsgSent).(*prom.CounterVec)

	serverHandledHistogram := prom.NewHistogramVec(
		prom.HistogramOpts{
			Name: "grpc_handling_seconds",
			Help: "Histogram of response latency (seconds) of gRPC that had been application-level handled by the server.",
		},
		[]string{"grpc_type", "grpc_service", "grpc_method", "service"},
	)
	serverHandledHistogram = registerOrGet(opts.Registerer, serverHandledHistogram).(*prom.HistogramVec)

	serverSendSizeHistogram := prom.NewHistogramVec(
		prom.HistogramOpts{
			Name:    "grpc_send_bytes",
			Help:    "Histogram of size of gRPC message sent out by server.",
			Buckets: []float64{5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000},
		},
		[]string{"grpc_type", "grpc_service", "grpc_method", "service"},
	)
	serverSendSizeHistogram = registerOrGet(opts.Registerer, serverSendSizeHistogram).(*prom.HistogramVec)

	serverReceiveSizeHistogram := prom.NewHistogramVec(
		prom.HistogramOpts{
			Name:    "grpc_recv_bytes",
			Help:    "Histogram of size of gRPC message received by server.",
			Buckets: []float64{5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000},
		},
		[]string{"grpc_type", "grpc_service", "grpc_method", "service"},
	)
	serverReceiveSizeHistogram = registerOrGet(opts.Registerer, serverReceiveSizeHistogram).(*prom.HistogramVec)

	return &Middleware{
		serviceName:                opts.ServiceName,
		serverStartedCounter:       serverStartedCounter,
		serverHandledCounter:       serverHandledCounter,
		serverStreamMsgReceived:    serverStreamMsgReceived,
		serverStreamMsgSent:        serverStreamMsgSent,
		serverHandledHistogram:     serverHandledHistogram,
		serverSendSizeHistogram:    serverSendSizeHistogram,
		serverReceiveSizeHistogram: serverReceiveSizeHistogram,
		errorToCode:                opts.ErrorToCode,
	}
}

type serverReporter struct {
	rpcType     grpcType
	serviceName string
	methodName  string
	startTime   time.Time
	mid         *Middleware
}

func newServerReporter(rpcType grpcType, fullMethod string, mid *Middleware) *serverReporter {
	r := &serverReporter{
		rpcType: rpcType,
		mid:     mid,
	}
	r.startTime = time.Now()
	r.serviceName, r.methodName = SplitMethodName(fullMethod)
	r.mid.serverStartedCounter.WithLabelValues(string(r.rpcType), r.serviceName, r.methodName, mid.serviceName).Inc()
	return r
}

func (r *serverReporter) ReceivedMessage(request interface{}, err error) {
	if err != nil {
		r.mid.serverStreamMsgReceived.WithLabelValues(string(r.rpcType), r.serviceName, r.methodName, r.mid.serviceName).Inc()
	}

	// try to convert response to proto.Message to check the size
	msg, ok := request.(proto.Message)
	if ok {
		msgSize := proto.Size(msg)
		r.mid.serverReceiveSizeHistogram.WithLabelValues(string(r.rpcType), r.serviceName, r.methodName, r.mid.serviceName).Observe(float64(msgSize))
	} else {
		lmsg, ok := request.(legacyproto.Message)
		if ok {
			msgSize := legacyproto.Size(lmsg)
			r.mid.serverReceiveSizeHistogram.WithLabelValues(string(r.rpcType), r.serviceName, r.methodName, r.mid.serviceName).Observe(float64(msgSize))
		}
	}
}

// SentMessage called while send response message
func (r *serverReporter) SentMessage(response interface{}, err error) {
	if err != nil {
		r.mid.serverStreamMsgSent.WithLabelValues(string(r.rpcType), r.serviceName, r.methodName, r.mid.serviceName).Inc()
	}

	// try to convert response to proto.Message to check the size
	msg, ok := response.(proto.Message)
	if ok {
		msgSize := proto.Size(msg)
		r.mid.serverSendSizeHistogram.WithLabelValues(string(r.rpcType), r.serviceName, r.methodName, r.mid.serviceName).Observe(float64(msgSize))
	} else {
		lmsg, ok := response.(legacyproto.Message)
		if ok {
			msgSize := legacyproto.Size(lmsg)
			r.mid.serverSendSizeHistogram.WithLabelValues(string(r.rpcType), r.serviceName, r.methodName, r.mid.serviceName).Observe(float64(msgSize))
		}
	}
}

func (r *serverReporter) Handled(code codes.Code) {
	r.mid.serverHandledCounter.WithLabelValues(string(r.rpcType), r.serviceName, r.methodName, code.String(), r.mid.serviceName).Inc()
	r.mid.serverHandledHistogram.WithLabelValues(string(r.rpcType), r.serviceName, r.methodName, r.mid.serviceName).Observe(time.Since(r.startTime).Seconds())

}

// preRegisterMethod is invoked on Register of a Server, allowing all gRPC m_services labels to be pre-populated.
// func preRegisterMethod(serviceName string, mInfo *grpc.MethodInfo, mid *Middleware) {
// 	methodName := mInfo.Name
// 	methodType := string(typeFromMethodInfo(mInfo))
// 	// These are just references (no increments), as just referencing will create the labels but not set values.
// 	mid.serverStartedCounter.GetMetricWithLabelValues(methodType, serviceName, methodName)
// 	mid.serverStreamMsgReceived.GetMetricWithLabelValues(methodType, serviceName, methodName)
// 	mid.serverStreamMsgSent.GetMetricWithLabelValues(methodType, serviceName, methodName)
// 	mid.serverHandledHistogram.GetMetricWithLabelValues(methodType, serviceName, methodName)
// 	for _, code := range allCodes {
// 		mid.serverHandledCounter.GetMetricWithLabelValues(methodType, serviceName, methodName, code.String())
// 	}
// }

// func typeFromMethodInfo(mInfo *grpc.MethodInfo) grpcType {
// 	if mInfo.IsClientStream == false && mInfo.IsServerStream == false {
// 		return Unary
// 	}
// 	if mInfo.IsClientStream == true && mInfo.IsServerStream == false {
// 		return ClientStream
// 	}
// 	if mInfo.IsClientStream == false && mInfo.IsServerStream == true {
// 		return ServerStream
// 	}
// 	return BidiStream
// }

func registerOrGet(r prom.Registerer, c prom.Collector) prom.Collector {
	if err := r.Register(c); err != nil {
		if are, ok := err.(prom.AlreadyRegisteredError); ok {
			return are.ExistingCollector
		}
		panic(err)
	}
	return c
}
