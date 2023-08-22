package grpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	grpc_ot "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	"github.com/opentracing/opentracing-go"
	"io/ioutil"
	"time"

	grpcMiddleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpcZap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpcRetry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/oldjon/gx/modules/grpc/resolver"
	"github.com/pkg/errors"
	etcd "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	grpcResolver "google.golang.org/grpc/resolver"
)

// ServiceDialer is the interface to dial to service
type ServiceDialer interface {
	DialService(ctx context.Context, serviceName string, opts ...grpc.DialOption) (*grpc.ClientConn, error)
	DialWithSelector(ctx context.Context, target string, selector resolver.Selector, opts ...grpc.DialOption) (*grpc.ClientConn, error)
}

var _ ServiceDialer = (*Dialer)(nil)

var errDialerRequireEtcd = errors.New("dialer require etcd bot to dial")

type Dialer struct {
	HostName   string
	EtcdClient *etcd.Client
	Logger     *zap.Logger
	Tracer     opentracing.Tracer

	UnaryClientMiddlewares []UnaryClientMiddleware

	// EnableTLS will turn on tls support while connecting to grpc server
	EnableTLS bool

	// CAFile is root cert file path
	//  that clients use when verifying server certificates.
	//  if set to empty, will use host's root CA set
	CAFile string

	// ServerName is used to determine grpc server name is same in certification
	//  used with EnableTLS
	//  when empty, will use name in certification to compare with
	ServerName string

	// EnableClientAuth enable tls bot authentication support at bot side
	//  ref to https://blog.cloudflare.com/introducing-tls-client-auth/
	EnableClientAuth bool

	// CertFile used with EnableClientAuth for tls bot authentication support
	CertFile string

	// KeyFile used with EnableClientAuth for tls bot authentication support
	KeyFile string
}

func (d *Dialer) tlsDialOption() (grpc.DialOption, error) {
	tlsConfig := tls.Config{
		// InsecureSkipVerify: true,
		ServerName: d.ServerName,
	}

	if d.CAFile != "" {
		certPool := x509.NewCertPool()
		ca, err := ioutil.ReadFile(d.CAFile)
		if err != nil {
			return nil, fmt.Errorf("count not read ca certificate: %s", err)
		}

		if ok := certPool.AppendCertsFromPEM(ca); !ok {
			return nil, fmt.Errorf("cound not append ca certificate")
		}

		tlsConfig.RootCAs = certPool
	}

	if d.EnableClientAuth {
		certificate, err := tls.LoadX509KeyPair(d.CertFile, d.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("cound not load bot key pair: %s", err)
		}

		tlsConfig.Certificates = []tls.Certificate{certificate}
	}

	creds := credentials.NewTLS(&tlsConfig)

	return grpc.WithTransportCredentials(creds), nil
}

func (d *Dialer) interceptorDialOptions() ([]grpc.DialOption, error) {
	logger := d.Logger

	// decide whether to log grpc payload
	decider := func(ctx context.Context, fullMethodName string) bool {
		return logger.Core().Enabled(zap.DebugLevel)
	}

	callOpts := []grpcRetry.CallOption{
		grpcRetry.WithMax(3),
		grpcRetry.WithBackoff(grpcRetry.BackoffLinearWithJitter(500*time.Millisecond, 0.2)),
	}

	unaryClientMiddlewares := d.UnaryClientMiddlewares
	if unaryClientMiddlewares == nil {
		unaryClientMiddlewares = DefaultOptions.UnaryClientMiddlewares
	}

	dialOptions := []grpc.DialOption{
		grpc.WithUnaryInterceptor(
			grpcMiddleware.ChainUnaryClient(
				d.createClientUnaryInterceptors(unaryClientMiddlewares, logger)...,
			),
		),
		grpc.WithStreamInterceptor(
			grpcMiddleware.ChainStreamClient(
				grpcZap.StreamClientInterceptor(logger),
				grpcZap.PayloadStreamClientInterceptor(logger, decider),
				grpcRetry.StreamClientInterceptor(callOpts...),
				grpc_ot.StreamClientInterceptor(grpc_ot.WithTracer(d.Tracer)),
			),
		),
	}

	return dialOptions, nil
}

type DialerOptions struct {
	EtcdClient *etcd.Client
	Logger     *zap.Logger
	Tracer     opentracing.Tracer

	UnaryClientMiddlewares []UnaryClientMiddleware

	// EnableTLS will turn on tls support while connecting to grpc server
	EnableTLS bool

	// CAFile is root cert file path
	//  that clients use when verifying server certificates.
	//  if set to empty, will use host's root CA set
	CAFile string

	// ServerName is used to determine grpc server name is same in certification
	//  used with EnableTLS
	//  when empty, will use name in certification to compare with
	ServerName string

	// EnableClientAuth enable tls bot authentication support at bot side
	//  ref to https://blog.cloudflare.com/introducing-tls-client-auth/
	EnableClientAuth bool

	// CertFile used with EnableClientAuth for tls bot authentication support
	CertFile string

	// KeyFile used with EnableClientAuth for tls bot authentication support
	KeyFile string
}

func NewDialer(opts DialerOptions) (*Dialer, error) {
	// nolint gosimple
	d := Dialer{
		EtcdClient: opts.EtcdClient,
		Logger:     opts.Logger,
		Tracer:     opts.Tracer,

		UnaryClientMiddlewares: opts.UnaryClientMiddlewares,

		EnableTLS:        opts.EnableTLS,
		CAFile:           opts.CAFile,
		ServerName:       opts.ServerName,
		EnableClientAuth: opts.EnableClientAuth,
		CertFile:         opts.CertFile,
		KeyFile:          opts.KeyFile,
	}

	return &d, nil
}

// DialService used to dial to service with specific ServiceDialerOptions
//
//	could provide metadata matcher in ServiceDialerOptions
//	 servicePath: "/" + hostName + "/" + moduleName
func (d *Dialer) DialService(ctx context.Context, servicePath string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return d.DialWithSelector(ctx, servicePath, nil, opts...)
}

// DialWithSelector is core method to create grpc.ClientConn
//
//	it is maybe changed in the future, don't rely on this method
func (d *Dialer) DialWithSelector(ctx context.Context, target string, selector resolver.Selector, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	if d.EtcdClient == nil {
		return nil, errDialerRequireEtcd
	}

	var dialOpts []grpc.DialOption

	// tls options
	if d.EnableTLS {
		tlsopt, err := d.tlsDialOption()
		if err != nil {
			return nil, err
		}
		dialOpts = append(dialOpts, tlsopt)
	} else {
		dialOpts = append(dialOpts, grpc.WithInsecure())
	}

	// interceptor options
	interceptorOpts, err := d.interceptorDialOptions()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	dialOpts = append(dialOpts, interceptorOpts...)

	rBuilder, _ := resolver.NewBuilder(d.EtcdClient, selector)

	grpcResolver.Register(rBuilder)

	dialOpts = append(dialOpts,
		grpc.WithResolvers(rBuilder),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig":[{"round_robin":{}}]}`))

	dialOpts = append(dialOpts, opts...)

	conn, err := grpc.DialContext(ctx, rBuilder.Scheme()+"://"+d.HostName+"/"+target, dialOpts...)
	return conn, err
}
