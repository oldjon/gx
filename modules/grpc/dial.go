package grpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"time"

	etcd "github.com/coreos/etcd/clientv3"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/oldjon/gx/modules/grpc/naming"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var errDialerRequireEtcd = errors.New("dialer require etcd bot to dial")

type Dialer struct {
	EtcdClient *etcd.Client
	Logger     *zap.Logger
	// Tracer     opentracing.Tracer

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

// Deprecated: use DialService instead.
func (d Dialer) Dial(target string, selector naming.Selector, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return d.DialContext(context.Background(), target, selector, opts...)
}

// Deprecated: use DialService instead.
func (d Dialer) DialContext(ctx context.Context, target string, selector naming.Selector, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	if d.EtcdClient == nil {
		return nil, errDialerRequireEtcd
	}

	var dialopts []grpc.DialOption

	// tls options
	if d.EnableTLS {
		tlsopt, err := d.tlsDialOption()
		if err != nil {
			return nil, err
		}
		dialopts = append(dialopts, tlsopt)
	} else {
		dialopts = append(dialopts, grpc.WithInsecure())
	}

	// interceptor options
	interceptoropts, err := d.interceptorDialOptions()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	dialopts = append(dialopts, interceptoropts...)

	resolver := &naming.EtcdResolver{
		Client:   d.EtcdClient,
		Selector: selector,
	}
	balancer := grpc.RoundRobin(resolver)
	dialopts = append(dialopts, grpc.WithBalancer(balancer))

	// override using bot provided opts
	dialopts = append(dialopts, opts...)
	conn, err := grpc.DialContext(ctx, target, dialopts...)
	fmt.Println(target, dialopts, err)
	return conn, err
}

func (d Dialer) tlsDialOption() (grpc.DialOption, error) {
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

func (d Dialer) interceptorDialOptions() ([]grpc.DialOption, error) {
	logger := d.Logger

	// decide whether to log grpc payload
	decider := func(ctx context.Context, fullMethodName string) bool {
		return logger.Core().Enabled(zap.DebugLevel)
	}

	callopts := []grpc_retry.CallOption{
		grpc_retry.WithMax(3),
		grpc_retry.WithBackoff(grpc_retry.BackoffLinearWithJitter(500*time.Millisecond, 0.2)),
	}

	unaryClientMiddlewares := d.UnaryClientMiddlewares
	if unaryClientMiddlewares == nil {
		unaryClientMiddlewares = DefaultOptions.UnaryClientMiddlewares
	}

	dialOptions := []grpc.DialOption{
		grpc.WithUnaryInterceptor(
			grpc_middleware.ChainUnaryClient(
				d.createClientUnaryInterceptors(unaryClientMiddlewares, logger)...,
			),
		),
		grpc.WithStreamInterceptor(
			grpc_middleware.ChainStreamClient(
				grpc_zap.StreamClientInterceptor(logger),
				grpc_zap.PayloadStreamClientInterceptor(logger, decider),
				grpc_retry.StreamClientInterceptor(callopts...),
				// grpc_ot.StreamClientInterceptor(grpc_ot.WithTracer(d.Tracer)),
			),
		),
	}

	return dialOptions, nil
}
