package grpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"math"
	"net"
	"sync"
	"time"

	"github.com/oldjon/gx/common"

	"github.com/google/uuid"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/oldjon/gx/modules"
	grpcprom "github.com/oldjon/gx/modules/grpc/prometheus"
	grpcrecover "github.com/oldjon/gx/modules/grpc/recover"
	"github.com/oldjon/gx/service"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/reflection"
)

const (
	defaultCloseTimeout    = 10 * time.Second
	defaultShutdownTimeout = 10 * time.Second
)

type ModuleConfig struct {
	modules.ModuleConfig `mapstructure:",squash"`

	CloseTimeout int64 `mapstructure:"close_timeout"`
}

// nolint: revive
//  nolint explain, the GRPCServer class is used for a long time , could not change that name
// implement service.ModuleServer Serve method to run an internal loop
// implement service.ModuleCloser Close method to release resource or connection while app exit
type GRPCServer interface {
	Register(*grpc.Server)
}

// passing parameter should be service.ScopedHost type, but now for compatible reason , it is service.Host now
type ServerProviderFunc func(driver service.ModuleDriver) (GRPCServer, error)

func New(spf ServerProviderFunc) service.ModuleProvider {
	return service.ModuleProviderFromFunc("grpc", func(driver service.ModuleDriver) (service.Module, error) {
		return newGRPCModule(driver, spf)
	})
}

type module struct {
	driver         service.ModuleDriver
	spf            ServerProviderFunc
	moduleConfig   ModuleConfig
	server         *grpc.Server
	processOptions []service.ProcessOption

	unaryServerMiddlewares []UnaryServerMiddleware
	prom                   *grpcprom.Middleware
	recovery               *grpcrecover.Middleware

	handler GRPCServer
	hh      healthHandler
}

func (m *module) GetHost() service.Host {
	return m.driver.Host()
}

func (m *module) GetModuleHandler() interface{} {
	return m.handler
}

func newGRPCModule(driver service.ModuleDriver, spf ServerProviderFunc) (service.Module, error) {
	m := &module{
		driver: driver,
		spf:    spf,
	}

	// todo, find a way to pass this option from builder
	m.unaryServerMiddlewares = DefaultOptions.UnaryServerMiddlewares

	return m, nil
}

func (m *module) PreServe(ctx context.Context) error {
	_ = ctx
	host := m.driver.Host()
	grpcServer, err := m.spf(m.driver)
	if err != nil {
		return fmt.Errorf("failed to create grpc handler for %s:%s, %w", host.Name(), m.driver.ModuleName(), err)
	}

	m.handler = grpcServer

	return nil
}

func (m *module) unmarshalConfig() error {
	err := m.driver.ModuleConfig().Unmarshal(&m.moduleConfig)

	if err != nil {
		return fmt.Errorf("failed to Unmarshal module config, %w", err)
	}

	// check if handler implement ModuleMetaDataConfiger
	mc, ok := m.handler.(service.ModuleMetaDataConfig)
	if ok {
		metadataStruct := mc.NewModuleMetadata()
		err = m.driver.ModuleConfig().UnmarshalKey("metadata", metadataStruct)
		if err != nil {
			return fmt.Errorf("failed to Unmarshal metadata, %w", err)
		}

		m.moduleConfig.ModuleConfig.Metadata = metadataStruct
	}

	return nil
}

func (m *module) Serve(ctx context.Context) error {
	var err error
	logger := m.driver.Logger()

	chanBufferSize := 16 + len(m.processOptions)*2
	errorc := make(chan error, chanBufferSize)

	err = m.unmarshalConfig()
	if err != nil {
		return fmt.Errorf("failed to call unmarshalConfig, %w", err)
	}

	logger.Info("grpc module config", zap.Any("module_config", m.moduleConfig))

	// build up grpc options
	mid := grpcprom.New(grpcprom.Options{
		ServiceName: m.driver.ModuleName(),
		Registerer:  m.driver.Metrics(),
		ErrorToCode: errorToCode,
	})
	m.prom = mid

	recovery := grpcrecover.New(grpcrecover.Options{
		ServiceName: m.driver.ModuleName(),
		Registerer:  m.driver.Metrics(),
	})
	m.recovery = recovery

	isOpenLogAddUUID := common.IsGrpcLogAddUUID()
	decider := func(ctx context.Context, fullMethodName string, servingObject interface{}) bool {
		if !isOpenLogAddUUID {
			return logger.Core().Enabled(zap.DebugLevel)
		}

		grpc_ctxtags.Extract(ctx).Set("uuid", uuid.NewString())
		return logger.Core().Enabled(zap.InfoLevel)
	}

	interceptors := m.createServerUnaryInterceptors(m.unaryServerMiddlewares, isOpenLogAddUUID, logger)
	streamInterceptors := []grpc.StreamServerInterceptor{
		grpc_zap.StreamServerInterceptor(logger, grpc_zap.WithCodes(errorToCode)),
		grpc_zap.PayloadStreamServerInterceptor(logger, decider),
		mid.StreamServerInterceptor(),
		// grpc_ot.StreamServerInterceptor(grpc_ot.WithTracer(m.host.Tracer())),
	}

	if common.IsGrpcLogAddUUID() {
		interceptors = append([]grpc.UnaryServerInterceptor{grpc_ctxtags.UnaryServerInterceptor()}, interceptors...)
		streamInterceptors = append([]grpc.StreamServerInterceptor{grpc_ctxtags.StreamServerInterceptor()}, streamInterceptors...)
	}

	unaryInterceptor := grpc_middleware.ChainUnaryServer(interceptors...)

	recoveryInterceptor := recovery.StreamServerInterceptor(logger)
	if recoveryInterceptor != nil {
		streamInterceptors = append(streamInterceptors, recoveryInterceptor)
	}

	streamInterceptor := grpc_middleware.ChainStreamServer(streamInterceptors...)

	opts := []grpc.ServerOption{
		grpc.UnaryInterceptor(unaryInterceptor),
		grpc.StreamInterceptor(streamInterceptor),
		// The default number of concurrent streams/requests on a bot connection
		// is 100, while the server is unlimited. The bot setting can only be
		// controlled by adjusting the server value. Set a very large value for the
		// server value so that we have no fixed limit on the number of concurrent
		// streams/requests on either the bot or server.
		grpc.MaxConcurrentStreams(math.MaxInt32),
	}

	// tls support for
	if m.moduleConfig.EnableTLS {
		// compatible with old config
		clientCAFile := m.moduleConfig.ClientCAFile
		if clientCAFile == "" && m.moduleConfig.CAFile != "" {
			logger.Warn("ca_file is used in config file, please use client_ca_file instead")
			clientCAFile = m.moduleConfig.CAFile
		}

		var creds grpc.ServerOption
		creds, err = m.createCreds(m.moduleConfig.CertFile, m.moduleConfig.KeyFile, m.moduleConfig.EnableClientAuth, clientCAFile)
		if err != nil {
			return errors.WithStack(err)
		}

		opts = append(opts, creds)
	}
	m.server = grpc.NewServer(opts...)

	listenAddr := m.moduleConfig.ListenAddress
	logger.Info("listening tcp", zap.String("addr", listenAddr))
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return errors.WithStack(err)
	}

	if common.IsGRPCServerHealthProtocolEnabled() {
		// register healthproto
		m.hh.RegisterServer(m.server)
	}

	// register grpc handler
	m.handler.Register(m.server)

	// reflection
	reflection.Register(m.server)

	ctx, cancel := context.WithCancel(ctx)

	var running sync.WaitGroup

	moduleServer, ok := m.handler.(service.ModuleServer)
	if ok {
		running.Add(1)
		go func() {
			logger.Info("enter module serve goroutine")
			defer func() {
				running.Done()
				logger.Info("exit module serve goroutine")
			}()
			if err := moduleServer.Serve(ctx); err != nil {
				logger.Error("module grpc serve failed", zap.Error(err))
				errorc <- err
			}
		}()
	}

	running.Add(1)
	go func() {
		logger.Info("enter grpc goroutine")
		defer func() {
			running.Done()
			logger.Info("exit grpc goroutine")
		}()
		if err := m.server.Serve(listener); err != nil {
			logger.Error("grpc server failed", zap.Error(err))
			errorc <- err
		}
	}()

	// register module
	registerAddr := listenAddr
	if m.moduleConfig.RegisterAddress != "" {
		registerAddr = m.moduleConfig.RegisterAddress
	}
	registerPath := m.driver.ModuleName()
	cr, ok := m.handler.(service.ModuleCustomizedRegister)
	if ok {
		registerPath = cr.ModuleRegisterName()
	}
	if err := m.driver.Host().RegisterModule(registerPath, registerAddr, m.moduleConfig.Metadata); err != nil {
		logger.Error("register module failed", zap.Error(err))
		errorc <- err
	}

	select {
	case <-ctx.Done():
		logger.Info("grpc context done")
		err = ctx.Err()
	case e := <-errorc:
		logger.Info("grpc serving failed", zap.Error(err))
		err = e
	}
	cancel()
	m.shutdown()
	m.closeServer(m.handler)

	logger.Info("waiting grpc goroutine exit")
	running.Wait()
	return err
}

func (m *module) createCreds(certFile string, keyFile string, enableClientAuth bool, clientCAFile string) (grpc.ServerOption, error) {
	logger := m.driver.Logger().With(
		zap.String("cert_file", certFile),
		zap.String("key_file", keyFile),
		zap.Bool("enable_client_auth", enableClientAuth),
		zap.String("client_ca_file", clientCAFile),
	)

	certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to LoadX509KeyPair certFile[%s] keyFile[%s]", certFile, keyFile)
	}

	tlsConfig := tls.Config{
		Certificates: []tls.Certificate{certificate},
	}

	if enableClientAuth {
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert

		certPool := x509.NewCertPool()
		ca, err := ioutil.ReadFile(clientCAFile)
		if err != nil {
			return nil, fmt.Errorf("cound not read certificate file for bot auth: %w", err)
		}

		if ok := certPool.AppendCertsFromPEM(ca); !ok {
			return nil, fmt.Errorf("failed to append ca certs")
		}

		tlsConfig.ClientCAs = certPool
	}

	creds := credentials.NewTLS(&tlsConfig)

	logger.Info("created grpc tls creds")
	return grpc.Creds(creds), nil
}

func (m *module) closeServer(server GRPCServer) {
	logger := m.driver.Logger()
	logger.Info("closing server")
	defer logger.Info("closed server")

	closer, ok := server.(service.ModuleCloser)
	if !ok {
		return
	}

	var closeTimeout time.Duration
	if m.moduleConfig.CloseTimeout == 0 {
		closeTimeout = defaultCloseTimeout
	} else {
		closeTimeout = time.Duration(m.moduleConfig.CloseTimeout) * time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
	defer cancel()

	err := closer.Close(ctx)
	if err != nil {
		logger.Warn("failed to close server", zap.Error(err))
	}
}

func (m *module) shutdown() {
	logger := m.driver.Logger()
	logger.Info("shutting down grpc server")

	done := make(chan struct{})
	go func() {
		m.server.GracefulStop()
		close(done)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
	select {
	case <-ctx.Done():
		logger.Error("graceful shutdown grpc timeout, force shutting down",
			zap.Duration("timeout", defaultShutdownTimeout))
		m.server.Stop()
	case <-done:
	}

	logger.Info("shutted down grpc server")
	cancel()
}

func (m *module) SetProcessOptions(options []service.ProcessOption) {
	m.processOptions = options
}
