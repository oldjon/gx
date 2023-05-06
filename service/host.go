package service

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"

	grpcMiddleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpcZap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	"github.com/oldjon/gutil/env"
	"github.com/oldjon/gx/common"
	"github.com/oldjon/gx/modules/grpc/resolver"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/robfig/cron"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	etcdNS "go.etcd.io/etcd/client/v3/namespace"
	etcdYaml "go.etcd.io/etcd/client/v3/yaml"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	grpcResolver "google.golang.org/grpc/resolver"
	"gopkg.in/natefinch/lumberjack.v2"
)

const (
	defaultConfigPath = "./config"
)

type Host interface {
	Name() string
	Logger() *zap.Logger
	EventLogger() *EventLogger
	ModuleConfig() env.ModuleConfig
	Serve() error
	RegisterModule(moduleName string, addr string, metaData interface{}) error
	GetConfigPath() string
	Metrics() prometheus.Registerer
	EtcdSession() *concurrency.Session
	KVManager() *KVManager
}

type Option func(h *host)

func WithArgs(args []string) Option {
	return func(h *host) {
		h.args = args
	}
}

func WithLogger(logger *zap.Logger) Option {
	return func(h *host) {
		h.logger = logger
	}
}

type host struct {
	args       []string
	configPath string

	cron *cron.Cron

	viper           *viper.Viper
	configVariables map[string]env.VarReader

	moduleConfig env.ModuleConfig

	logLevel          zap.AtomicLevel
	logger            *zap.Logger
	loggerOnExit      func() error
	eventLogger       *EventLogger
	eventLoggerOnExit func() error

	registry *prometheus.Registry

	serviceConfig serviceConfig
	tlsConfig     tlsConfig

	modules            []*moduleDriver
	moduleCloseCh      chan struct{}
	moduleStartCounter int
	moduleStopCounter  int

	internalServerConfig *internalServerConfig
	internalServer       *http.Server

	signalCh          chan os.Signal
	errorCh           chan error
	internalRunningWG sync.WaitGroup

	instanceID string

	etcdClient  *etcd.Client
	etcdSession *concurrency.Session
	kvManager   *KVManager
}

func newHost(b Builder) (Host, error) {
	h := &host{
		args:          os.Args,
		cron:          cron.New(),
		modules:       make([]*moduleDriver, 0, 16),
		moduleCloseCh: make(chan struct{}, 128),
		viper:         viper.New(),
		registry:      prometheus.NewRegistry(),
		signalCh:      make(chan os.Signal),
		errorCh:       make(chan error, 128),
		instanceID:    strconv.FormatUint(randomID(), 36),
	}
	for _, o := range b.options {
		o(h)
	}

	if err := h.setupConfig(); err != nil {
		return nil, errors.WithStack(err)
	}
	if err := h.setupLogging(); err != nil {
		return nil, errors.WithStack(err)
	}
	// this should be as early as possible, other subsystem may dependent on it
	if err := h.setupTLS(); err != nil {
		return nil, errors.WithStack(err)
	}
	if err := h.setupInternalServer(); err != nil {
		return nil, errors.WithStack(err)
	}
	if err := h.setupEtcd(); err != nil {
		return nil, errors.WithStack(err)
	}
	if err := h.setupKeysManager(); err != nil {
		return nil, errors.WithStack(err)
	}

	h.logger.Info("adding modules")
	for _, m := range b.modules {
		if err := h.addModule(m); err != nil {
			return nil, errors.WithStack(err)
		}
	}
	h.logger.Info("created host",
		zap.Any("config_path", h.configPath),
		zap.Any("config", h.serviceConfig))

	return h, nil
}

func (h *host) Name() string {
	return h.serviceConfig.Name
}

func (h *host) Logger() *zap.Logger {
	return h.logger
}

func (h *host) Serve() error {
	h.logger.Info("starting serve modules")
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer func() {
		cancel()
		_ = h.cleanup()
	}()

	if err := h.prepare(ctx); err != nil {
		h.logger.Error("prepare for serve failed", zap.Error(err))
		return errors.WithStack(err)
	}

	moduleContext, moduleCancel := context.WithCancel(ctx)
	defer func() {
		moduleCancel()
	}()
	h.serveModule(moduleContext)

	var exitErr error
	looping := true
	for looping {
		select {
		case sig := <-h.signalCh:
			if exitErr == nil {
				exitErr = fmt.Errorf("receive signal: %s", sig.String())
			}
			h.logger.Error("receive signal", zap.String("signal", sig.String()))
			moduleCancel()
		case <-h.etcdSession.Done():
			if exitErr == nil {
				exitErr = fmt.Errorf("etcd etcdSession expired, %d", int64(h.etcdSession.Lease()))
			}
			h.logger.Error("etcd etcdSession expired", zap.Int64("lease_id", int64(h.etcdSession.Lease())))
			moduleCancel()
		case err := <-h.errorCh:
			if exitErr == nil {
				exitErr = fmt.Errorf("module serve error, %w", err)
			}
			h.logger.Error("some module failed")
			moduleCancel()

		case <-h.moduleCloseCh:
			h.moduleStopCounter++

			h.logger.Warn("some module exit",
				zap.Int("running_modules", h.moduleStartCounter-h.moduleStopCounter),
				zap.Int("total_modules", h.moduleStartCounter))

			if h.moduleStopCounter >= h.moduleStartCounter {
				h.logger.Warn("all module exited")

				// all modules are existed
				cancel()
			}

		case <-ctx.Done():
			h.logger.Warn("host context done")
			looping = false
		}
	}

	h.logger.Info("waiting internal server exit")
	h.internalRunningWG.Wait()
	h.logger.Info("finish internal server exit")

	return exitErr
}

func (h *host) RegisterModule(moduleName string, addr string, metaData interface{}) error { // TODO 用consul实现服务注册
	if h.EtcdSession() == nil {
		h.logger.Warn("skip RegisterModule because etcd etcdSession is nil", zap.String("module_name", moduleName))
		return nil
	}

	r := resolver.NewResolver(h.etcdClient)

	gAddr := grpcResolver.Address{
		Addr:     addr,
		Metadata: metaData,
	}

	err := r.Register(context.Background(), moduleName, gAddr, h.EtcdSession().Lease())
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (h *host) Metrics() prometheus.Registerer {
	return h.registry
}

func (h *host) GetConfigPath() string {
	return h.configPath
}

func (h *host) EventLogger() *EventLogger {
	return h.eventLogger
}

func (h *host) EtcdSession() *concurrency.Session {
	return h.etcdSession
}

func (h *host) KVManager() *KVManager {
	return h.kvManager
}

func (h *host) setupConfig() error {
	// Logger is not inited yet, do not use h.Logger in this function

	// bind flags
	flagSet := pflag.NewFlagSet(h.args[0], pflag.ExitOnError)
	bindFlags(flagSet)
	err := flagSet.Parse(h.args[1:])
	if err != nil {
		return errors.WithStack(err)
	}

	err = h.viper.BindPFlags(flagSet)
	if err != nil {
		return errors.WithStack(err)
	}

	// set config path from command line if provided
	configPath := h.viper.GetString("config-path")
	if configPath == "" {
		configPath = defaultConfigPath
	}
	absConfigPath, err := filepath.Abs(configPath)
	if err != nil {
		return errors.WithStack(err)
	}
	h.configPath = absConfigPath

	h.viper.SetConfigName("config")
	h.viper.AddConfigPath(h.configPath)
	if err := h.viper.ReadInConfig(); err != nil {
		return errors.WithStack(err)
	}

	if err := h.viper.Unmarshal(&h.serviceConfig); err != nil {
		return errors.WithStack(err)
	}

	// load variables
	variables := make(map[string]env.VarReader)
	if common.IsConfigVariablesEnabled() {
		// create secret viper to read variables file
		files := common.GetConfigVariablesFiles()
		for _, file := range files {
			if _, find := variables[file]; !find {
				variablesViper := viper.New()

				variablesViper.SetConfigName(file)
				variablesViper.AddConfigPath(h.configPath)

				err = variablesViper.ReadInConfig()
				if err != nil {
					h.logger.Error("failed to load variables file", zap.String("file", file), zap.Error(err))
				} else {
					variables[file] = variablesViper
				}
			}
		}
	}

	if common.IsInternalConfigVariablesEnabled() {
		// create from fx internal variables
		variables["gxinternal"] = newInternalVariables()
	}

	h.configVariables = variables

	return nil
}

func (h *host) setupLogging() error {
	var loggingCfg loggingConfig
	if err := h.viper.UnmarshalKey("logging", &loggingCfg); err != nil {
		return errors.WithStack(err)
	}

	var opts []zap.Option
	var encoder zapcore.Encoder
	switch loggingCfg.Env {
	case "development":
		level, err := strToLevel(loggingCfg.Level, zapcore.DebugLevel)
		if err != nil {
			return err
		}
		h.logLevel = zap.NewAtomicLevelAt(level)
		opts = []zap.Option{
			zap.Development(),
			zap.AddCaller(),
			zap.AddStacktrace(zap.WarnLevel),
		}
		encoder = zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig())
	case "production":
		level, err := strToLevel(loggingCfg.Level, zapcore.InfoLevel)
		if err != nil {
			return err
		}
		h.logLevel = zap.NewAtomicLevelAt(level)
		opts = []zap.Option{
			zap.AddCaller(),
			zap.AddStacktrace(zap.ErrorLevel),
		}
		encoder = zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
	default:
		return fmt.Errorf("unsupported env %s", loggingCfg.Env)
	}

	absPath, err := filepath.Abs(loggingCfg.Path)
	if err != nil {
		return errors.WithStack(err)
	}

	if loggingCfg.Unique {
		absPath = joinLogPathWithInstanceID(absPath, h.instanceID)
	}

	rotateWriter := &lumberjack.Logger{
		Filename:  absPath,
		MaxSize:   int(loggingCfg.MaxSize), // MB
		LocalTime: loggingCfg.LocalTime,
	}
	writer := zapcore.AddSync(rotateWriter)
	if loggingCfg.Stderr {
		writer = zap.CombineWriteSyncers(writer, os.Stderr)
	}
	if loggingCfg.Stdout {
		writer = zap.CombineWriteSyncers(writer, os.Stdout)
	}

	wCore := zapcore.NewCore(encoder, writer, h.logLevel)
	core := zapcore.NewTee(wCore)

	h.logger = zap.New(core, opts...).With(zap.String("instance_id", h.instanceID))

	// rotate log file
	h.logger.Info("add log file rotate cron job", zap.String("cron", loggingCfg.Cron))
	err = h.cron.AddFunc(loggingCfg.Cron,
		func() {
			logger := h.logger.With(zap.String("path", absPath))
			logger.Info("rotating log file")
			if err := rotateWriter.Rotate(); err != nil {
				logger.Error("failed to rotate log file", zap.Error(err))
			}
			logger.Info("rotated log file")
		},
	)
	if err != nil {
		return errors.WithStack(err)
	}

	h.loggerOnExit = func() error {
		_ = h.logger.Sync()
		return rotateWriter.Rotate()
	}

	return nil
}

func (h *host) setupEventLogging() error {
	// if FX_DISABLE_EVENT_LOGGER is set to true, we turn off event logger support
	if !common.IsEventLoggerEnabled() {
		h.logger.Warn("event Logger create is skipped")
		return nil
	}

	var elConfig eventLoggingConfig
	cr := env.NewModuleConfig(h.viper, h.configVariables)
	if err := cr.UnmarshalKey("event_logging", &elConfig); err != nil {
		return errors.WithStack(err)
	}

	if len(elConfig.Channels) == 0 {
		// to compatible with file channel, we add file channel if channel list is empty
		elConfig.Channels = make([]eventLoggingChannelConfig, 1)
		elConfig.Channels[0] = eventLoggingChannelConfig{
			EnableChannel: true,
			ChannelType:   "file",
			ChannelPath:   elConfig.Path,
		}
	}

	elLoggers := make(map[eventLoggerChannel]*zap.Logger, len(elConfig.Channels))
	rotateWriters := make([]*lumberjack.Logger, len(elConfig.Channels))
	for i, elChannelConfig := range elConfig.Channels {
		elChannel, err := h.setupEventLoggingChannel(elConfig, elChannelConfig)
		if err != nil {
			return fmt.Errorf("failed to setup el channel: %w", err)
		}

		elLoggers[elChannel.Type] = elChannel.Logger
		rotateWriters[i] = elChannel.RotateWriter
	}

	h.eventLogger = &EventLogger{
		metadata: elConfig.MetaData,
		loggers:  elLoggers,
	}

	h.eventLoggerOnExit = func() error {
		_ = h.eventLogger.Sync()
		for _, rotateWriter := range rotateWriters {
			_ = rotateWriter.Rotate()
		}
		return nil
	}

	return nil
}

type eventLoggingChannel struct {
	Type         eventLoggerChannel
	Logger       *zap.Logger
	RotateWriter *lumberjack.Logger
}

func (h *host) setupEventLoggingChannel(elConfig eventLoggingConfig, channelConfig eventLoggingChannelConfig) (*eventLoggingChannel, error) {
	absPath, err := filepath.Abs(channelConfig.ChannelPath)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if elConfig.Unique {
		absPath = joinLogPathWithInstanceID(absPath, h.instanceID)
	}

	rotateWriter := &lumberjack.Logger{
		Filename:  absPath,
		MaxSize:   int(elConfig.MaxSize), // MB
		LocalTime: elConfig.LocalTime,
	}

	var elType eventLoggerChannel
	var encoder zapcore.Encoder

	switch channelConfig.ChannelType {
	case "file":
		elType = eventLoggerChannelFile
		encoderConfig := zapcore.EncoderConfig{
			TimeKey:    "ts",
			MessageKey: "event",
			LineEnding: zapcore.DefaultLineEnding,
			EncodeTime: zapcore.EpochTimeEncoder,
		}
		encoder = zapcore.NewJSONEncoder(encoderConfig)
	default:
		return nil, fmt.Errorf("do not support el channel: %s", channelConfig.ChannelType)
	}

	core := zapcore.NewCore(encoder, zapcore.AddSync(rotateWriter), zap.NewAtomicLevelAt(zap.InfoLevel))

	logger := zap.New(core)

	// rotate event log file
	h.logger.Info("add event log file rotate cron job", zap.String("cron", elConfig.Cron), zap.String("elChannel", channelConfig.ChannelType))
	err = h.cron.AddFunc(elConfig.Cron, func() {
		l := h.logger.With(zap.String("path", absPath))
		l.Info("rotating event log file")
		if err := rotateWriter.Rotate(); err != nil {
			l.Error("failed to rotate event log file", zap.Error(err))
		}
		l.Info("rotated event log file")
	})
	if err != nil {
		return nil, fmt.Errorf("failed to add rotate of elChannel[%s] to cron job, %w", absPath, err)
	}

	return &eventLoggingChannel{
		Type:         elType,
		Logger:       logger,
		RotateWriter: rotateWriter,
	}, nil
}

func (h *host) setupEtcd() error {
	if !common.IsETCDEnabled() {
		h.logger.Warn("etcd bot is disabled, skipping setup")
		return nil
	}

	h.logger.Info("creating etcd bot")
	etcdCfg, err := etcdYaml.NewConfig(path.Join(h.configPath, "etcd.yaml"))
	if err != nil {
		return errors.WithStack(err)
	}

	logAll := func(ctx context.Context, fullMethodName string) bool {
		return true
	}

	etcdCfg.DialOptions = []grpc.DialOption{
		grpc.WithUnaryInterceptor(
			grpcMiddleware.ChainUnaryClient(
				grpcZap.UnaryClientInterceptor(h.logger),
				grpcZap.PayloadUnaryClientInterceptor(h.logger, logAll),
			),
		),
		grpc.WithStreamInterceptor(
			grpcMiddleware.ChainStreamClient(
				grpcZap.StreamClientInterceptor(h.logger),
				grpcZap.PayloadStreamClientInterceptor(h.logger, logAll),
			),
		),
	}
	client, err := etcd.New(*etcdCfg)
	if err != nil {
		return errors.WithStack(err)
	}

	prefix := "/" + h.Name() + "/"
	h.logger.Info("etcd prefix is " + prefix)
	client.KV = etcdNS.NewKV(client.KV, prefix)
	client.Lease = etcdNS.NewLease(client.Lease, prefix)
	client.Watcher = etcdNS.NewWatcher(client.Watcher, prefix)

	h.etcdClient = client
	h.logger.Info("created etcd bot")
	return nil
}

func (h *host) setupKeysManager() error {
	if !common.IsETCDEnabled() {
		h.logger.Warn("etcd bot is disabled, skipping setup key manager")
		return nil
	}

	h.kvManager = &KVManager{
		client: h.etcdClient,
		logger: h.logger,
	}
	return nil
}

func (h *host) setupTLS() error {
	h.logger.Info("setup tls start")
	var tlsConfig tlsConfig
	if err := h.viper.UnmarshalKey("tls", &tlsConfig); err != nil {
		return errors.WithStack(err)
	}
	h.tlsConfig = tlsConfig
	h.logger.Info("setup tls", zap.Any("tls_config", tlsConfig))
	return nil
}

// convert logLevel string to zapCore.Level, use defaultLevel if logLevel is empty
func strToLevel(level string, defaultLevel zapcore.Level) (zapcore.Level, error) {
	if len(level) != 0 {
		if err := defaultLevel.UnmarshalText([]byte(level)); err != nil {
			return zapcore.FatalLevel, fmt.Errorf("unsupported logging logLevel %s", level)
		}
	}
	return defaultLevel, nil
}

func joinLogPathWithInstanceID(path string, instanceID string) string {
	if strings.HasSuffix(path, ".log") {
		index := len(path) - 4
		path = path[:index]
	}
	path = path + "-" + instanceID + ".log"
	return path
}

func (h *host) setupInternalServer() error {
	var isCfg internalServerConfig
	if err := h.viper.UnmarshalKey("internal_server", &isCfg); err != nil {
		return errors.WithStack(err)
	}

	h.registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{
		PidFn:        func() (int, error) { return os.Getpid(), nil },
		Namespace:    "",
		ReportErrors: false,
	}))

	mux := http.NewServeMux()
	metricHandler := promhttp.HandlerFor(h.registry, promhttp.HandlerOpts{})

	// metric
	mux.Handle("/metrics", metricHandler)

	// instance id
	mux.HandleFunc("/instance_id", func(rw http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(rw, h.instanceID)
	})

	// debug
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	// logging level
	mux.Handle("/logging/level", h.logLevel)

	h.internalServerConfig = &isCfg
	h.internalServer = &http.Server{
		Addr:    isCfg.ListenAddr,
		Handler: mux,
	}

	// setup bot auth
	if h.tlsConfig.EnableTLS && isCfg.AuthClient {
		ca, err := os.ReadFile(h.tlsConfig.CAFile)
		if err != nil {
			return fmt.Errorf("count not read certificate file: %s", err)
		}

		certPool := x509.NewCertPool()
		if ok := certPool.AppendCertsFromPEM(ca); !ok {
			return fmt.Errorf("failed to append ca certs")
		}

		tlsConfig := &tls.Config{
			ClientAuth: tls.RequireAndVerifyClientCert,
			ClientCAs:  certPool,
		}

		h.internalServer.TLSConfig = tlsConfig
	}
	return nil
}

func (h *host) ModuleConfig() env.ModuleConfig {
	return h.moduleConfig
}

func (h *host) addModule(mi moduleInfo) error {
	module, supported, err := newModuleDriver(h, mi, h.tlsConfig, "")
	if err != nil {
		return errors.WithStack(err)
	}

	if !supported {
		return nil
	}

	logger := h.logger.With(
		zap.String("service", module.options.serviceName),
		zap.String("module", module.options.moduleName),
		zap.Strings("roles", module.options.roles),
	)

	logger.Info("add module",
		zap.String("service", module.options.serviceName),
		zap.String("module", module.options.moduleName),
		zap.Strings("host_roles", h.serviceConfig.Roles),
	)
	h.modules = append(h.modules, module)

	moduleCfg := module.moduleConfig
	if moduleCfg.IsSet("forks") { // fork 子module
		var forks map[string]env.ModuleConfig
		if err := moduleCfg.UnmarshalKey("forks", &forks); err != nil {
			return errors.WithStack(err)
		}
		for forkName := range forks {
			forkedModule, forkSupported, err := newModuleDriver(h, mi, h.tlsConfig, forkName)
			if err != nil {
				return errors.WithStack(err)
			}
			if !forkSupported {
				continue
			}
			logger := h.logger.With(
				zap.String("service", module.options.serviceName),
				zap.String("module", module.options.moduleName),
				zap.Strings("roles", module.options.roles),
				zap.String("fork_name", forkName),
			)

			logger.Info("add fork module",
				zap.String("service", module.options.serviceName),
				zap.String("module", module.options.moduleName),
				zap.Strings("host_roles", h.serviceConfig.Roles),
				zap.String("fork_name", forkName),
			)
			h.modules = append(h.modules, forkedModule)
		}
	}

	return nil
}

func (h *host) serveModule(ctx context.Context) {
	for _, module := range h.modules {
		h.moduleStartCounter++
		go func(driver *moduleDriver) {
			logger := h.logger.With(
				zap.String("service", driver.options.serviceName),
				zap.String("driver", driver.options.moduleName),
			)

			logger.Info("enter serving driver goroutine")
			defer func() {
				if r := recover(); r != nil {
					err := fmt.Errorf("%v", r)
					logger.Error("driver recover panic", zap.Error(err))
					nonblockingError(h.errorCh, err)
				}

				// send close to ch
				h.moduleCloseCh <- struct{}{}

				logger.Info("exit serving driver goroutine")
			}()

			err := driver.preServe(ctx)
			if err != nil {
				logger.Error("driver pre serve error", zap.Error(err))
				nonblockingError(h.errorCh, err)
			}

			err = driver.serve(ctx)
			if err != nil {
				logger.Error("driver serving error", zap.Error(err))
				nonblockingError(h.errorCh, err)
			}
		}(module)
	}
}

func (h *host) prepare(ctx context.Context) error {
	// etcd etcdSession
	if common.IsETCDEnabled() && common.IsETCDSessionEnabled() {
		h.logger.Info("creating etcd etcdSession")
		var sessionConfig sessionConfig
		if err := h.viper.UnmarshalKey("etcdSession", &sessionConfig); err != nil {
			return errors.WithStack(err)
		}

		ttl := sessionConfig.TTL
		session, err := concurrency.NewSession(h.etcdClient, concurrency.WithTTL(ttl))
		if err != nil {
			h.logger.Error("created etcd etcdSession failed", zap.Error(err))
			return errors.WithStack(err)
		}
		h.etcdSession = session
		h.logger.Info("created etcd etcdSession",
			zap.Int64("lease_id", int64(h.etcdSession.Lease())),
			zap.Int("session_ttl", ttl))
	} else {
		h.logger.Warn("creating etcd etcdSession is skipped")
	}

	// internal server
	h.serveInternalServer(ctx)

	// start cron
	h.logger.Info("starting cron")
	h.cron.Start()
	h.logger.Info("started cron")

	h.registerSignalHandlers()

	return nil
}

func (h *host) serveInternalServer(ctx context.Context) {
	h.internalRunningWG.Add(1)
	go func() {
		h.logger.Info("enter internal server goroutine")
		defer func() {
			h.internalRunningWG.Done()
			h.logger.Info("exit internal server goroutine")
		}()

		errorCh := make(chan error)
		go func() {
			h.logger.Info("enter internal server http goroutine")
			defer func() {
				h.logger.Info("exit internal server http goroutine")
			}()

			if h.etcdClient != nil && h.etcdSession != nil {
				// register on etcd
				serverKey := InternalServerPathPrefix + h.internalServer.Addr
				_, err := h.etcdClient.Put(context.Background(), serverKey, h.internalServer.Addr, etcd.WithLease(h.EtcdSession().Lease()))
				if err != nil {
					h.logger.Error("internal server serving register to etcd failed", zap.Error(err))
					errorCh <- err
				}
			} else {
				h.logger.Warn("skipping internal server serving register ")
			}

			certFile := h.internalServerConfig.CertFile
			keyFile := h.internalServerConfig.KeyFile
			h.logger.Info("internal server serving",
				zap.Any("config", h.internalServerConfig),
				zap.Any("tls_config", h.tlsConfig))
			if h.tlsConfig.EnableTLS {
				if err := h.internalServer.ListenAndServeTLS(certFile, keyFile); err != nil {
					h.logger.Error("internal server serving https failed", zap.Error(err))
					errorCh <- err
				}
			} else {
				if err := h.internalServer.ListenAndServe(); err != nil {
					h.logger.Error("internal server serving http failed", zap.Error(err))
					errorCh <- err
				}
			}
		}()

		select {
		case <-ctx.Done():
			h.logger.Info("internal server serving context done")
			if err := h.internalServer.Shutdown(context.Background()); err != nil {
				h.logger.Error("internal server failed to shutdown", zap.Error(err))
			}

		case err := <-errorCh:
			h.logger.Error("pass internal server serving error to host", zap.Error(err))
			nonblockingError(h.errorCh, err)
		}
	}()
}

func (h *host) registerSignalHandlers() {
	h.logger.Info("registering signal handler")
	signal.Notify(h.signalCh,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGABRT,
		syscall.SIGQUIT,
	)
}

func (h *host) cleanup() error {
	h.logger.Info("cleaning up host")

	h.logger.Info("closing tracers")
	h.logger.Info("closed tracers")

	h.logger.Info("stopping cron")
	h.cron.Stop()
	h.logger.Info("stopped cron")

	if h.etcdSession != nil {
		h.logger.Info("closing etcd etcdSession")
		_ = h.etcdSession.Close()
		h.etcdSession = nil
	}
	if h.eventLogger != nil {
		h.logger.Info("exit event Logger")
		if err := h.eventLoggerOnExit(); err != nil {
			h.logger.Error("event Logger on exit failed", zap.Error(err))
		}
		h.eventLogger = nil
	}
	if h.logger != nil {
		h.logger.Info("exit Logger")
		if err := h.loggerOnExit(); err != nil {
			// Logger is existing, try to save the error to stdout
			fmt.Printf("error happen during loggerOnExit, %s", err)
		}

		h.logger = nil
	}
	return nil
}

func nonblockingError(errorCh chan<- error, err error) {
	select {
	case errorCh <- err:
	default:
	}
}
