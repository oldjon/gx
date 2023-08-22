package http

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/NYTimes/gziphandler"
	"github.com/oldjon/gx/common"
	"github.com/oldjon/gx/modules"
	"github.com/oldjon/gx/modules/http/opentracing"
	"github.com/oldjon/gx/service"
	"go.uber.org/zap"
)

const (
	defaultCloseTimeout    = 10 * time.Second
	defaultShutdownTimeout = 10 * time.Second
)

type errorHandler func(w http.ResponseWriter, r *http.Request, err string)

type ModuleConfig struct {
	modules.ModuleConfig `mapstructure:",squash"`

	ReadTimeout int64 `mapstructure:"read_timeout"`

	WriteTimeout int64 `mapstructure:"write_timeout"`

	IdleTimeout int64 `mapstructure:"idle_timeout"`

	HandlerTimeout int64 `mapstructure:"handler_timeout"`

	DisableGzipMiddleware bool `mapstructure:"disable_gzip_middleware"`
}

type GXHttpHandler interface {
	http.Handler
}

type GXHttpHandlerWithPaths interface {
	GXHttpHandler

	GetPaths() []string
}

type HandlerProviderFunc func(driver service.ModuleDriver) (GXHttpHandler, error)

func New(hpf HandlerProviderFunc) service.ModuleProvider {
	return service.ModuleProviderFromFunc("http", func(driver service.ModuleDriver) (service.Module, error) {
		return newHttpModule(driver, hpf)
	})
}

// implement service.ModuleServer Serve method to run an internal loop
// implement service.ModuleCloser Close method to release resources or connection while app exit
type module struct {
	driver         service.ModuleDriver
	hpf            HandlerProviderFunc
	moduleConfig   ModuleConfig
	server         *http.Server
	processOptions []service.ProcessOption

	handler GXHttpHandler
}

func (m *module) GetHost() service.Host {
	return m.driver.Host()
}

func (m *module) GetModuleHandler() interface{} {
	return m.handler
}

func newHttpModule(driver service.ModuleDriver, hpf HandlerProviderFunc) (*module, error) {
	return &module{
		driver: driver,
		hpf:    hpf,
	}, nil
}

func (m *module) PreServe(_ context.Context) error {
	handler, err := m.hpf(m.driver)
	if err != nil {
		return fmt.Errorf("failed to create http handler for %s:%s, %w", m.driver.Host().Name(), m.driver.ModuleName(), err)
	}
	m.handler = handler

	return nil
}

func (m *module) unmarshalConfig() error {
	err := m.driver.ModuleConfig().Unmarshal(&m.moduleConfig)

	if err != nil {
		return fmt.Errorf("failed to Unmarshal module config, %w", err)
	}

	// // check if handler implement ModuleMetaDataConfiger
	// mc, ok := m.handler.(service.ModuleMetaDataConfiger)
	// if ok {
	// 	metadataStruct := mc.NewModuleMetadata()
	// 	err = m.driver.ModuleConfig().UnmarshalKey("metadata", metadataStruct)
	// 	if err != nil {
	// 		return fmt.Errorf("failed to Unmarshal metadata, %w", err)
	// 	}
	//
	// 	m.moduleConfig.ModuleConfig.Metadata = metadataStruct
	// }

	return nil
}

func (m *module) Serve(ctx context.Context) error {
	var err error
	logger := m.driver.Logger()

	chanBufferSize := 16 + len(m.processOptions)*2
	errorCh := make(chan error, chanBufferSize)

	err = m.unmarshalConfig()
	if err != nil {
		return fmt.Errorf("failed to call unmarshalConfig, %w", err)
	}
	logger.Info("http module config", zap.Any("module_config", m.moduleConfig))

	var paths []string
	if h, ok := m.handler.(GXHttpHandlerWithPaths); ok {
		paths = h.GetPaths()
	}
	logger.Info("http module paths", zap.Any("paths", paths))
	pm := NewMetrics(MetricsOptions{
		HostName:   m.driver.Host().Name(),
		ModuleName: m.driver.ModuleName(),
		Paths:      paths,
		Registerer: m.driver.Host().Metrics(),
	})
	tm := opentracing.New(opentracing.Options{
		Tracer: m.driver.Tracer(),
	})
	tom := NewTimeOut(TimeOutOptions{
		Timeout: time.Duration(m.moduleConfig.HandlerTimeout) * time.Second,
	})

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
				logger.Error("module http serve failed", zap.Error(err))
				errorCh <- err
			}
		}()
	}

	// middlewares are executed from inside to outside
	// timeout middleware should be executed first
	newHandler := tom.Handler(m.handler)
	if !m.moduleConfig.DisableGzipMiddleware {
		newHandler = gziphandler.GzipHandler(newHandler)
	}
	newHandler = pm.Handler(tm.Handler(newHandler))

	m.server = &http.Server{
		Addr:         m.moduleConfig.ListenAddress,
		ReadTimeout:  time.Duration(m.moduleConfig.ReadTimeout) * time.Second,
		WriteTimeout: time.Duration(m.moduleConfig.WriteTimeout) * time.Second,
		IdleTimeout:  time.Duration(m.moduleConfig.IdleTimeout) * time.Second,
		Handler:      newHandler,
	}

	running.Add(1)
	go func() {
		logger.Info("enter http goroutine")
		defer func() {
			running.Done()
			logger.Info("exist http goroutine")
		}()

		logger.Info("http server serving", zap.Any("config", m.moduleConfig))
		if m.moduleConfig.CertFile != "" && m.moduleConfig.KeyFile != "" {
			logger.Info("starting serve http tls")
			if err := m.server.ListenAndServeTLS(m.moduleConfig.CertFile, m.moduleConfig.KeyFile); err != nil {
				logger.Error("http server tls failed", zap.Error(err))
				errorCh <- err
			}
		} else {
			logger.Info("starting serve http")
			if err := m.server.ListenAndServe(); err != nil {
				logger.Error("http server failed", zap.Error(err))
				errorCh <- err
			}
		}
	}()

	if common.IsETCDEnabled() {
		// register module
		registerAddr := m.moduleConfig.ListenAddress
		if m.moduleConfig.RegisterAddress != "" {
			registerAddr = m.moduleConfig.RegisterAddress
		}
		registerPath := m.driver.ModuleName()
		if err := m.driver.Host().RegisterModule(registerPath, registerAddr, m.moduleConfig.Metadata); err != nil {
			logger.Error("register module failed", zap.Error(err))
			errorCh <- err
		}
	} else {
		// http register is not mandatory , so we can skip it
		logger.Warn("register module to etcd is skipped because etcd is disabled")
	}

	select {
	case <-ctx.Done():
		logger.Info("http context done")
		err = ctx.Err()
	case e := <-errorCh:
		logger.Info("http serving failed", zap.Error(err))
		err = e
	}
	cancel()
	m.shutdown()
	m.closeServer(m.handler)

	logger.Info("waiting http exit.")
	running.Wait()
	return err
}

func (m *module) closeServer(server GXHttpHandler) {
	logger := m.driver.Logger()
	logger.Info("closing server")
	defer logger.Info("closed server")

	closer, ok := server.(service.ModuleCloser)
	if !ok {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultCloseTimeout)
	defer cancel()

	err := closer.Close(ctx)
	if err != nil {
		logger.Warn("failed to close server", zap.Error(err))
	}

}

func (m *module) shutdown() {
	logger := m.driver.Logger()
	logger.Info("shutting down http server")

	ctx, cancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
	if err := m.server.Shutdown(ctx); err != nil {
		logger.Error("shutdown http failed", zap.Error(err))
	}

	logger.Info("shut down http server")
	cancel()
}

func (m *module) Close(_ context.Context) error { return nil }

func (m *module) SetProcessOptions(options []service.ProcessOption) {
	m.processOptions = options
}
