package tcp

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/oldjon/gx/common"
	"github.com/oldjon/gx/modules"
	"github.com/oldjon/gx/service"
	"go.uber.org/zap"
)

type ModuleConfig struct {
	modules.ModuleConfig `mapstructure:",squash"`

	// ReadTimeout int64 `mapstructure:"read_timeout"`
	// WriteTimeout int64 `mapstructure:"write_timeout"`
	// IdleTimeout int64 `mapstructure:"idle_timeout"`
	// HandlerTimeout int64 `mapstructure:"handler_timeout"`
	// DisableGzipMiddleware bool `mapstructure:"disable_gzip_middleware"`
}

type GXTCPHandler interface {
	HandleConn(context.Context, *net.TCPConn)
}

type HandlerProviderFunc func(driver service.ModuleDriver) (GXTCPHandler, error)

func New(hpf HandlerProviderFunc) service.ModuleProvider {
	return service.ModuleProviderFromFunc("tcp", func(driver service.ModuleDriver) (service.Module, error) {
		return newTCPModule(driver, hpf)
	})
}

type module struct {
	driver       service.ModuleDriver
	hpf          HandlerProviderFunc
	moduleConfig ModuleConfig
	listener     *net.TCPListener
	logger       *zap.Logger

	handler GXTCPHandler
}

func (m *module) GetHost() service.Host {
	return m.driver.Host()
}

func (m *module) GetModuleHandler() interface{} {
	return m.handler
}

func newTCPModule(driver service.ModuleDriver, hpf HandlerProviderFunc) (*module, error) {
	m := &module{
		driver: driver,
		hpf:    hpf,
		logger: driver.Logger(),
	}
	return m, nil
}

func (m *module) unmarshalConfig() error {
	err := m.driver.ModuleConfig().Unmarshal(&m.moduleConfig)

	if err != nil {
		return fmt.Errorf("failed to Unmarshal module config, %w", err)
	}

	return nil
}

func (m *module) bind(address string) error {

	tcpAddr, err := net.ResolveTCPAddr("tcp4", address)
	if err != nil {
		m.logger.Error("tcp parse listen addr failed ", zap.String("addr", address))
		return err
	}

	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		m.logger.Error("tcp listen failed ", zap.String("addr", address))
		return err
	}

	m.listener = listener
	return nil
}

func (m *module) accept() (*net.TCPConn, error) {

	m.listener.SetDeadline(time.Now().Add(time.Second * 1))

	conn, err := m.listener.AcceptTCP()
	if err != nil {
		return nil, err
	}

	conn.SetKeepAlive(true)
	conn.SetKeepAlivePeriod(time.Minute)
	conn.SetNoDelay(true)
	conn.SetWriteBuffer(128 * 1024)
	conn.SetReadBuffer(128 * 1024)

	return conn, nil
}

func (m *module) bindAndAccept(ctx context.Context, address string, handler GXTCPHandler) error {
	err := m.bind(address)
	if err != nil {
		return err
	}
	go func() {
		for {
			conn, err := m.accept()
			if err != nil {
				continue
			}
			handler.HandleConn(ctx, conn)
		}
	}()
	return nil
}

func (m *module) CLose() error {
	return m.listener.Close()
}

func (m *module) Serve(ctx context.Context) error {
	var err error
	logger := m.driver.Logger()

	err = m.unmarshalConfig()
	if err != nil {
		return fmt.Errorf("failed to call unmarshalConfig, %w", err)
	}
	logger.Info("http module config", zap.Any("module_config", m.moduleConfig))

	// pm := NewMetrics(MetricsOptions{
	// 	HostName:   m.driver.Host().Name(),
	// 	ModuleName: m.driver.ModuleName(),
	// 	Registerer: m.driver.Host().Metrics(),
	// })
	// tom := NewTimeOut(TimeOutOptions{
	// 	Timeout: time.Duration(m.moduleConfig.HandlerTimeout) * time.Second,
	// })
	errorCh := make(chan error, 1)
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

	// start listen and accept
	running.Add(1)
	go func() {
		logger.Info("enter module serve goroutine")
		defer func() {
			running.Done()
			logger.Info("exit module serve goroutine")
		}()
		err = m.bindAndAccept(ctx, m.moduleConfig.ListenAddress, m.handler)
		if err != nil {
			errorCh <- err
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
	logger.Info("waiting http exit.")
	running.Wait()
	return err
}

func (m *module) PreServe(_ context.Context) error {
	handler, err := m.hpf(m.driver)
	if err != nil {
		return fmt.Errorf("failed to create tcp handler for %s:%s, %w", m.driver.Host().Name(), m.driver.ModuleName(), err)
	}
	m.handler = handler
	return nil
}
