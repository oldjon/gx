package service

import (
	"context"
	"fmt"
	"strings"

	"github.com/oldjon/gutil/env"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

// ModuleProvider create Module from Host
// bot provide ModuleProvider other than Module so Module can be created lazily
type ModuleProvider interface {
	DefaultName() string
	Create(ModuleDriver) (Module, error)
}

func ModuleProviderFromFunc(name string, cf func(ModuleDriver) (Module, error)) ModuleProvider {
	return &moduleProvider{name, cf}
}

type moduleProvider struct {
	name string
	cf   func(ModuleDriver) (Module, error)
}

func (m *moduleProvider) DefaultName() string                        { return m.name }
func (m *moduleProvider) Create(driver ModuleDriver) (Module, error) { return m.cf(driver) }

type ModuleServer interface {
	Serve(context.Context) error
}

type ModuleCloser interface {
	Close(context.Context) error
}

type ModuleMetaDataConfig interface {
	// NewModuleMetadata to create a module config's metadata structure, will be used to unmarshal from config.yaml
	NewModuleMetadata() interface{}
}

type ModuleCustomizedRegister interface {
	// ModuleRegisterName return a string could be used to register path like "{hostName}/{ModuleRegisterName}" used in service discovery
	//  if module don't provide this interface, it will use "{hostName}/{moduleName}" instead
	ModuleRegisterName() string
}

type Module interface {
	ModuleServer

	PreServe(ctx context.Context) error
}

type HostModule interface {
	// GetHost return the host belong to
	GetHost() Host

	// GetModuleHandler return the module handler created during build
	GetModuleHandler() interface{}
}

type ModuleOption func(*moduleOptions)

type moduleOptions struct {
	serviceName    string
	moduleName     string
	roles          []string
	processOptions []ProcessOption
}

func WithName(name string) ModuleOption {
	return func(o *moduleOptions) {
		o.serviceName = name
	}
}

func WithModuleName(name string) ModuleOption {
	return func(o *moduleOptions) {
		o.moduleName = name
	}
}

func WithRole(role string) ModuleOption {
	return func(o *moduleOptions) {
		if role == "" {
			return
		}
		for _, r := range o.roles {
			if r == role {
				return
			}
		}

		o.roles = append(o.roles, role)
	}
}

type ModuleDriver interface {
	Host() Host
	ModuleName() string
	ModuleConfig() env.ModuleConfig
	Logger() *zap.Logger
	Metrics() prometheus.Registerer
}

type moduleDriver struct {
	host Host

	module        Module
	options       moduleOptions
	tlsConfig     tlsConfig
	moduleConfig  env.ModuleConfig
	preServeHooks []PreServeHook
}

func (md *moduleDriver) Host() Host {
	return md.host
}

func (md *moduleDriver) ModuleName() string {
	return md.options.moduleName
}

func (md *moduleDriver) ModuleConfig() env.ModuleConfig {
	return md.moduleConfig
}

func (md *moduleDriver) Logger() *zap.Logger {
	return md.host.Logger()
}

func (md *moduleDriver) Metrics() prometheus.Registerer {
	return md.host.Metrics()
}

// @return
// 1. moduleDriver pointer of module instance
// 2. bool if this host supports this module by roles
// 3. error
func newModuleDriver(host *host, mi moduleInfo, tlsConfig tlsConfig, forkName string) (*moduleDriver, bool, error) {
	mos := moduleOptions{
		serviceName:    host.Name(),
		moduleName:     mi.provider.DefaultName(),
		roles:          []string{},
		processOptions: []ProcessOption{},
	}
	for _, o := range mi.options {
		o(&mos)
	}

	// check if host supports roles of this module
	if !Roles(host.serviceConfig.Roles).Support(mos.roles) {
		host.logger.Info("skip module due to roles",
			zap.Strings("host_roles", host.serviceConfig.Roles))
		return nil, false, nil
	}

	driver := &moduleDriver{
		host:          host,
		options:       mos,
		tlsConfig:     tlsConfig,
		preServeHooks: make([]PreServeHook, 0),
	}
	var err error
	driver.module, err = mi.provider.Create(driver)
	if err != nil {
		return nil, false, errors.WithStack(err)
	}

	// pass ProcessHandler information to module
	if mp, ok := driver.module.(ModuleProcesses); ok {
		mp.SetProcessOptions(mos.processOptions)
	}

	err = driver.initModuleConfig(host.viper, host.configVariables, forkName)
	if err != nil {
		return nil, false, errors.WithStack(err)
	}

	return driver, true, nil
}

func (md *moduleDriver) preServe(ctx context.Context) error {
	err := md.module.PreServe(ctx)
	if err != nil {
		return fmt.Errorf("failed to call modle PreServe, %w", err)
	}

	// run preServe hooks
	for i, hook := range md.preServeHooks {
		if hook == nil {
			continue
		}
		err := hook(ctx)
		if err != nil {
			return fmt.Errorf("failed to run hook[%d], %w", i, err)
		}
	}
	return nil
}

func (md *moduleDriver) serve(ctx context.Context) error {
	err := md.module.Serve(ctx)
	return errors.WithStack(err)
}

type PreServeHook func(ctx context.Context) error

func (md *moduleDriver) initModuleConfig(core *viper.Viper, configVariables map[string]env.VarReader, forkName string) error {
	var sub *viper.Viper

	// verify host config has submodule config
	moduleName := md.options.moduleName
	subModuleConfig := core.Sub("modules").Sub(moduleName)
	if subModuleConfig == nil {
		return fmt.Errorf("failed to load module config for [%s], maybe it is not configed in config file", moduleName)
	}

	if forkName == "" {
		sub = subModuleConfig
	} else {
		sub = subModuleConfig.Sub("forks").Sub(forkName)

		for _, key := range subModuleConfig.AllKeys() {
			if !strings.HasPrefix(key, "forks.") {
				if !sub.IsSet(key) {
					sub.Set(key, subModuleConfig.Get(key))
				}
			}
		}
	}

	if !sub.IsSet("enable_tls") {
		sub.Set("enable_tls", md.tlsConfig.EnableTLS)
	}

	if !sub.IsSet("ca_file") {
		sub.Set("ca_file", md.tlsConfig.CAFile)
	}

	moduleConfig := env.NewModuleConfig(sub, configVariables)
	md.moduleConfig = moduleConfig

	return nil
}
