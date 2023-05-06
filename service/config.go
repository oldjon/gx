package service

import (
	"github.com/oldjon/gx/common/buildinfo"
	"github.com/spf13/pflag"
)

type serviceConfig struct {
	Name        string
	Description string
	Owner       string
	Roles       []string
	// Tags        map[string]string
}

type tlsConfig struct {
	CAFile    string `mapstructure:"ca_file"`
	EnableTLS bool   `mapstructure:"enable_tls"`
}

type sessionConfig struct {
	TTL int
}

type loggingConfig struct {
	Env         string
	Path        string
	MaxSize     uint64 `mapstructure:"max_size"`
	Cron        string
	Level       string
	Stderr      bool // output to stderr at same time
	Stdout      bool // output to stdout at same time
	ErrorMetric bool `mapstructure:"error_metric"`
	Unique      bool
	LocalTime   bool `mapstructure:"local_time"`
}

type internalServerConfig struct {
	ListenAddr string `mapstructure:"listen_addr"`
	AuthClient bool   `mapstructure:"auth_client"`
	CertFile   string `mapstructure:"cert_file"`
	KeyFile    string `mapstructure:"key_file"`
}

func bindFlags(flagSet *pflag.FlagSet) {
	flagSet.StringSlice("roles", []string{}, "roles to enable")
	flagSet.String("config-path", "", "config path to locate config files")
}

type eventLoggingChannelConfig struct {
	EnableChannel bool   `mapstructure:"enable"`
	ChannelType   string `mapstructure:"type"` // support file
	ChannelPath   string `mapstructure:"path"`
}

type eventLoggingConfig struct {
	Path      string
	MaxSize   uint64 `mapstructure:"max_size"`
	Cron      string
	MetaData  map[string]string
	Unique    bool
	Channels  []eventLoggingChannelConfig `mapstructure:"channels"`
	LocalTime bool                        `mapstructure:"local_time"`
}

type internalVariables struct {
	variables map[string]string
}

func newInternalVariables() *internalVariables {
	i := &internalVariables{
		variables: make(map[string]string),
	}

	i.variables["build_info.code_version"] = buildinfo.GetCodeVersion()
	i.variables["build_info.res_version"] = buildinfo.GetResVersion()
	i.variables["build_info.date_time"] = buildinfo.GetDateTime()
	i.variables["build_info.go_version"] = buildinfo.GetGoVersion()

	return i
}

func (i *internalVariables) GetString(key string) string {
	v, ok := i.variables[key]
	if ok {
		return v
	}

	return ""
}

func (i *internalVariables) IsSet(key string) bool {
	_, ok := i.variables[key]

	return ok
}
