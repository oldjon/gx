package service

import (
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
