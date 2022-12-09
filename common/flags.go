package common

import (
	"strings"

	"github.com/spf13/viper"
)

const (
	defaultConfigSecretsFileName = "secrets" // don't include extension
)

func init() {
	viper.SetDefault("GX_ENABLE_EVENT_LOGGER_FLOAT_FIX", false)
	viper.SetDefault("GX_DISABLE_ETCD", false)
	viper.SetDefault("GX_DISABLE_EVENT_LOGGER", false)
	viper.SetDefault("GX_ENABLE_HTTP_RICH_TRACE", false)
	viper.SetDefault("GX_DISABLE_ETCD_SESSION", false)
	viper.SetDefault("GX_ENABLE_GRPC_SERVER_PANIC_RECOVER", false)
	viper.SetDefault("GX_ENABLE_GRPC_SERVER_PANIC_RECOVER_STACKTRACE", true)
	viper.SetDefault("GX_ENABLE_GRPC_SERVER_HEALTH_PROTOCOL", false)
	viper.SetDefault("GX_ENABLE_CONFIG_VARIABLES", false)
	viper.SetDefault("GX_CONFIG_VARIABLES_FILES", "")
	viper.SetDefault("GX_ENABLE_GRPC_LOGGER_ADD_UUID", false)
	viper.SetDefault("GX_ENABLE_UNIFIED_HTTP_METRICS_NAME", false)
	viper.SetDefault("GX_HTTP_TRACE_DISABLED_PATHS", "")
}

func IsETCDEnabled() bool {
	return !viper.GetBool("GX_DISABLE_ETCD")
}

func IsEventLoggerFloatFixEnabled() bool {
	return viper.GetBool("GX_ENABLE_EVENT_LOGGER_FLOAT_FIX")
}

func IsEventLoggerEnabled() bool {
	return !viper.GetBool("GX_DISABLE_EVENT_LOGGER")
}

func IsHTTPRichTraceEnabled() bool {
	return viper.GetBool("GX_ENABLE_HTTP_RICH_TRACE")
}

func IsETCDSessionEnabled() bool {
	return !viper.GetBool("GX_DISABLE_ETCD_SESSION")
}

func IsConfigVariablesEnabled() bool {
	return viper.GetBool("GX_ENABLE_CONFIG_VARIABLES")
}

func GetConfigVariablesFiles() []string {
	filesStr := viper.GetString("GX_CONFIG_VARIABLES_FILES")

	if filesStr == "" {
		filesStr = defaultConfigSecretsFileName
	}

	files := strings.Split(filesStr, ",")
	return files
}

func IsGRPCServerRecoverEnabled() bool {
	return viper.GetBool("GX_ENABLE_GRPC_SERVER_PANIC_RECOVER")
}

func IsGRPCServerRecoverStackTraceEnabled() bool {
	return viper.GetBool("GX_ENABLE_GRPC_SERVER_PANIC_RECOVER_STACKTRACE")
}

func IsGRPCServerHealthProtocolEnabled() bool {
	return viper.GetBool("GX_ENABLE_GRPC_SERVER_HEALTH_PROTOCOL")
}

func IsGrpcLogAddUUID() bool {
	return viper.GetBool("GX_ENABLE_GRPC_LOGGER_ADD_UUID")
}

func UsingUnifiedMetricsName() bool {
	return viper.GetBool("GX_ENABLE_UNIFIED_HTTP_METRICS_NAME")
}

func GetHTTPTraceDisabledPaths() []string {
	pathsStr := viper.GetString("GX_HTTP_TRACE_DISABLED_PATHS")

	paths := strings.Split(pathsStr, ",")
	return paths
}
