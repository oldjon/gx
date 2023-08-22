package common

import (
	"strings"

	"github.com/oldjon/gutil"
	"github.com/spf13/viper"
)

const (
	defaultConfigSecretsFileName = "secrets" // don't include extension
	SnowflakeMaxClusterBits      = 3
)

const (
	GX_DISABLE_ETCD                                = "GX_DISABLE_ETCD"
	GX_DISABLE_EVENT_LOGGER                        = "GX_DISABLE_EVENT_LOGGER"
	GX_ENABLE_HTTP_RICH_TRACE                      = "GX_ENABLE_HTTP_RICH_TRACE"
	GX_DISABLE_ETCD_SESSION                        = "GX_DISABLE_ETCD_SESSION"
	GX_ENABLE_GRPC_SERVER_PANIC_RECOVER            = "GX_ENABLE_GRPC_SERVER_PANIC_RECOVER"
	GX_ENABLE_GRPC_SERVER_PANIC_RECOVER_STACKTRACE = "GX_ENABLE_GRPC_SERVER_PANIC_RECOVER_STACKTRACE"
	GX_ENABLE_GRPC_SERVER_HEALTH_PROTOCOL          = "GX_ENABLE_GRPC_SERVER_HEALTH_PROTOCOL"
	GX_ENABLE_CONFIG_VARIABLES                     = "GX_ENABLE_CONFIG_VARIABLES"
	GX_CONFIG_VARIABLES_FILES                      = "GX_CONFIG_VARIABLES_FILES"
	GX_ENABLE_GRPC_LOGGER_ADD_UUID                 = "GX_ENABLE_GRPC_LOGGER_ADD_UUID"
	GX_ENABLE_UNIFIED_HTTP_METRICS_NAME            = "GX_ENABLE_UNIFIED_HTTP_METRICS_NAME"
	GX_HTTP_TRACE_DISABLED_PATHS                   = "GX_HTTP_TRACE_DISABLED_PATHS"
	GX_ENABLE_INTERNAL_CONFIG_VARIABLES            = "GX_ENABLE_INTERNAL_CONFIG_VARIABLES"
	GX_SNOWFLAKE_CLUSTER_BITS                      = "GX_SNOWFLAKE_CLUSTER_BITS"
	GX_SNOWFLAKE_CLUSTER_ID                        = "GX_SNOWFLAKE_CLUSTER_ID"
)

func init() {
	viper.SetDefault(GX_DISABLE_ETCD, false)
	viper.SetDefault(GX_DISABLE_EVENT_LOGGER, false)
	viper.SetDefault(GX_ENABLE_HTTP_RICH_TRACE, false)
	viper.SetDefault(GX_DISABLE_ETCD_SESSION, false)
	viper.SetDefault(GX_ENABLE_GRPC_SERVER_PANIC_RECOVER, true)
	viper.SetDefault(GX_ENABLE_GRPC_SERVER_PANIC_RECOVER_STACKTRACE, true)
	viper.SetDefault(GX_ENABLE_GRPC_SERVER_HEALTH_PROTOCOL, false)
	viper.SetDefault(GX_ENABLE_CONFIG_VARIABLES, false)
	viper.SetDefault(GX_CONFIG_VARIABLES_FILES, "")
	viper.SetDefault(GX_ENABLE_GRPC_LOGGER_ADD_UUID, false)
	viper.SetDefault(GX_ENABLE_UNIFIED_HTTP_METRICS_NAME, false)
	viper.SetDefault(GX_HTTP_TRACE_DISABLED_PATHS, "")
	viper.SetDefault(GX_ENABLE_INTERNAL_CONFIG_VARIABLES, false)
	viper.SetDefault(GX_SNOWFLAKE_CLUSTER_BITS, 0)
	viper.SetDefault(GX_SNOWFLAKE_CLUSTER_ID, 0)
}

func IsETCDEnabled() bool {
	return !viper.GetBool(GX_DISABLE_ETCD)
}

func IsEventLoggerEnabled() bool {
	return !viper.GetBool(GX_DISABLE_EVENT_LOGGER)
}

func IsHTTPRichTraceEnabled() bool {
	return viper.GetBool(GX_ENABLE_HTTP_RICH_TRACE)
}

func IsETCDSessionEnabled() bool {
	return !viper.GetBool(GX_DISABLE_ETCD_SESSION)
}

func IsConfigVariablesEnabled() bool {
	return viper.GetBool(GX_ENABLE_CONFIG_VARIABLES)
}

func GetConfigVariablesFiles() []string {
	filesStr := viper.GetString(GX_CONFIG_VARIABLES_FILES)

	if filesStr == "" {
		filesStr = defaultConfigSecretsFileName
	}

	files := strings.Split(filesStr, ",")
	return files
}

func IsGRPCServerRecoverEnabled() bool {
	return viper.GetBool(GX_ENABLE_GRPC_SERVER_PANIC_RECOVER)
}

func IsGRPCServerRecoverStackTraceEnabled() bool {
	return viper.GetBool(GX_ENABLE_GRPC_SERVER_PANIC_RECOVER_STACKTRACE)
}

func IsGRPCServerHealthProtocolEnabled() bool {
	return viper.GetBool(GX_ENABLE_GRPC_SERVER_HEALTH_PROTOCOL)
}

func IsGrpcLogAddUUID() bool {
	return viper.GetBool(GX_ENABLE_GRPC_LOGGER_ADD_UUID)
}

func SetGrpcLogAddUUID() {
	viper.Set(GX_ENABLE_GRPC_LOGGER_ADD_UUID, true)
}

func UsingUnifiedMetricsName() bool {
	return viper.GetBool(GX_ENABLE_UNIFIED_HTTP_METRICS_NAME)
}

func GetHTTPTraceDisabledPaths() []string {
	pathsStr := viper.GetString(GX_HTTP_TRACE_DISABLED_PATHS)

	paths := strings.Split(pathsStr, ",")
	return paths
}

func IsInternalConfigVariablesEnabled() bool {
	return viper.GetBool(GX_ENABLE_INTERNAL_CONFIG_VARIABLES)
}

func SetSnowflakeClusterBits(clusterBits uint64) {
	viper.Set(GX_SNOWFLAKE_CLUSTER_BITS, clusterBits)
}

func GetSnowflakeClusterBits() uint64 {
	bits := viper.GetUint64(GX_SNOWFLAKE_CLUSTER_BITS)
	return gutil.Min(bits, SnowflakeMaxClusterBits)
}

func SetSnowflakeClusterID(clusterId uint64) {
	viper.Set(GX_SNOWFLAKE_CLUSTER_ID, clusterId)
}

func GetSnowflakeClusterID() uint64 {
	return viper.GetUint64(GX_SNOWFLAKE_CLUSTER_ID)
}
