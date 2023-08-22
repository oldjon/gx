package service

// etcd prefix used internal by host
const (
	keysPrefix               = "__keys/"
	snowflakePrefix          = "__snowflake/"
	snowflakeLockPrefix      = "__snowflake_lock/"
	InternalServerPathPrefix = "__internal/"
)

const (
	// snowflake type
	SnowflakeType_Default = 0
	SnowflakeType_53      = 1
)
