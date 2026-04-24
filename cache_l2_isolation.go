package db

import "strings"

// RedisL2IsolationReport 描述 Redis 作为分页 L2 的隔离评估结果。
type RedisL2IsolationReport struct {
	Allowed bool
	Mode    string
	Reason  string
}

// EvaluateRedisL2Isolation 根据配置评估 Redis 是否满足分页 L2 隔离要求。
//
// 通过条件：
// 1) cluster 模式（视为具备分片隔离能力）；
// 2) standalone 下使用非默认 DB；
// 3) standalone 下使用非空 key namespace。
func EvaluateRedisL2Isolation(cfg *RedisConnectionConfig, keyNamespace string) RedisL2IsolationReport {
	if cfg == nil {
		return RedisL2IsolationReport{Allowed: false, Mode: "none", Reason: "redis config is nil"}
	}

	if cfg.ClusterMode && len(cfg.ClusterAddrs) > 0 {
		return RedisL2IsolationReport{Allowed: true, Mode: "cluster", Reason: "cluster mode enabled"}
	}

	namespace := strings.TrimSpace(keyNamespace)
	if cfg.DB > 0 {
		return RedisL2IsolationReport{Allowed: true, Mode: "standalone_db", Reason: "non-default redis db selected"}
	}
	if namespace != "" {
		return RedisL2IsolationReport{Allowed: true, Mode: "standalone_namespace", Reason: "key namespace configured"}
	}

	return RedisL2IsolationReport{
		Allowed: false,
		Mode:    "standalone_default",
		Reason:  "requires cluster mode, non-default db, or explicit key namespace",
	}
}
