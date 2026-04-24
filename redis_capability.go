package db

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	redisFeatureStateSupported   = "supported"
	redisFeatureStateDegraded    = "degraded"
	redisFeatureStateUnavailable = "unavailable"

	redisFeatureSourceStaticContract = "static_contract"
	redisFeatureSourceRuntimeProbe   = "runtime_probe"
)

// RedisRuntimeInspector 适配器可选接口：用于输出 Redis 运行时能力摘要与支持矩阵。
type RedisRuntimeInspector interface {
	InspectRedisRuntime(ctx context.Context) (*RedisRuntimeCapabilityReport, error)
}

// RedisCapabilityModule Redis 模块信息。
type RedisCapabilityModule struct {
	Name    string `json:"name"`
	Version string `json:"version,omitempty"`
}

// RedisCapabilitySnapshot Redis 运行时能力快照。
type RedisCapabilitySnapshot struct {
	ServerVersion string                  `json:"server_version,omitempty"`
	Mode          string                  `json:"mode,omitempty"`
	Modules       []RedisCapabilityModule `json:"modules,omitempty"`
	Commands      []string                `json:"commands,omitempty"`
	MaxMemory     string                  `json:"maxmemory,omitempty"`
	EvictionPolicy string                 `json:"eviction_policy,omitempty"`
	ACLAvailable  bool                    `json:"acl_available,omitempty"`
	PingLatency   time.Duration           `json:"ping_latency,omitempty"`
}

// RedisFeatureStatus 描述单项能力状态。
type RedisFeatureStatus struct {
	State             string `json:"state"`
	Reason            string `json:"reason,omitempty"`
	MissingDependency string `json:"missing_dependency,omitempty"`
	FallbackAction    string `json:"fallback_action,omitempty"`
	Source            string `json:"source,omitempty"`
	Enforced          bool   `json:"enforced,omitempty"`
}

// RedisFeatureSupportMatrix Redis 功能支持矩阵。
type RedisFeatureSupportMatrix struct {
	AdapterRequired map[string]RedisFeatureStatus `json:"adapter_required,omitempty"`
	Core            map[string]RedisFeatureStatus `json:"core,omitempty"`
	Cluster         map[string]RedisFeatureStatus `json:"cluster,omitempty"`
	Stack           map[string]RedisFeatureStatus `json:"stack,omitempty"`
	Ops             map[string]RedisFeatureStatus `json:"ops,omitempty"`
}

// RedisRuntimeCapabilityReport Redis 运行时能力摘要。
type RedisRuntimeCapabilityReport struct {
	Snapshot             *RedisCapabilitySnapshot   `json:"snapshot,omitempty"`
	Matrix               *RedisFeatureSupportMatrix `json:"matrix,omitempty"`
	AllRequiredSupported bool                       `json:"all_required_supported"`
	Summary              string                     `json:"summary,omitempty"`
}

func (a *RedisAdapter) InspectRedisRuntime(ctx context.Context) (*RedisRuntimeCapabilityReport, error) {
	snapshot, err := a.GetCapabilitySnapshot(ctx)
	if err != nil {
		return nil, err
	}
	matrix := a.BuildFeatureSupportMatrix(snapshot)
	allRequiredSupported := true
	for _, feature := range matrix.AdapterRequired {
		if feature.Enforced && feature.State != redisFeatureStateSupported {
			allRequiredSupported = false
			break
		}
	}
	if allRequiredSupported {
		for _, feature := range matrix.Core {
			if feature.Enforced && feature.State != redisFeatureStateSupported {
				allRequiredSupported = false
				break
			}
		}
	}

	moduleNames := make([]string, 0, len(snapshot.Modules))
	for _, mod := range snapshot.Modules {
		moduleNames = append(moduleNames, mod.Name)
	}
	return &RedisRuntimeCapabilityReport{
		Snapshot:             snapshot,
		Matrix:               matrix,
		AllRequiredSupported: allRequiredSupported,
		Summary:              fmt.Sprintf("mode=%s modules=%s required_supported=%t", snapshot.Mode, strings.Join(moduleNames, ","), allRequiredSupported),
	}, nil
}

// GetCapabilitySnapshot 返回 Redis 运行时能力快照。
func (a *RedisAdapter) GetCapabilitySnapshot(ctx context.Context) (*RedisCapabilitySnapshot, error) {
	if a == nil || a.client == nil {
		return nil, fmt.Errorf("redis client not connected")
	}

	pingStart := time.Now()
	if err := a.client.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	snapshot := &RedisCapabilitySnapshot{
		Mode:        "standalone",
		Commands:    make([]string, 0),
		Modules:     make([]RedisCapabilityModule, 0),
		PingLatency: time.Since(pingStart),
	}

	if info, err := a.client.Info(ctx, "server", "memory").Result(); err == nil {
		for _, line := range strings.Split(info, "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "redis_version:") {
				snapshot.ServerVersion = strings.TrimSpace(strings.TrimPrefix(line, "redis_version:"))
			}
			if strings.HasPrefix(line, "maxmemory:") {
				snapshot.MaxMemory = strings.TrimSpace(strings.TrimPrefix(line, "maxmemory:"))
			}
			if strings.HasPrefix(line, "maxmemory_policy:") {
				snapshot.EvictionPolicy = strings.TrimSpace(strings.TrimPrefix(line, "maxmemory_policy:"))
			}
		}
	}

	if clusterInfo, err := a.client.Do(ctx, "CLUSTER", "INFO").Text(); err == nil {
		if strings.Contains(clusterInfo, "cluster_state:ok") || a.clusterMode {
			snapshot.Mode = "cluster"
		}
	}

	if aclList, err := a.client.Do(ctx, "ACL", "LIST").Result(); err == nil {
		_ = aclList
		snapshot.ACLAvailable = true
	}

	if moduleResult, err := a.client.Do(ctx, "MODULE", "LIST").Result(); err == nil {
		snapshot.Modules = parseRedisModuleList(moduleResult)
	}

	commandNames := []string{"GET", "SET", "DEL", "MGET", "MSET", "EXPIRE", "TTL", "JSON.GET", "JSON.SET", "FT.SEARCH", "FT.CREATE", "GRAPH.QUERY", "XADD", "ACL", "MODULE"}
	commandAvailability := probeRedisCommands(ctx, a.client, commandNames)
	for _, name := range commandNames {
		if commandAvailability[name] {
			snapshot.Commands = append(snapshot.Commands, strings.ToLower(name))
		}
	}
	sort.Strings(snapshot.Commands)

	return snapshot, nil
}

// BuildFeatureSupportMatrix 根据快照生成 Redis 支持矩阵。
func (a *RedisAdapter) BuildFeatureSupportMatrix(snapshot *RedisCapabilitySnapshot) *RedisFeatureSupportMatrix {
	hasModule := func(name string) bool {
		for _, mod := range snapshot.Modules {
			if strings.EqualFold(mod.Name, name) {
				return true
			}
		}
		return false
	}
	hasCommand := func(name string) bool {
		name = strings.ToLower(name)
		for _, cmd := range snapshot.Commands {
			if cmd == name {
				return true
			}
		}
		return false
	}

	providerAvailable := a.GetQueryBuilderProvider() != nil
	adapterRequired := map[string]RedisFeatureStatus{
		"connectivity":        supportedStatus("redis ping succeeded", redisFeatureSourceRuntimeProbe, true),
		"query_execution":     degradedStatus("native query available via compiled plan route; Adapter.Query remains SQL-oriented", "use REDIS_CMD:: / REDIS_PIPE:: through QueryConstructorAuto", redisFeatureSourceStaticContract, true),
		"transaction":         degradedStatus("MULTI/EXEC wrapper is limited to native Redis command plans", "use Begin + Exec(REDIS_CMD::...) + Commit or TxPipelined", redisFeatureSourceStaticContract, true),
		"raw_conn":            supportedStatus("GetRawConn exposes redis.UniversalClient", redisFeatureSourceStaticContract, true),
		"scheduled_task":      degradedStatus("scheduled task falls back to application scheduler", "use in-process scheduler fallback", redisFeatureSourceStaticContract, true),
		"query_builder":       unsupportedStatus("query builder provider unavailable", "redis native query provider not initialized", "use GetRawConn directly", redisFeatureSourceStaticContract, true),
		"feature_declaration": supportedStatus("database and query features are declared", redisFeatureSourceStaticContract, true),
	}
	if providerAvailable {
		adapterRequired["query_builder"] = supportedStatus("native redis query provider available", redisFeatureSourceStaticContract, true)
	}

	core := map[string]RedisFeatureStatus{
		"kv_get_set":               statusFromCommands(hasCommand("get") && hasCommand("set") && hasCommand("del"), "GET/SET/DEL available", "core kv commands unavailable", redisFeatureSourceRuntimeProbe, true),
		"ttl_expire":               statusFromCommands(hasCommand("expire") && hasCommand("ttl"), "TTL commands available", "expire/ttl commands unavailable", redisFeatureSourceRuntimeProbe, true),
		"batch_mget_mset":          statusFromCommands(hasCommand("mget") && hasCommand("mset"), "MGET/MSET available", "batch commands unavailable", redisFeatureSourceRuntimeProbe, true),
		"namespace_prefix":         supportedStatus("namespaced key helper available", redisFeatureSourceStaticContract, true),
		"tag_invalidation_minimal": supportedStatus("tag invalidation helper available", redisFeatureSourceStaticContract, true),
		"pagination_l2_isolation":  degradedStatus("pagination L2 isolation requires explicit cache domain configuration", "configure dedicated prefix/db for pagination cache", redisFeatureSourceStaticContract, true),
	}

	isolation := EvaluateRedisL2Isolation(a.config, "")
	if isolation.Allowed {
		core["pagination_l2_isolation"] = supportedStatus("pagination L2 isolation satisfied: "+isolation.Mode, redisFeatureSourceRuntimeProbe, true)
	} else {
		core["pagination_l2_isolation"] = degradedStatus("pagination L2 isolation not satisfied: "+isolation.Reason, "configure cluster mode, non-default db, or dedicated key namespace", redisFeatureSourceRuntimeProbe, true)
	}

	cluster := map[string]RedisFeatureStatus{
		"cluster_routing":      statusFromCommands(snapshot.Mode == "cluster", "cluster topology detected", "cluster mode not enabled", redisFeatureSourceRuntimeProbe, false),
		"multi_slot_batch":     degradedStatus("cross-slot batch semantics depend on client routing and key layout", "use hash tags or single-slot keys", redisFeatureSourceRuntimeProbe, false),
		"failover_awareness":   statusFromCommands(snapshot.Mode == "cluster", "cluster failover awareness delegated to go-redis", "standalone mode has no cluster failover", redisFeatureSourceStaticContract, false),
	}

	stack := map[string]RedisFeatureStatus{
		"graph_join":         unsupportedStatus("JOIN is disabled until graph baseline/runtime policy is implemented", "graph stack unavailable", "fallback to application-level multi-query composition", redisFeatureSourceRuntimeProbe, false),
		"json_cache":         statusFromCommands(hasModule("ReJSON") || hasModule("RedisJSON") || hasCommand("json.get"), "RedisJSON available", "RedisJSON module unavailable", redisFeatureSourceRuntimeProbe, false),
		"search_index_cache": statusFromCommands(hasModule("search") || hasModule("ft") || hasCommand("ft.search"), "RediSearch available", "RediSearch module unavailable", redisFeatureSourceRuntimeProbe, false),
		"vector_cache":       statusFromCommands(hasCommand("ft.search") && (hasModule("search") || hasModule("ft")), "vector search may be available through RediSearch", "vector search requires RediSearch vector support", redisFeatureSourceRuntimeProbe, false),
	}

	ops := map[string]RedisFeatureStatus{
		"metrics_export": supportedStatus("ping latency and basic server info can be collected", redisFeatureSourceRuntimeProbe, false),
		"slowlog_read":   statusFromCommands(hasCommand("slowlog"), "SLOWLOG command available", "SLOWLOG command unavailable", redisFeatureSourceRuntimeProbe, false),
		"health_probe":   supportedStatus("PING and INFO are probed at runtime", redisFeatureSourceRuntimeProbe, true),
	}

	return &RedisFeatureSupportMatrix{
		AdapterRequired: adapterRequired,
		Core:            core,
		Cluster:         cluster,
		Stack:           stack,
		Ops:             ops,
	}
}

func (a *RedisAdapter) InspectJSONRuntime(ctx context.Context) (*JSONRuntimeCapability, error) {
	report, err := a.InspectRedisRuntime(ctx)
	if err != nil {
		return nil, err
	}
	jsonFeature := report.Matrix.Stack["json_cache"]
	moduleName := ""
	if report.Snapshot != nil {
		for _, mod := range report.Snapshot.Modules {
			if strings.EqualFold(mod.Name, "ReJSON") || strings.EqualFold(mod.Name, "RedisJSON") {
				moduleName = mod.Name
				break
			}
		}
	}
	return &JSONRuntimeCapability{
		NativeSupported:          jsonFeature.State == redisFeatureStateSupported,
		NativeJSONTypeSupported:  jsonFeature.State == redisFeatureStateSupported,
		Version:                  report.Snapshot.ServerVersion,
		PluginChecked:            true,
		PluginAvailable:          jsonFeature.State == redisFeatureStateSupported,
		PluginName:               moduleName,
		Notes:                    jsonFeature.Reason,
	}, nil
}

func (a *RedisAdapter) InspectFullTextRuntime(ctx context.Context) (*FullTextRuntimeCapability, error) {
	report, err := a.InspectRedisRuntime(ctx)
	if err != nil {
		return nil, err
	}
	searchFeature := report.Matrix.Stack["search_index_cache"]
	moduleName := ""
	if report.Snapshot != nil {
		for _, mod := range report.Snapshot.Modules {
			if strings.EqualFold(mod.Name, "search") || strings.EqualFold(mod.Name, "ft") {
				moduleName = mod.Name
				break
			}
		}
	}
	return &FullTextRuntimeCapability{
		NativeSupported:       searchFeature.State == redisFeatureStateSupported,
		PluginChecked:         true,
		PluginAvailable:       searchFeature.State == redisFeatureStateSupported,
		PluginName:            moduleName,
		TokenizationSupported: searchFeature.State == redisFeatureStateSupported,
		TokenizationMode:      "plugin",
		Notes:                 searchFeature.Reason,
	}, nil
}

func parseRedisModuleList(raw interface{}) []RedisCapabilityModule {
	rows, ok := raw.([]interface{})
	if !ok {
		return nil
	}
	modules := make([]RedisCapabilityModule, 0, len(rows))
	for _, row := range rows {
		entries, ok := row.([]interface{})
		if !ok {
			continue
		}
		mod := RedisCapabilityModule{}
		for i := 0; i+1 < len(entries); i += 2 {
			key := strings.ToLower(strings.TrimSpace(fmt.Sprint(entries[i])))
			value := strings.TrimSpace(fmt.Sprint(entries[i+1]))
			switch key {
			case "name":
				mod.Name = value
			case "ver":
				mod.Version = value
			}
		}
		if mod.Name != "" {
			modules = append(modules, mod)
		}
	}
	sort.Slice(modules, func(i, j int) bool { return strings.ToLower(modules[i].Name) < strings.ToLower(modules[j].Name) })
	return modules
}

func probeRedisCommands(ctx context.Context, client redis.UniversalClient, names []string) map[string]bool {
	available := make(map[string]bool, len(names))
	for _, name := range names {
		upper := strings.ToUpper(strings.TrimSpace(name))
		if upper == "" {
			continue
		}
		result, err := client.Do(ctx, "COMMAND", "INFO", upper).Result()
		if err != nil {
			continue
		}
		entries, ok := result.([]interface{})
		if !ok || len(entries) == 0 || entries[0] == nil {
			continue
		}
		available[upper] = true
	}
	return available
}

func supportedStatus(reason, source string, enforced bool) RedisFeatureStatus {
	return RedisFeatureStatus{State: redisFeatureStateSupported, Reason: reason, Source: source, Enforced: enforced}
}

func degradedStatus(reason, fallback, source string, enforced bool) RedisFeatureStatus {
	return RedisFeatureStatus{State: redisFeatureStateDegraded, Reason: reason, FallbackAction: fallback, Source: source, Enforced: enforced}
}

func unsupportedStatus(reason, missing, fallback, source string, enforced bool) RedisFeatureStatus {
	return RedisFeatureStatus{State: redisFeatureStateUnavailable, Reason: reason, MissingDependency: missing, FallbackAction: fallback, Source: source, Enforced: enforced}
}

func statusFromCommands(ok bool, successReason, failureReason, source string, enforced bool) RedisFeatureStatus {
	if ok {
		return supportedStatus(successReason, source, enforced)
	}
	return unsupportedStatus(failureReason, "runtime probe failed", "disable feature or fallback to application layer", source, enforced)
}