# EIT-DB v2.0 开发指南

**文档版本**：1.0  
**发布日期**：2026-03-18  
**适用阶段**：v2.0.0-alpha ~ GA  
**维护者**：DevTeam

---

## 目录

1. [本地开发环境](#本地开发环境)
2. [编码规范与项目结构](#编码规范与项目结构)
3. [测试策略](#测试策略)
4. [关键组件实现指南](#关键组件实现指南)
5. [版本验证清单](#版本验证清单)
6. [故障排查](#故障排查)

---

## 本地开发环境

### 前置依赖

| 工具 | 最低版本 | 推荐版本 | 说明 |
|------|---------|---------|------|
| **Go** | 1.20 | 1.22+ | 使用 gomod 管理依赖 |
| **Docker** | 20.10 | 27.0+ | 容器化 Adapter（MySQL、PostgreSQL、MongoDB 等） |
| **Redis** | 6.0 | 7.0+ | Event Bus 与监控树存储 |
| **ArangoDB** | 3.10 | 3.11+ | Control Plane 存储（可选，v2.0.0-alpha 时可用本地 JSON） |
| **Make** | 3.81 | 4.3+ | 构建自动化 |
| **git** | 2.20 | 2.43+ | 版本控制 |

### 快速启动

```bash
# 1. 克隆仓库
git clone https://github.com/your-org/eit-db.git
cd eit-db

# 2. 安装 Go 依赖
go mod download
go mod tidy

# 3. 启动 Docker Compose（Redis + 示例数据库）
docker-compose -f docker-compose.yml up -d

# 4. 验证环境
make test-env

# 5. 构建二进制
make build

# 6. 首次运行
./bin/eit-db --version
```

### Docker Compose 扩展（v2.0 开发用）

在项目根目录的 `docker-compose-dev.yml` 中定义：

```yaml
version: '3.9'

services:
  # 核心中间件
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 5

  # 测试用 SQL Adapters
  postgres_primary:
    image: postgres:15-alpine
    environment:
      POSTGRES_USER: eit
      POSTGRES_PASSWORD: eitdev
      POSTGRES_DB: test_db
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

  mysql:
    image: mysql:8
    environment:
      MYSQL_ROOT_PASSWORD: eitdev
      MYSQL_DATABASE: test_db
      MYSQL_USER: eit
      MYSQL_PASSWORD: eitdev
    ports:
      - "3306:3306"
    volumes:
      - mysql_data:/var/lib/mysql

  # ArangoDB（可选，v2.0.0-beta 后启用）
  arangodb:
    image: arangodb:3.11
    environment:
      ARANGO_ROOT_PASSWORD: eitdev
    ports:
      - "8529:8529"
    volumes:
      - arango_data:/var/lib/arangodb3

volumes:
  redis_data:
  postgres_data:
  mysql_data:
  arango_data:
```

启动：
```bash
docker-compose -f docker-compose-dev.yml up -d
```

### 开发工具配置

#### VS Code 推荐扩展

```json
{
  "recommendations": [
    "golang.go",
    "golang.go-nightly",
    "eamodio.gitlens",
    "ms-azuretools.vscode-docker",
    "redhat.vscode-yaml",
    "esbenp.prettier-vscode"
  ]
}
```

#### Makefile 常用命令

```makefile
# 开发常用
make build              # 构建二进制
make test               # 运行全部单元测试
make test-v2            # 仅运行 v2.0 相关测试
make test-integration   # 集成测试
make test-coverage      # 生成覆盖率报告
make lint               # 代码风格检查
make fmt                # 自动格式化
make dev                # 启动开发模式（热重载）

# 性能与分析
make benchmark          # 性能基准测试
make profile-cpu        # CPU 性能分析
make profile-mem        # 内存分析
make trace              # 执行跟踪

# 清理
make clean              # 清理构建产物
make clean-docker       # 停止 Docker 容器
make clean-all          # 彻底清理
```

---

## 编码规范与项目结构

### 目录结构

```
eit-db/
├── cmd/
│   ├── eit-migrate/         # CLI 工具
│   │   ├── main.go
│   │   ├── commands.go      # 新增：compile、blueprint 子命令
│   │   ├── generate.go
│   │   └── init.go
│   └── eit-db/              # 运行时二进制（未来考虑）
│
├── pkg/
│   ├── adapter/             # Adapter 接口 & 实现
│   │   ├── adapter.go
│   │   └── [各 DB 适配器]
│   │
│   ├── apphost/             # AppHost 控制面（v2.0 新增）
│   │   ├── apphost.go       # 核心 Controller
│   │   ├── event_loop.go    # 事件处理循环
│   │   ├── blueprint_manager.go
│   │   └── planner.go       # Planner 协调
│   │
│   ├── actor/               # Actor 运行时（v2.0 新增）
│   │   ├── actor_supervisor.go
│   │   ├── base.go          # Actor 接口定义
│   │   ├── monitor_actor.go       # 监控树
│   │   ├── linker_actor.go        # 符号链接
│   │   ├── coordinator_actor.go   # 跨库编排
│   │   ├── cache_actor.go         # 计划缓存
│   │   └── adapter_actor.go       # Adapter 包装
│   │
│   ├── blueprint/           # Blueprint 系统（v2.0 新增）
│   │   ├── blueprint.go     # 核心数据结构
│   │   ├── arango_store.go  # ArangoDB 持久化
│   │   ├── loader.go        # Blueprint 加载与验证
│   │   ├── dsl/             # DSL 解析与编译
│   │   │   ├── parser.go
│   │   │   ├── compiler.go
│   │   │   └── lexer.go
│   │   └── symbol_table.go  # 符号表结构
│   │
│   ├── cache/               # 计划缓存（v2.0 新增）
│   │   ├── plan_cache.go
│   │   ├── key_generator.go
│   │   └── invalidation.go
│   │
│   ├── linker/              # 动态链接（v2.0 新增）
│   │   ├── linker.go        # Linker 核心逻辑
│   │   ├── link_table.go    # Link Table 维护
│   │   └── binding_strategy.go
│   │
│   ├── monitor/             # 监控树（v2.0 新增）
│   │   ├── topology.go      # 拓扑数据结构
│   │   ├── snapshot.go      # 快照管理
│   │   ├── fingerprint.go   # 能力指纹
│   │   └── redis_store.go   # Redis 存储
│   │
│   ├── router/              # 查询路由（v2.0 新增）
│   │   ├── router.go
│   │   └── consistency/
│   │       ├── policy.go
│   │       └── executor.go
│   │
│   ├── observability/       # 可观测性（v2.0 新增）
│   │   ├── tracing.go       # Jaeger 集成
│   │   ├── metrics.go       # Prometheus
│   │   └── audit.go         # 审计日志
│   │
│   ├── query/               # 查询处理（现有）
│   │   ├── query_builder.go
│   │   └── query_features.go
│   │
│   ├── schema/              # Schema（现有）
│   │   ├── schema.go
│   │   └── reflection.go
│   │
│   └── [其他现有包]
│
├── tests/
│   ├── unit/                # 单元测试
│   │   ├── apphost_test.go
│   │   ├── actor_test.go
│   │   ├── blueprint_test.go
│   │   ├── linker_test.go
│   │   └── monitor_test.go
│   │
│   ├── integration/         # 集成测试
│   │   ├── monitor_tree_test.go
│   │   ├── linker_binding_test.go
│   │   ├── cache_invalidation_test.go
│   │   ├── topology_change_test.go
│   │   └── cross_db_query_test.go
│   │
│   ├── e2e/                 # 端到端测试
│   │   ├── full_scenario_test.go
│   │   └── failover_test.go
│   │
│   ├── benchmark/           # 性能测试
│   │   ├── linker_binding_bench.go
│   │   ├── plan_cache_bench.go
│   │   └── coordinator_bench.go
│   │
│   ├── fixtures/            # 测试数据
│   │   ├── blueprint_fixtures.go
│   │   ├── adapter_mocks.go
│   │   └── sample_queries.go
│   │
│   └── test_helpers.go
│
├── docs/
│   ├── V2_0_ROADMAP.md           # 版本路线图（本文件）
│   ├── V2_0_DEV_GUIDE.md         # 本文件
│   ├── BLUEPRINT_V2_DRAFT.md     # 架构设计
│   ├── V2_0_API_SPEC.md          # API 规范（待建）
│   ├── OPERATIONAL_GUIDE.md      # 运维指南（待建）
│   └── [其他文档]
│
├── examples/
│   ├── blueprint_simple.json     # 简单 Blueprint
│   ├── blueprint_complex.json    # 复杂跨库 Blueprint
│   ├── dsl_example.blueprint     # DSL 示例
│   └── readme.md
│
├── scripts/
│   ├── setup-dev-env.sh          # 环境初始化
│   ├── generate-testdata.sh      # 生成测试数据
│   └── benchrun.sh               # 性能测试runner
│
├── docker-compose.yml                    # 生产配置
├── docker-compose-dev.yml                # 开发配置（新增）
├── Makefile                              # 构建配置
├── go.mod / go.sum
├── CONTRIBUTORS.md
└── LICENSE
```

### Go 编码规范

#### 1. 命名约定

**Package 命名**：
- 使用小写、简短、清晰（如 `monitor`、`linker`、`blueprint`）
- 避免缩写（不用 `mgr`，用 `manager`）
- 避免 `util`, `helper` 等名字，用具体功能名

**Interface 命名**：
- 单方法接口用 `er` 后缀（如 `Reader`, `Writer`）
- 多方法接口描述接口功能（如 `TopologyMonitor`, `SymbolLinker`）

**Struct 命名**：
- 使用 CapWords，不用所有字母大写（如 `MonitorActor` 而非 `MONITORActor`）
- Internal struct 用小写开头（如 `linkingContext`）

**Function/Method 命名**：
- Verb-Noun 模式（如 `StartMonitoring`, `ComputeBinding`, `InvalidateCache`）
- Test 函数用 `Test_ComponentName_Scenario` 格式

#### 2. 结构体设计

```go
// ✅ Good
type MonitorActor struct {
    id       string
    adapters map[string]*AdapterInstance
    redis    *redis.Client
    logger   Logger
    ctx      context.Context
    cancel   context.CancelFunc
}

// 公开方法
func (m *MonitorActor) Start(ctx context.Context) error { ... }
func (m *MonitorActor) Probe(adapter *AdapterInstance) (*HealthReport, error) { ... }

// 私有方法
func (m *monitorActor) computeFingerprint(reports []*HealthReport) string { ... }

// ❌ Avoid
type actor struct {  // 应该是 MonitorActor
    m map[string]interface{}  // 字段过度抽象
    p func()  // p 不清楚
}
```

#### 3. Error 处理

```go
// ✅ Good - 使用自定义 error type
type LinkingError struct {
    SymbolID string
    Reason   string
    Cause    error
}

func (e *LinkingError) Error() string {
    return fmt.Sprintf("linking failed for symbol %s: %s", e.SymbolID, e.Reason)
}

func (m *LinkerActor) Relink(ctx context.Context, symbols []string) error {
    for _, sym := range symbols {
        if err := m.bindSymbol(ctx, sym); err != nil {
            return &LinkingError{
                SymbolID: sym,
                Reason:   "no suitable adapter found",
                Cause:    err,
            }
        }
    }
    return nil
}

// ❌ Avoid - 裸错误
func (m *LinkerActor) Relink(ctx context.Context, symbols []string) error {
    // ...
    return errors.New("linking failed")  // 上下文不足
}
```

#### 4. 常量与文件头

```go
// 每个 go 文件必须包含文件头
package monitor

/*
File: topology.go
Purpose: 监控树数据结构与快照管理
Author: @team-v2
Updated: 2026-03-18

Key types:
  - TopologySnapshot: 监控树快照
  - CapabilityFingerprint: Adapter 能力指纹
*/

import (
    "context"
    "sync"
    "time"

    "github.com/eitdb/eitdb/pkg/adapter"
    "github.com/eitdb/eitdb/pkg/observability"
)

// 常量使用 const block 分组
const (
    // MonitoringInterval 定制心跳探测间隔
    MonitoringInterval = 60 * time.Second

    // HealthyThreshold Adapter 健康判定阈值
    HealthyThreshold = 0.95

    // TopologyChangeEventType 事件类型常量
    TopologyChangeEventType = "topology.changed"
)
```

#### 5. Context 使用

```go
// ✅ Good - context 作为第一参数
func (m *MonitorActor) Probe(ctx context.Context, adapterID string) (*HealthReport, error) {
    // 尊重 context 超时
    select {
    case <-ctx.Done():
        return nil, ctx.Err()
    case report := <-m.probeAsync(adapterID):
        return report, nil
    }
}

// ❌ Avoid - 忽略 context
func (m *MonitorActor) Probe(adapterID string) (*HealthReport, error) { ... }
```

---

## 测试策略

### 测试金字塔

```
        ╱╲
       ╱  ╲        E2E (端到端) - 5%
      ╱────╲      完整流程：跨库查询 + 故障转移
     ╱      ╲
    ╱────────╲    Integration - 30%
   ╱          ╲   多个组件协作（Actor←→Redis, Adapter←→Linker）
  ╱────────────╲
 ╱              ╲ Unit Tests - 65%
╱────────────────╲ 单个组件、接口、逻辑函数
```

### 单元测试（Unit Tests）

**覆盖范围**：
- 每个 public 方法至少 1 个测试
- happy path + 至少 2 个 error path
- 边界条件（nil, empty, overflow）

**示例**：

```go
// File: tests/unit/blueprint_test.go
package unit

import (
    "testing"
    "github.com/stretchr/testify/assert"
    "github.com/eitdb/eitdb/pkg/blueprint"
)

func TestBlueprintValidation_ValidBlueprint(t *testing.T) {
    bp := &blueprint.Blueprint{
        Version: "2.0",
        Entities: []blueprint.Entity{
            {
                ID:        "User",
                Adapters:  []string{"mysql_primary", "postgres_replica"},
            },
        },
    }
    
    err := bp.Validate()
    assert.NoError(t, err)
    assert.Equal(t, "2.0", bp.Version)
}

func TestBlueprintValidation_MissingEntity(t *testing.T) {
    bp := &blueprint.Blueprint{
        Version:  "2.0",
        Entities: []blueprint.Entity{},  // 空
    }
    
    err := bp.Validate()
    assert.Error(t, err)
    assert.Contains(t, err.Error(), "at least one entity")
}

func TestBlueprintValidation_InvalidAdapterRef(t *testing.T) {
    bp := &blueprint.Blueprint{
        Version: "2.0",
        Entities: []blueprint.Entity{
            {
                ID:       "User",
                Adapters: []string{"unknown_adapter"},
            },
        },
    }
    
    err := bp.Validate()
    assert.Error(t, err)
    assert.Contains(t, err.Error(), "unknown_adapter")
}
```

**运行**：
```bash
go test -v ./tests/unit/...
go test -cover ./tests/unit/...  # 覆盖率
```

### 集成测试（Integration Tests）

**覆盖范围**：
- Actor 协作（MonitorActor ←→ LinkerActor）
- Event Bus 消息流
- Redis 持久化与恢复
- Adapter 与监控树交互

**示例**：

```go
// File: tests/integration/monitor_tree_test.go
package integration

import (
    "context"
    "testing"
    "time"
    "github.com/stretchr/testify/assert"
    "github.com/eitdb/eitdb/pkg/actor"
    "github.com/eitdb/eitdb/pkg/monitor"
    "github.com/eitdb/eitdb/tests/fixtures"
)

func TestMonitoringTopologyChange(t *testing.T) {
    // Setup: 创建 MonitorActor 与假 Adapter 集群
    supervisor := actor.NewSupervisor()
    monitorActor, _ := supervisor.SpawnMonitorActor()
    
    // 注册 2 个 Adapter
    adapters := fixtures.CreateTestAdapters(2)
    for _, adapter := range adapters {
        monitorActor.Register(adapter)
    }
    
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()
    
    // Act: 启动监控并等待一个心跳周期
    assert.NoError(t, monitorActor.Start(ctx))
    time.Sleep(65 * time.Second)  // 等待首次心跳
    
    // 获取拓扑快照
    snapshot, _ := monitorActor.GetSnapshot(ctx)
    
    // Assert: 应该有 2 个 Adapter 且都是 healthy
    assert.Equal(t, 2, len(snapshot.Adapters))
    for _, a := range snapshot.Adapters {
        assert.True(t, a.IsHealthy)
    }
    
    // Arrange: 故障注入 - 让第一个 Adapter 失败
    adapters[0].InjectFailure()
    
    // Act: 等待下一个心跳周期
    time.Sleep(65 * time.Second)
    snapshot, _ = monitorActor.GetSnapshot(ctx)
    
    // Assert: Adapter 应该被标记为 unhealthy，LinkerActor 应该收到事件
    assert.False(t, snapshot.Adapters[0].IsHealthy)
}
```

**运行**：
```bash
go test -v -timeout 180s ./tests/integration/...
```

### 端到端测试（E2E Tests）

**覆盖场景**：
- 跨库查询完整流程（规划 → 编译 → 路由 → 执行 → 聚合）
- Adapter 故障与自动转移
- Blueprint 热加载与缓存失效

**示例**：

```go
// File: tests/e2e/full_scenario_test.go
package e2e

import (
    "context"
    "testing"
    "time"
    "github.com/stretchr/testify/assert"
    "github.com/eitdb/eitdb/pkg/apphost"
    "github.com/eitdb/eitdb/tests/fixtures"
)

func TestCrossDBQuery_FailoverScenario(t *testing.T) {
    // Setup: 启动完整系统（AppHost + Actor + 多个 Adapter）
    host := fixtures.SetupFullSystem(t, map[string]interface{}{
        "adapters": []string{"mysql_primary", "postgres_replica", "mongo_backup"},
        "redis":    "localhost:6379",
    })
    defer host.Shutdown()
    
    ctx := context.Background()
    
    // 等待监控树初始化
    time.Sleep(2 * time.Second)
    
    // Act: 执行跨库查询（从 MySQL 和 PostgreSQL 聚合数据）
    query := `
        SELECT u.id, u.name, o.total
        FROM mysql:users u
        JOIN postgres:orders o ON u.id = o.user_id
        WHERE u.country = ?
    `
    result, err := host.Query(ctx, query, "USA")
    
    // Assert: 应该成功返回结果
    assert.NoError(t, err)
    assert.Greater(t, len(result.Rows), 0)
    
    // Arrange: 注入故障 - MySQL Adapter 宕机
    host.KillAdapter("mysql_primary")
    
    // Act: 立即重新执行相同查询（应该 fallback 到备用）
    result2, err := host.Query(ctx, query, "USA")
    
    // Assert: 查询应该在 1 分钟内成功（故障转移时间）
    assert.NoError(t, err)
    assert.GreaterOrEqual(t, len(result2.Rows), len(result.Rows)-10)  // 允许小幅差异
}
```

**运行**：
```bash
go test -v -timeout 600s ./tests/e2e/...
```

### 性能基准测试（Benchmark）

```go
// File: tests/benchmark/linker_binding_bench.go
package benchmark

import (
    "context"
    "testing"
    "github.com/eitdb/eitdb/pkg/linker"
    "github.com/eitdb/eitdb/tests/fixtures"
)

func BenchmarkLinkerBinding_100Symbols_10Adapters(b *testing.B) {
    linker := fixtures.CreateTestLinker(100, 10)
    ctx := context.Background()
    
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        _ = linker.Relink(ctx, fixtures.GenerateSymbolList(100))
    }
}

// 运行: go test -bench=. -benchmem ./tests/benchmark/...
// 输出: BenchmarkLinkerBinding_100Symbols_10Adapters 10000  120456 ns/op  8032 B/op  45 allocs/op
```

### 测试数据与 Mock

**使用 fixture 工厂创建测试对象**：

```go
// File: tests/fixtures/adapters.go
package fixtures

type MockAdapter struct {
    ID          string
    Healthy     bool
    FailCount   int
    Features    map[string]bool
    failureMode string  // "timeout", "error", "none"
}

func CreateTestAdapter(id string) *MockAdapter {
    return &MockAdapter{
        ID:      id,
        Healthy: true,
        Features: map[string]bool{
            "transactions": true,
            "nested_select": true,
        },
    }
}

func CreateTestAdapters(count int) []*MockAdapter {
    var adapters []*MockAdapter
    for i := 0; i < count; i++ {
        adapters = append(adapters, CreateTestAdapter(fmt.Sprintf("adapter_%d", i)))
    }
    return adapters
}
```

---

## 关键组件实现指南

### 1. Adapter 监控树实现

**Phase**：v2.0.0-alpha, Week 1-2

**核心文件**：
- `pkg/monitor/topology.go` - 数据结构
- `pkg/monitor/snapshot.go` - 快照管理
- `pkg/monitor/redis_store.go` - 持久化
- `pkg/actor/monitor_actor.go` - 心跳逻辑

**实现步骤**：

1. **定义数据结构**（Day 1-2）

```go
// pkg/monitor/topology.go
type TopologySnapshot struct {
    Timestamp    time.Time
    Adapters     map[string]*AdapterSnapshot
    TTL          time.Duration
    Hash         string  // 快照内容哈希，用于变化检测
}

type AdapterSnapshot struct {
    ID                string
    Endpoint          string
    IsHealthy         bool
    LastProbeTime     time.Time
    LastProbeDelay    time.Duration
    FeatureLevelMap   map[string]string  // feature -> "full"/"partial"/"weak"
    Fingerprint       string  // 指纹哈希
    ErrorCount        int
}

type CapabilityFingerprint struct {
    AdapterID        string
    Version          string
    FeatureMap       map[string]string  // 特性 → 支持强度
    ChecksumSHA256   string
    Timestamp        time.Time
}
```

2. **实现 MonitorActor**（Day 3-5）

```go
// pkg/actor/monitor_actor.go
type MonitorActor struct {
    id          string
    adapters    map[string]*AdapterInstance
    snapshot    *TopologySnapshot
    redis       *redis.Client
    eventChan   chan TopologyEvent
    ticker      *time.Ticker
    logger      Logger
    ctx         context.Context
    cancel      context.CancelFunc
}

func (m *MonitorActor) Start(ctx context.Context) error {
    m.ctx, m.cancel = context.WithCancel(ctx)
    m.ticker = time.NewTicker(MonitoringInterval)
    
    go func() {
        for {
            select {
            case <-m.ctx.Done():
                return
            case <-m.ticker.C:
                m.probeAll()
            }
        }
    }()
    
    return nil
}

func (m *MonitorActor) probeAll() error {
    newSnapshot := &TopologySnapshot{
        Timestamp: time.Now(),
        Adapters:  make(map[string]*AdapterSnapshot),
        TTL:       60 * time.Second,
    }
    
    for adapterID, instance := range m.adapters {
        report := m.probeAdapter(adapterID, instance)
        newSnapshot.Adapters[adapterID] = report
    }
    
    // 检测变化并发送事件
    if m.hasTopologyChanged(m.snapshot, newSnapshot) {
        m.snapshot = newSnapshot
        m.publishTopologyChanged()
    }
    
    // 持久化到 Redis
    return m.persistSnapshot(newSnapshot)
}

func (m *MonitorActor) probeAdapter(id string, instance *AdapterInstance) *AdapterSnapshot {
    start := time.Now()
    err := instance.Ping(m.ctx)
    delay := time.Since(start)
    
    snapshot := &AdapterSnapshot{
        ID:            id,
        Endpoint:      instance.Endpoint,
        IsHealthy:     err == nil,
        LastProbeTime: time.Now(),
        LastProbeDelay: delay,
    }
    
    if err == nil {
        snapshot.FeatureLevelMap = m.extractFeatures(instance)
        snapshot.Fingerprint = m.computeFingerprint(snapshot)
    }
    
    return snapshot
}

func (m *MonitorActor) persistSnapshot(snapshot *TopologySnapshot) error {
    data, _ := json.Marshal(snapshot)
    return m.redis.Set(
        m.ctx,
        fmt.Sprintf("eit:topology:snapshot:%s", time.Now().Format("2006-01-02")),
        data,
        snapshot.TTL,
    ).Err()
}
```

3. **Redis 存储**（Day 6）

```go
// pkg/monitor/redis_store.go
type RedisTopologyStore struct {
    client *redis.Client
}

func (s *RedisTopologyStore) SaveSnapshot(ctx context.Context, snapshot *TopologySnapshot) error {
    // 保存当前快照
    data, _ := json.Marshal(snapshot)
    key := fmt.Sprintf("eit:topo:current")
    if err := s.client.Set(ctx, key, data, 60*time.Second).Err(); err != nil {
        return err
    }
    
    // 备份历史版本
    histKey := fmt.Sprintf("eit:topo:history:%d", time.Now().Unix())
    return s.client.Set(ctx, histKey, data, 7*24*time.Hour).Err()
}

func (s *RedisTopologyStore) GetSnapshot(ctx context.Context) (*TopologySnapshot, error) {
    data, err := s.client.Get(ctx, "eit:topo:current").Result()
    if err != nil {
        return nil, err
    }
    
    var snapshot TopologySnapshot
    if err := json.Unmarshal([]byte(data), &snapshot); err != nil {
        return nil, err
    }
    
    return &snapshot, nil
}
```

4. **测试**（Day 7）

- 单测：心跳逻辑、指纹计算、变化检测
- 集成测试：MonitorActor + Redis
- 压测：100+ 个 Adapter 的监控吞吐量

**验收标准**：
- 故障检测延迟 < 70s
- Redis 写 QPS > 1K
- 内存占用 < 50MB（100 个 Adapter）

---

### 2. Symbol Linker 实现

**Phase**：v2.0.0-alpha, Week 3-4

**核心文件**：
- `pkg/linker/linker_actor.go` - 链接策略与路由
- `pkg/linker/link_table.go` - Link Table 维护
- `pkg/linker/binding_strategy.go` - 绑定算法

**实现步骤**：

1. **定义 Link Table 与数据结构**（Day 1）

```go
// pkg/linker/link_table.go
type LinkTable struct {
    mu      sync.RWMutex
    bindings map[string]*SymbolBinding  // symbol_id → binding
    version  int64
}

type SymbolBinding struct {
    SymbolID        string
    SelectedAdapter string  // Adapter ID
    Alternatives    []string  // 备选 Adapter 列表（优先级排序）
    Strength        string  // feature support strength
    LastBindTime    time.Time
    BindGeneration  int64
}

type LinkingContext struct {
    TopologySnapshot *TopologySnapshot
    SymbolTable      *SymbolTable
    Policy           RoutingPolicy
}

type RoutingPolicy struct {
    PreferPrimary    bool
    AllowFallback    bool
    MaxFallbackDepth int
}
```

2. **LinkerActor 与绑定算法**（Day 2-4）

```go
// pkg/linker/linker_actor.go
type LinkerActor struct {
    linkTable *LinkTable
    strategy  *BindingStrategy
    redis     *redis.Client
    eventChan chan TopologyEvent
    logger    Logger
}

func (l *LinkerActor) OnTopologyChanged(snapshot *TopologySnapshot) error {
    ctx := &LinkingContext{
        TopologySnapshot: snapshot,
        SymbolTable:      l.getCurrentSymbolTable(),
        Policy: RoutingPolicy{
            PreferPrimary: true,
            AllowFallback: true,
        },
    }
    
    // 重新计算所有受影响的 symbol 绑定
    affectedSymbols := l.findAffectedSymbols(snapshot)
    
    for _, sym := range affectedSymbols {
        binding, err := l.strategy.ComputeBinding(ctx, sym)
        if err != nil {
            l.logger.Warn("binding failed for symbol", sym, err)
            continue
        }
        
        l.linkTable.Update(sym, binding)
    }
    
    // 发送 LinkingUpdated 事件（触发 CacheActor 失效）
    l.publishLinkingUpdated(affectedSymbols)
    
    return nil
}

func (s *BindingStrategy) ComputeBinding(ctx *LinkingContext, symbolID string) (*SymbolBinding, error) {
    symbol := ctx.SymbolTable.Get(symbolID)
    if symbol == nil {
        return nil, fmt.Errorf("symbol not found: %s", symbolID)
    }
    
    // 候选 Adapter 列表（从 symbol 定义中获取，按优先级排序）
    candidates := symbol.AdapterPreference  // ["mysql", "postgres", "mongo"]
    
    // 根据拓扑快照过滤可用 Adapter
    available := make([]string, 0)
    for _, candidate := range candidates {
        if snapshot, ok := ctx.TopologySnapshot.Adapters[candidate]; ok {
            if snapshot.IsHealthy && s.supportsFeature(snapshot, symbol.RequiredFeature) {
                available = append(available, candidate)
            }
        }
    }
    
    if len(available) == 0 {
        return nil, fmt.Errorf("no healthy adapter for symbol %s", symbolID)
    }
    
    return &SymbolBinding{
        SymbolID:        symbolID,
        SelectedAdapter: available[0],  // 选择第一个（最优先）
        Alternatives:    available[1:],
        Strength:        symbol.RequiredStrength,
        LastBindTime:    time.Now(),
        BindGeneration:  ctx.TopologySnapshot.Generation,
    }, nil
}

func (s *BindingStrategy) supportsFeature(snapshot *AdapterSnapshot, feature string) bool {
    level, ok := snapshot.FeatureLevelMap[feature]
    if !ok {
        return false
    }
    // "full" 和 "partial" 都支持，"weak" 等需要特殊处理
    return level == "full" || level == "partial"
}
```

3. **Link Table 存储与查询**（Day 5）

```go
// pkg/linker/link_table.go
func (lt *LinkTable) GetBinding(symbolID string) *SymbolBinding {
    lt.mu.RLock()
    defer lt.mu.RUnlock()
    return lt.bindings[symbolID]
}

func (lt *LinkTable) Update(symbolID string, binding *SymbolBinding) {
    lt.mu.Lock()
    defer lt.mu.Unlock()
    
    lt.bindings[symbolID] = binding
    lt.version++
}

func (lt *LinkTable) GetChangeList(fromVersion int64) map[string]*SymbolBinding {
    lt.mu.RLock()
    defer lt.mu.RUnlock()
    
    if fromVersion >= lt.version {
        return nil
    }
    
    changes := make(map[string]*SymbolBinding)
    for id, binding := range lt.bindings {
        // 真实实现需要记录版本历史
        changes[id] = binding
    }
    return changes
}
```

4. **集成与测试**（Day 6-7）

```go
// tests/integration/linker_binding_test.go
func TestLinkerRebinding_OnAdapterFailure(t *testing.T) {
    // Setup
    linker := createTestLinker()
    symbol := &Symbol{
        ID: "cross_db_query_1",
        AdapterPreference: []string{"mysql", "postgres", "mongo"},
    }
    
    // 初始绑定：mysql（主）
    binding := linker.ComputeBinding(symbol, healthyAdapters)
    assert.Equal(t, "mysql", binding.SelectedAdapter)
    assert.Equal(t, []string{"postgres", "mongo"}, binding.Alternatives)
    
    // 模拟 MySQL 故障
    failedAdapters := removeAdapter(healthyAdapters, "mysql")
    
    // 重新绑定
    newBinding := linker.ComputeBinding(symbol, failedAdapters)
    assert.Equal(t, "postgres", newBinding.SelectedAdapter)
    assert.Equal(t, []string{"mongo"}, newBinding.Alternatives)
}
```

**验收标准**：
- 绑定计算延迟 < 100ms（100 symbols）
- Link Table 隔离性正确（无竞态条件）
- 故障转移正确性（1 级以上故障自动转移）

---

### 3. Blueprint DSL Parser 实现

**Phase**：v2.0.0-beta, Week 9-10

**示例 DSL 语法**：

```blueprint
// example.blueprint

namespace "ecommerce" {
  version "2.0"

  // 实体定义
  entity "User" {
    adapters = ["mysql_primary", "postgres_replica"]
    
    fields {
      id: string @required @key
      email: string @index
      country: string
    }
  }

  entity "Orders" {
    adapters = ["postgres_primary", "mongo_secondary"]
    
    when feature:transactions {
      consistency = "strong"
    } or {
      consistency = "eventual"
    }
  }

  // 关系定义
  relationship "user_orders" {
    from = "User.id"
    to = "Orders.user_id"
    
    where hasFeatures{transactions, nested_select} or fallback to SingleQuery
  }

  // 查询模板
  query "get_user_orders" {
    require_adapters = ["mysql", "postgres"]
    template = """
      SELECT u.id, u.email, COUNT(o.id) as order_count
      FROM User u
      LEFT JOIN Orders o ON u.id = o.user_id
      GROUP BY u.id
    """
    
    strategy {
      when feature_level:full {
        use_parallel_fetch = true
      } or {
        use_parallel_fetch = false
      }
    }
  }

  // 包含其他模块
  include "./adapters/mysql_config.blueprint"
}
```

**实现步骤**：

1. **Lexer（词法分析）**（Day 1-2）

```go
// pkg/blueprint/dsl/lexer.go
type TokenType int

const (
    TOKEN_NAMESPACE TokenType = iota
    TOKEN_ENTITY
    TOKEN_RELATIONSHIP
    TOKEN_QUERY
    TOKEN_INCLUDE
    TOKEN_WHERE
    TOKEN_WHEN
    TOKEN_OR
    // ... 其他 token
)

type Token struct {
    Type  TokenType
    Value string
    Line  int
    Col   int
}

type Lexer struct {
    input  string
    pos    int
    line   int
    col    int
}

func (l *Lexer) NextToken() *Token {
    // 跳过空格和注释
    l.skipWhitespaceAndComments()
    
    if l.pos >= len(l.input) {
        return &Token{Type: TOKEN_EOF}
    }
    
    ch := l.input[l.pos]
    
    // 识别关键字
    if isLetter(ch) {
        return l.readKeywordOrIdentifier()
    }
    
    // 识别字符串
    if ch == '"' {
        return l.readString()
    }
    
    // ... 处理其他 token
}
```

2. **Parser（语法分析）**（Day 3-5）

```go
// pkg/blueprint/dsl/parser.go
type Parser struct {
    tokens  []*Token
    pos     int
    current *Token
}

func (p *Parser) parseBlueprint() (*Blueprint, error) {
    bp := &Blueprint{
        Entities:      make([]Entity, 0),
        Relationships: make([]Relationship, 0),
        Queries:       make([]Query, 0),
    }
    
    for !p.isAtEnd() {
        switch p.current.Type {
        case TOKEN_NAMESPACE:
            p.advance()
            name := p.expect(TOKEN_IDENTIFIER).Value
            if err := p.parseNamespace(bp); err != nil {
                return nil, err
            }
            bp.Namespace = name
            
        case TOKEN_ENTITY:
            entity, err := p.parseEntity()
            if err != nil {
                return nil, err
            }
            bp.Entities = append(bp.Entities, *entity)
            
        case TOKEN_INCLUDE:
            path := p.parseInclude()
            if err := p.loadIncludedBlueprint(bp, path); err != nil {
                return nil, err
            }
        }
        
        p.advance()
    }
    
    return bp, nil
}

func (p *Parser) parseEntity() (*Entity, error) {
    p.expect(TOKEN_ENTITY)
    name := p.expect(TOKEN_IDENTIFIER).Value
    
    p.expect(TOKEN_LBRACE)
    
    entity := &Entity{ID: name}
    
    for !p.check(TOKEN_RBRACE) {
        switch p.current.Value {
        case "adapters":
            entity.Adapters = p.parseStringArray()
        case "fields":
            entity.Fields = p.parseFields()
        case "when":
            condition := p.parseWhenCondition()
            entity.Conditions = append(entity.Conditions, condition)
        }
        
        p.advance()
    }
    
    p.expect(TOKEN_RBRACE)
    return entity, nil
}

func (p *Parser) parseWhenCondition() *Condition {
    // 解析 when feature:transactions { ... } or { ... }
    p.expect(TOKEN_WHEN)
    
    feature := p.parseFeatureRef()  // e.g., "feature:transactions"
    
    p.expect(TOKEN_LBRACE)
    primary := p.parseConditionBody()
    p.expect(TOKEN_RBRACE)
    
    var fallback *ConditionBody
    if p.match(TOKEN_OR) {
        p.expect(TOKEN_LBRACE)
        fallback = p.parseConditionBody()
        p.expect(TOKEN_RBRACE)
    }
    
    return &Condition{
        Feature:   feature,
        Primary:   primary,
        Fallback:  fallback,
    }
}
```

3. **编译器（IR 生成）**（Day 6）

```go
// pkg/blueprint/dsl/compiler.go
type Compiler struct {
    bp *Blueprint
}

func (c *Compiler) Compile() (*CompiledBlueprint, error) {
    compiled := &CompiledBlueprint{
        Namespace:   c.bp.Namespace,
        Version:     c.bp.Version,
        SymbolTable: NewSymbolTable(),
    }
    
    // 编译 entity → symbols
    for _, entity := range c.bp.Entities {
        for _, field := range entity.Fields {
            symbol := c.createSymbol(entity.ID, field)
            compiled.SymbolTable.Add(symbol)
        }
        
        // 处理 when 条件
        for _, cond := range entity.Conditions {
            variants := c.expandCondition(cond)
            for _, variant := range variants {
                compiled.SymbolTable.AddVariant(symbol, variant)
            }
        }
    }
    
    return compiled, nil
}
```

**验收标准**：
- 解析错误率 < 0.1%（在有效语法下）
- 编译延迟 < 50ms（普通 Blueprint）
- 代码覆盖率 > 80%

---

### 4. 计划缓存与 CacheActor 实现

**Phase**：v2.0.0-beta, Week 11-12

**实现参考** `16. 计划缓存键规范` 部分（见 BLUEPRINT_V2_DRAFT.md）

缓存键格式：
```
cache:plan:{blueprint_version}:{symbol_ids_hash}:{param_hash}
```

**实现步骤**：

```go
// pkg/cache/plan_cache.go
type PlanCache struct {
    mu          sync.RWMutex
    plans       map[string]*CachedPlan
    lru         *lru.Cache
    maxSize     int
    hitCount    int64
    missCount   int64
}

type CachedPlan struct {
    Key              string
    Plan             *ExecutionPlan
    BlueprintVersion string
    SymbolFingers    []string
    ParamHash        string
    CreatedAt        time.Time
    ExpiresAt        time.Time
    HitCount         int64
}

func (pc *PlanCache) Get(key string) (*ExecutionPlan, bool) {
    pc.mu.RLock()
    defer pc.mu.RUnlock()
    
    if cached, ok := pc.plans[key]; ok {
        if time.Now().Before(cached.ExpiresAt) {
            pc.hitCount++
            cached.HitCount++
            return cached.Plan, true
        }
    }
    
    pc.missCount++
    return nil, false
}

func (pc *PlanCache) Set(key string, plan *ExecutionPlan, ttl time.Duration) error {
    pc.mu.Lock()
    defer pc.mu.Unlock()
    
    if len(pc.plans) >= pc.maxSize {
        pc.lru.RemoveOldest()
    }
    
    pc.plans[key] = &CachedPlan{
        Key:       key,
        Plan:      plan,
        CreatedAt: time.Now(),
        ExpiresAt: time.Now().Add(ttl),
    }
    
    return nil
}

func (pc *PlanCache) Invalidate(symbolIDs []string) error {
    pc.mu.Lock()
    defer pc.mu.Unlock()
    
    toDelete := make([]string, 0)
    for key, cached := range pc.plans {
        for _, symbol := range symbolIDs {
            for _, finger := range cached.SymbolFingers {
                if finger == symbol {
                    toDelete = append(toDelete, key)
                    break
                }
            }
        }
    }
    
    for _, key := range toDelete {
        delete(pc.plans, key)
    }
    
    return nil
}

// CacheActor 监听 LinkingUpdated 事件并触发失效
// pkg/actor/cache_actor.go
type CacheActor struct {
    cache     *PlanCache
    eventChan chan *LinkingUpdatedEvent
}

func (ca *CacheActor) OnLinkingUpdated(event *LinkingUpdatedEvent) error {
    // 精确失效：仅失效受影响的计划
    return ca.cache.Invalidate(event.AffectedSymbols)
}
```

---

## 版本验证清单

### v2.0.0-alpha 验收清单

**功能完整性**：
- [ ] MonitorActor 完整实现
  - [ ] 心跳探测（60s 间隔）
  - [ ] 能力指纹提取
  - [ ] 拓扑变化检测
  - [ ] Redis 持久化

- [ ] LinkerActor 完整实现
  - [ ] Symbol 绑定算法
  - [ ] Link Table 维护
  - [ ] Fallback 策略
  - [ ] 事件发布

- [ ] Blueprint 基础模型
  - [ ] MVP 字段集定义
  - [ ] ArangoDB 存储
  - [ ] 加载与验证

**性能基准**：
- [ ] 监控心跳 QPS > 1K（100 个 Adapter）
- [ ] Symbol 绑定延迟 < 100ms（100 symbols）
- [ ] Blueprint 加载延迟 < 50ms

**稳定性**：
- [ ] 5 天长运行测试无内存泄漏
- [ ] 故障检测准确率 > 95%
- [ ] 事件丢失率 = 0%

**文档**：
- [ ] API 设计文档
- [ ] 组件交互图
- [ ] 故障排查指南

---

### v2.0.0-beta 验收清单

基于 alpha 的所有项目，加上：

**新增功能**：
- [ ] DSL 编译器（所有语法）
- [ ] 计划缓存系统
- [ ] CoordinatorActor 完整实现
- [ ] 可观测性（tracing + metrics）

**性能基准**：
- [ ] 缓存命中率 > 80%
- [ ] 跨库查询 p99 < 500ms
- [ ] 故障转移时间 < 1min

**代码质量**：
- [ ] 覆盖率 ≥ 75%
- [ ] 无死代码（checked by govet）
- [ ] Lint 无警告

---

### v2.0.0 GA 验收清单

基于 beta 的所有项目，加上：

**生产就绪**：
- [ ] 单机 10K+ tps 压测通过
- [ ] 与 v1.0 向后兼容
- [ ] 安全审计通过
- [ ] 故障演练成功

**文档完整**：
- [ ] Release Notes
- [ ] Migration Guide
- [ ] API 文档终稿
- [ ] 运维指南

---

## 故障排查

### 常见问题

#### Q1：LinkerActor 无法连接 Redis
**症状**：`linking failed: redis connection timeout`

**排查**：
```bash
# 1. 检查 Redis 运行状态
docker ps | grep redis
docker exec redis-container redis-cli ping

# 2. 检查配置
grep "redis_addr" config/dev.yaml

# 3. 查看日志
tail -f logs/linker_actor.log | grep "redis"

# 修复：重启 Redis 或更新连接字符串
```

#### Q2：计划缓存命中率太低
**症状**：缓存命中率 < 50%（预期 80%+）

**排查**：
```bash
# 1. 检查缓存大小配置
grep "cache_size_mb" config/dev.yaml

# 2. 查看缓存统计
make debug-cache-stats

# 3. 分析 symbol 指纹分布
tail -f logs/cache_actor.log | grep "fingerprint"

# 修复：增加缓存大小或检查是否正确计算指纹
```

#### Q3：监控树故障检测延迟超过 60s
**症状**：人为宕掉 Adapter 后，监控树 > 120s 才检测

**排查**：
```bash
# 1. 检查心跳间隔配置
grep "monitoring_interval" config/dev.yaml

# 2. 查看 Adapter 响应时间
tail -f logs/monitor_actor.log | grep "probe_latency"

# 3. 检查是否有网络延迟
ping <adapter_host>

# 修复：减小监控间隔或优化网络
```

### 调试工作流

**启用 Debug 模式**：
```bash
export EIT_DEBUG=1
export EIT_LOG_LEVEL=debug
make build
./bin/eit-db

# 查看 pprof 分析
go tool pprof http://localhost:6060/debug/pprof/heap
```

**性能分析**：
```bash
# CPU 分析
go tool pprof http://localhost:6060/debug/pprof/profile?seconds=30

# 并发分析
go tool pprof http://localhost:6060/debug/pprof/goroutine
```

---

## 提交与审查流程

### Commit 规范

```
feat(v2/monitor): implement MonitorActor heartbeat logic

- Add TopologySnapshot data structure
- Implement Adapter probing with configurable interval
- Add Redis persistence layer
- Support health detection with > 95% accuracy

Fixes #123
```

### 代码审查检查项

每个 PR 必须通过：

- [ ] `go test -cover ./...` 覆盖率 ≥ 75%
- [ ] `golangci-lint run` 0 warnings
- [ ] `gofmt` 格式检查
- [ ] 至少 1 个 maintainer 审查
- [ ] 关键组件（monitor, linker, blueprint）需要 2 个审查

---

该文档将定期更新。如有问题，请在 GitHub Issues 中反馈。
