# 声明式优先配置与 TOML 方案（vNext 草案）

本文定义 EIT-DB 在下一阶段的配置策略：声明式优先，构建式补充。

## 1. 结论

建议采用以下原则：

1. 声明式优先：系统拓扑、适配器注册、协作层绑定、Blueprint、Schema、Relation、Query Module 由配置文件声明。
2. 构建式补充：运行时仅做局部覆盖，不再承担主配置职责。
3. TOML 优先：新能力以 TOML 为唯一主规范，YAML/JSON 进入兼容层。

这能降低多后端协作场景下的心智负担，并提升可审计性与可复现性。

## 2. 为什么不是构建式优先

1. 构建式在单适配器场景简洁，但在多适配器协作与 Blueprint 路由场景会快速失控。
2. 代码拼装配置不利于平台化治理，难以做配置审计、差异对比和环境推广。
3. 声明式更适合 CI 门禁（Schema/Relation/Blueprint 可静态检查）。

## 3. TOML 相比 YAML 的收益

1. 层级与数组语义更稳定，减少复杂 YAML 错误（缩进、锚点、隐式类型）。
2. 对工程化配置更友好，适合长生命周期项目。
3. 更适合模块拆分与合并（主配置 + 子模块文件）。

## 4. vNext 配置模型（建议）

```toml
version = "2"
profile = "prod"

[runtime]
startup_mode = "declarative_first"
strict_validation = true

[adapters.postgres_main]
kind = "postgres"
enabled = true

[adapters.postgres_main.connection]
host = "127.0.0.1"
port = 5432
username = "postgres"
password = "postgres"
database = "app"
ssl_mode = "disable"

[adapters.redis_collab]
kind = "redis"
enabled = true

[adapters.redis_collab.connection]
host = "127.0.0.1"
port = 56379
db = 0

[adapters.arango_ledger]
kind = "arango"
enabled = true

[adapters.arango_ledger.connection]
uri = "http://127.0.0.1:58529"
database = "_system"
namespace = "collab_prod"

[collaboration]
enabled = true
namespace = "collab_prod"
control_plane = "redis_collab"
ledger_plane = "arango_ledger"

[[collaboration.bindings]]
group = "adapter-postgres"
adapter_ref = "postgres_main"
node_policy = "managed"
heartbeat_seconds = 30

[blueprint]
enabled = true
source = "file"
path = "./blueprints/main.toml"

[schema]
enabled = true
source = "file"
path = "./schemas/main.toml"

[relations]
enabled = true
source = "file"
path = "./relations/main.toml"

[query_modules]
enabled = true
source = "file"
path = "./queries/main.toml"
```

## 5. 关键语义定义

1. adapters: 声明可实例化适配器池，按 ID 引用。
2. collaboration: 声明协作层控制面、账本面和节点绑定。
3. blueprint: 声明路由策略输入与版本来源。
4. schema/relations: 声明关系语义编译输入。
5. query_modules: 声明可复用查询模块与能力约束。

## 6. 配置加载与优先级

建议优先级：

1. TOML 主配置文件（必选）
2. 环境变量覆盖（可选，仅覆盖敏感字段与部署差异）
3. 运行时构建式覆盖（仅调试与临时策略）

原则：任何覆盖都必须可追踪并可导出最终生效配置快照。

## 7. API 演进建议

建议新增：

1. LoadRuntimeManifest(path string) (*RuntimeManifest, error)
2. InitRuntimeFromManifest(path string) (*Runtime, error)
3. ExportEffectiveManifest() ([]byte, error)

兼容期保留：

1. LoadConfig / LoadAdapterRegistry（YAML/JSON）
2. NewConfig / MustConfig（构建式入口）

但新功能仅在 Manifest/TOML 路径扩展。

## 8. 事务与关系语义对齐

1. 事务语义由适配器能力声明决定，不由后端类型硬编码。
2. 关系语义分级作为 Blueprint 路由输入：
   - 强支持：图原生关系
   - 中支持：外键 + JOIN
   - 弱支持：聚合模拟
3. Query Module 在声明期绑定能力要求，不在执行期临时猜测。

## 9. 迁移路线

### Phase 1（下个 minor）

1. 引入 TOML 解析与 RuntimeManifest 结构。
2. 支持 adapters + collaboration + blueprint 基础字段。
3. 保持 YAML/JSON 可用，但仅兼容。

### Phase 2（后续 minor）

1. 接入 schema/relations/query_modules 声明加载。
2. 将 Blueprint 路由与关系语义分级联动。
3. 输出配置门禁检查报告。

### Phase 3（再后续）

1. 文档与脚手架默认输出 TOML。
2. YAML/JSON 标注为 deprecated。
3. 最终移除旧解析器扩展能力。

## 10. 实施边界

本文为设计草案，不改变当前稳定 API 行为。

具体落地以发布说明和迁移公告为准。
