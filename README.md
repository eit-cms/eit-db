# EIT-DB - Go 数据库抽象层

一个受 Ecto (Elixir) 启发的 Go 数据库抽象层，提供类型安全的 Schema、Changeset 和跨数据库适配器支持。

## ✨ 特性

- **Schema 系统** - 声明式数据结构定义，支持验证器和默认值
- **Changeset 验证** - 数据变更追踪和验证，类似 Ecto.Changeset，支持丰富的验证规则
- **Query Constructor** - 三层查询构造架构，支持 MySQL/PostgreSQL/SQLite 方言 (v0.4.1+)
- **Migration 工具** - 灵活的数据库迁移工具，支持 Schema-based 和 Raw SQL 两种方式
- **跨数据库适配器** - 支持 MySQL, PostgreSQL, SQLite, SQL Server
- **查询构建器** - 类型安全的查询接口
- **GORM 集成** - 完全兼容 GORM v1/v2，可无缝协作
- **动态建表** - 支持运行时动态创建表，PostgreSQL 用触发器，MySQL/SQLite 用 GORM Hook
- **定时任务** - 跨数据库的定时任务支持

## 📦 安装

```bash
go get github.com/eit-cms/eit-db
```

## 🚀 快速开始

### 1. 配置数据库连接

**使用 YAML 配置文件：**

```yaml
# config.yaml
database:
  adapter: sqlite
  database: ./data/app.db
  pool:
    max_connections: 25
    idle_timeout: 300
```

**多 Adapter YAML 配置（新）：**

```yaml
# adapters.yaml
adapters:
    primary:
        adapter: postgres
        host: localhost
        port: 5432
        username: postgres
        password: ""
        database: app
        ssl_mode: disable

    search:
        adapter: mongodb
        database: search_db
        options:
            uri: "mongodb://localhost:27017"
```

**使用多 Adapter 配置：**

```go
registry, err := eit_db.LoadAdapterRegistry("adapters.yaml")
if err != nil {
        panic(err)
}

if err := eit_db.RegisterAdapterConfigs(registry); err != nil {
        panic(err)
}

repo, err := eit_db.NewRepositoryFromAdapterConfig("primary")
if err != nil {
        panic(err)
}
defer repo.Close()
```

**或使用代码配置：**

```go
package main

import "github.com/eit-cms/eit-db"

func main() {
    config := &eit_db.Config{
        Adapter:   "sqlite",
        Database:  "./data/app.db",
        Pool: &eit_db.PoolConfig{
            MaxConnections: 25,
            IdleTimeout:    300,
        },
    }
    
    repo, err := eit_db.NewRepository(config)
    if err != nil {
        panic(err)
    }
    defer repo.Close()
    
    // 现在可以使用 GORM
    gormDB := repo.GetGormDB()
}
```

### 2. 定义 Schema

```go
func BuildUserSchema() db.Schema {
    schema := db.NewBaseSchema("users")
    
    schema.AddField(
        db.NewField("id", db.TypeInteger).
            PrimaryKey().
            Build(),
    )
    
    schema.AddField(
        db.NewField("email", db.TypeString).
            Null(false).
            Unique().
            Validate(&db.EmailValidator{}).
            Build(),
    )
    
    schema.AddField(
        db.NewField("created_at", db.TypeTime).Build(),
    )
    
    return schema
}
```

### 3. 使用 GORM ORM

```go
type User struct {
    ID    uint
    Email string
}

repo, _ := eit_db.InitDB("config.yaml")
gormDB := repo.GetGormDB()

// 使用 GORM 的所有功能
var users []User
gormDB.Find(&users)

gormDB.Create(&User{Email: "test@example.com"})
```

### 4. 使用 Changeset 进行数据验证 (v0.3.1+)

```go
// 创建 Changeset
cs := db.NewChangeset(userSchema)
cs.Cast(map[string]interface{}{
    "name":  "Alice",
    "email": "alice@example.com",
    "age":   25,
})

// 链式验证
cs.ValidateRequired([]string{"name", "email"}).
   ValidateLength("name", 2, 50).
   ValidateFormat("email", `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`).
   ValidateNumber("age", map[string]interface{}{"greater_than_or_equal_to": 18.0})

// 检查验证结果
if cs.IsValid() {
    // 数据有效，可以保存
    data := cs.GetChanges()
} else {
    // 显示错误
    for field, errors := range cs.Errors() {
        fmt.Printf("%s: %v\n", field, errors)
    }
}
```

**可用的验证方法：**
- `ValidateRequired(fields)` - 验证必填字段
- `ValidateLength(field, min, max)` - 验证字符串长度
- `ValidateFormat(field, pattern)` - 正则表达式验证
- `ValidateInclusion(field, list)` - 白名单验证
- `ValidateExclusion(field, list)` - 黑名单验证
- `ValidateNumber(field, opts)` - 数字范围验证

### 5. 数据库迁移工具 (v0.4.0+)

EIT-DB 提供了强大的迁移工具，支持两种迁移方式：

**初始化迁移项目：**
```bash
# 安装工具
go install github.com/eit-cms/eit-db/cmd/eit-migrate@latest

# 或直接构建
cd /path/to/eit-db
go build -o ~/bin/eit-migrate ./cmd/eit-migrate

# 初始化迁移项目
eit-migrate init
```

**生成迁移文件：**
```bash
# 生成 Schema-based 迁移（类型安全）
eit-migrate generate create_users_table

# 生成 Raw SQL 迁移（完全控制）
eit-migrate generate add_indexes --type sql
```

**运行迁移：**
```bash
cd migrations
cp .env.example .env
# 编辑 .env 配置数据库连接

# 运行迁移
go run . up

# 查看状态
go run . status

# 回滚最后一个迁移
go run . down
```

**Schema-based Migration 示例：**
```go
func NewMigration_20260203150405_CreateUsersTable() db.MigrationInterface {
    migration := db.NewSchemaMigration("20260203150405", "create_users_table")

    userSchema := db.NewBaseSchema("users")
    userSchema.AddField(&db.Field{
        Name:    "id",
        Type:    db.TypeInteger,
        Primary: true,
        Autoinc: true,
    })
    userSchema.AddField(&db.Field{
        Name: "email",
        Type: db.TypeString,
        Null: false,
        Unique: true,
    })

    migration.CreateTable(userSchema)
    return migration
}
```

**Raw SQL Migration 示例：**
```go
func NewMigration_20260203160000_AddIndexes() db.MigrationInterface {
    migration := db.NewRawSQLMigration("20260203160000", "add_indexes")

    migration.AddUpSQL(`
        CREATE INDEX idx_users_email ON users(email);
        CREATE INDEX idx_posts_user_id ON posts(user_id);
    `)

    migration.AddDownSQL(`
        DROP INDEX idx_users_email;
        DROP INDEX idx_posts_user_id;
    `)

    return migration
}
```

**详细文档：**
- [Migration 完整指南](.dev-docs/MIGRATION_GUIDE.md) - 深入了解所有功能
- [快速开始](.dev-docs/QUICK_START_MIGRATION.md) - 5分钟上手指南

## 🗄️ 支持的数据库

| 数据库 | 适配器 | 状态 |
|--------|--------|------|
| SQLite | sqlite | ✅ |
| MySQL | mysql | ✅ |
| PostgreSQL | postgres | ✅ |

## 📖 文档

- [Migration 工具完整指南](.dev-docs/MIGRATION_GUIDE.md) - 数据库迁移工具使用说明
- [Migration 快速开始](.dev-docs/QUICK_START_MIGRATION.md) - 5分钟上手迁移工具
- [动态建表功能详解](.dev-docs/DYNAMIC_TABLE.md) - SaaS 多租户、分表分库等场景
- [快速参考和常见问题](.dev-docs/QUICK_REFERENCE.md)
- [v0.1.4 版本修复说明和完整使用指南](.dev-docs/FIXES_AND_TESTS.md)  
- [版本发布说明](.dev-docs/SUMMARY.md)
- [v1.0.0 路线图](.dev-docs/v1.0.0_ROADMAP.md)
- [v0.3.0 开发进度](.dev-docs/v0.3.0-PROGRESS.md)

## ❓ 常见问题

### GetGormDB() 返回 nil

确保 Repository 已成功初始化。如果创建时返回错误，GetGormDB() 会返回 nil。

```go
repo, err := eit_db.NewRepository(config)
if err != nil {
    log.Fatal(err)
}

gormDB := repo.GetGormDB() // 现在返回有效实例
```

### PostgreSQL 连接失败

检查是否在使用信任认证。如果使用信任认证，确保密码字段为空字符串：

```go
config := &eit_db.Config{
    Adapter:   "postgres",
    Username:  "postgres",
    Password:  "", // 信任认证
    Database:  "myapp",
    SSLMode:   "disable",
}
```

### MySQL 连接失败

确保 MySQL 服务器正在运行，用户名和密码正确：

```go
config := &eit_db.Config{
    Adapter:   "mysql",
    Host:      "localhost",
    Port:      3306,
    Username:  "root",
    Password:  "password",
    Database:  "myapp",
}
```

## 🧪 测试

运行所有测试：

```bash
go test -v ./...
```

运行特定测试：

```bash
# Changeset 验证测试
go test -v -run TestValidate

# 适配器测试
go test -v -run TestSQLiteAdapterInitialization

# 动态表测试
go test -v -run TestDynamicTable
```

性能基准测试：

```bash
go test -bench=BenchmarkGetGormDB -benchmem
```

## 📊 版本更新

### v0.4.2 - SQL Server Adapter (2026-02-03)

**核心新增**：SQL Server 数据库支持，验证三层查询构造架构的扩展性

**SQLServerDialect 实现**
- ✅ 方括号标识符引用：`[table].[column]` 而非反引号或双引号
- ✅ @pN 参数占位符：`@p1`, `@p2` 而非 `?` 或 `$1`
- ✅ SQL Server 专属分页语法：`OFFSET n ROWS FETCH NEXT m ROWS ONLY`
- ✅ 完整的三层架构兼容性验证

**SQLServerAdapter 实现**
- ✅ 基于 github.com/microsoft/go-mssqldb 和 gorm.io/driver/sqlserver
- ✅ 默认端口 1433，支持连接池配置
- ✅ 完整的事务支持（Commit/Rollback/Exec/Query/QueryRow）
- ✅ GetQueryBuilderProvider() 返回 SQL Server 方言提供者

**测试覆盖**
- ✅ TestSQLServerDialect：5个测试用例验证 SQL 生成
- ✅ TestSQLServerIdentifierQuoting：方括号引用验证
- ✅ TestSQLServerComplexQuery：复杂查询验证
- ✅ TestSQLServerDialectQuotingComparison：跨方言对比测试
- ✅ 所有现有测试继续通过，100% 向后兼容

**架构验证**：SQL Server 的独特语法完美融入三层架构，证明设计的可扩展性

---

### v0.4.1 - 查询构造器三层架构 (2026-02-03)

**核心改进**：建立查询构造器的三层分离架构，为 v0.5.0+ 多 Adapter 支持打基础

**顶层 - 用户 API 层**
- ✅ `QueryConstructor` 接口：用户通过此接口构建查询
- ✅ 流式 API：`Where()`, `WhereAll()`, `WhereAny()`, `Select()`, `OrderBy()`, `Limit()`, `Offset()`
- ✅ 灵活的条件构造器：`Eq()`, `Ne()`, `Gt()`, `Lt()`, `Gte()`, `Lte()`, `In()`, `Between()`, `Like()`
- ✅ 复合条件：`And()`, `Or()`, `Not()`

**中层 - Adapter 转义层**
- ✅ `QueryConstructorProvider` 接口：每个 Adapter 通过此接口提供数据库特定的实现
- ✅ `QueryBuilderCapabilities` 结构体：声明 Adapter 支持的操作和优化特性
- ✅ 方言无关的 API 设计

**底层 - 数据库执行层**
- ✅ `SQLQueryConstructor` 实现：标准 SQL 生成
- ✅ `SQLDialect` 接口：支持不同的 SQL 方言
- ✅ 方言实现：`MySQLDialect`, `PostgreSQLDialect`, `SQLiteDialect`
- ✅ 参数化查询：防止 SQL 注入，自动转换为 `?` 或 `$1` 等占位符

**测试覆盖**：20+ 单元测试，验证每个条件、操作符和组合的 SQL 生成正确性  

### v0.4.0 - Migration 工具 (2026-02-03)

✅ 全新的数据库迁移工具  
✅ 支持 Schema-based 和 Raw SQL 两种迁移方式  
✅ 命令行工具 eit-migrate  
✅ 自动版本管理和状态追踪  
✅ 支持跨数据库和非关系型数据库  

### v0.3.1 - Changeset 增强 (2026-02-03)

✅ 新增 7 个验证方法（Required, Length, Format, Inclusion, Exclusion, Number, GetChange）  
✅ 完整的测试套件  
✅ 修复 TestDynamicTableConfigBuilder 测试  

### v0.1.4 - 稳定性修复 (2026-02-02)

✅ 修复 MySQL 驱动 GetGormDB() 返回 nil 问题  
✅ 修复 PostgreSQL 认证 "role does not exist" 问题  
✅ 改进连接池配置，完整支持 MaxLifetime  
✅ 增强错误诊断信息，包含完整的连接参数  
✅ 添加完整的测试套件（10+ 测试用例）  
✅ 100% 向后兼容  

详见 [版本修复说明](.dev-docs/FIXES_AND_TESTS.md)

## 🔗 相关链接

- [GORM 文档](https://gorm.io)
- [Elixir Ecto 文档](https://hexdocs.pm/ecto)
- [GitHub Repository](https://github.com/deathcodebind/eit-db)
- [适配器工作流文档](./.dev-docs/ADAPTER_WORKFLOW.md)
- [测试覆盖范围](./.dev-docs/TEST_COVERAGE.md)

## 🧪 测试

### 单元测试

运行核心库测试：

```bash
go test ./... -v
```

### 集成测试

测试所有适配器（SQLite 无需依赖，PostgreSQL/MySQL 需要 Docker）：

```bash
# 仅 SQLite 测试（推荐开发期间使用）
go test ./adapter-application-tests -v

# 或使用测试脚本
./test.sh integration

# 完整测试（启动所有数据库 + 运行测试）
./test.sh all-keep
```

### 使用 Docker 运行完整测试

```bash
# 启动 PostgreSQL、MySQL、SQL Server 容器
./test.sh start

# 运行所有测试
./test.sh integration

# 停止容器
./test.sh stop

# 或一步完成
./test.sh all
```

### 测试覆盖范围

详见 [测试覆盖范围文档](./.dev-docs/TEST_COVERAGE.md)

**已验证的功能：**

- ✅ SQLite: CRUD、CTE、窗口函数、JSON、事务、UPSERT
- ✅ 多适配器管理：反射注册、YAML 配置、工厂模式
- ✅ QueryFeatures：版本感知、优先级路由、特性声明
- ⏭️ PostgreSQL：物化视图、数组、全文搜索、JSONB
- ⏭️ MySQL：全文搜索、JSON、窗口函数、ON DUPLICATE KEY
- ⏭️ SQL Server：MERGE、递归 CTE、临时表

## 📝 许可证

MIT License

## 📧 支持

如有问题或建议，欢迎提交 Issue 或 Pull Request。

---

**最后更新**：2026-02-04  
**当前版本**：v0.4.2  
**下一版本**：v0.5.0 (多适配器+集成测试完成)
