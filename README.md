# EIT-DB - Go 数据库抽象层

一个受 Ecto (Elixir) 启发的 Go 数据库抽象层，提供类型安全的 Schema、Changeset 和跨数据库适配器支持。

## ✨ 特性

- **Schema 系统** - 声明式数据结构定义，支持验证器和默认值
- **Changeset** - 数据变更追踪和验证，类似 Ecto.Changeset  
- **跨数据库适配器** - 支持 MySQL, PostgreSQL, SQLite
- **查询构建器** - 类型安全的查询接口
- **迁移系统** - 自动从 Schema 生成数据库迁移
- **GORM 集成** - 完全兼容 GORM v1/v2，可无缝协作

## 📦 安装

```bash
go get pathologyenigma/eit-db
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

**或使用代码配置：**

```go
package main

import "pathologyenigma/eit-db"

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

## 🗄️ 支持的数据库

| 数据库 | 适配器 | 状态 |
|--------|--------|------|
| SQLite | sqlite | ✅ |
| MySQL | mysql | ✅ |
| PostgreSQL | postgres | ✅ |

## 📖 文档

- [快速参考和常见问题](.dev-docs/QUICK_REFERENCE.md)
- [v0.1.4 版本修复说明和完整使用指南](.dev-docs/FIXES_AND_TESTS.md)  
- [版本发布说明](.dev-docs/SUMMARY.md)

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
go test -v
```

运行特定测试：

```bash
go test -v -run TestSQLiteAdapterInitialization
```

性能基准测试：

```bash
go test -bench=BenchmarkGetGormDB -benchmem
```

## 📊 v0.1.4 版本更新

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

## 📝 许可证

MIT License

## 📧 支持

如有问题或建议，欢迎提交 Issue 或 Pull Request。

---

**最后更新**：2026-02-02  
**当前版本**：v0.1.4
    for field, errors := range cs.Errors() {
        fmt.Printf("%s: %v\n", field, errors)
    }
}
```

### 3. 查询构建器

```go
// 初始化适配器
repo, _ := db.InitFromConfig("./config/database.yaml")

// 构建查询
qb := db.NewQueryBuilder(schema, repo)
result := qb.Query("email = ?", "user@example.com")

// 插入数据
cs := db.NewChangeset(schema).Cast(data).Validate()
qb.Insert(cs)

// 更新数据
updates := map[string]interface{}{"email": "new@example.com"}
cs := db.NewChangeset(schema).Cast(updates)
qb.Update(cs, "id = ?", userID)
```

### 4. 数据库迁移

```go
// 自动从 Schema 生成迁移
schemas := []db.Schema{BuildUserSchema(), BuildPostSchema()}
migrator := db.NewMigrator(repo)
migrator.AutoMigrate(schemas)
```

## 架构

EIT-DB 采用三层架构:

1. **Schema 层**: 定义数据结构和验证规则
2. **Changeset 层**: 管理数据变更和验证
3. **Adapter 层**: 抽象不同数据库的操作

这种设计使得你可以:
- 在不同数据库间轻松切换
- 在业务层使用统一的 API
- 轻松添加自定义验证器
- 保持代码的可测试性

## 支持的数据库

- MySQL 5.7+
- PostgreSQL 10+
- SQLite 3+

## 文档

详细文档请查看 [docs](./docs) 目录:

- Schema 定义指南
- Changeset 使用指南
- 查询构建器 API
- 自定义适配器开发

## License

MIT License
