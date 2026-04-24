# EIT-DB Migration Tool 使用指南

## 概述

eit-db-cli 是当前主命令入口，提供灵活的数据库迁移工具能力，支持两种迁移方式：
1. **Schema-based Migration** - 基于 eit-db Schema 的迁移
2. **Raw SQL Migration** - 原始 SQL 迁移（支持特定 adapter）

兼容策略：

- `eit-migrate` 作为历史别名继续兼容。
- 新功能将优先并持续在 `eit-db-cli` 下演进，旧别名仅保证兼容可用。

## 自定义 Adapter 注册迁移（v1.1+）

如果你维护的是第三方适配器，建议从旧式注册迁移到描述符注册，并显式声明 Metadata。

推荐迁移步骤：

1. 将 `RegisterAdapter` / `RegisterAdapterConstructor` 替换为 `MustRegisterAdapterDescriptor`
2. 将配置校验逻辑收敛到 `ValidateConfig`
3. 将默认配置收敛到 `DefaultConfig`
4. 增加 `Metadata`，声明 name/driverKind/vendor/version

示例：

```go
func init() {
    db.MustRegisterAdapterDescriptor("mydb", db.AdapterDescriptor{
        Factory: func(cfg *db.Config) (db.Adapter, error) {
            a, err := NewMyAdapter(cfg)
            if err != nil {
                return nil, err
            }
            if err := a.Connect(context.Background(), cfg); err != nil {
                return nil, err
            }
            return a, nil
        },
        ValidateConfig: func(cfg *db.Config) error {
            return nil
        },
        DefaultConfig: func() *db.Config {
            return &db.Config{Adapter: "mydb"}
        },
        Metadata: func() db.AdapterMetadata {
            return db.AdapterMetadata{
                Name:       "mydb",
                DriverKind: "sql", // sql | document | graph | kv
                Vendor:     "acme",
            }
        },
    })
}
```

迁移后，你可以通过统一 API 读取元信息：

```go
meta := db.ResolveAdapterMetadata("", adapter)
meta2 := repo.GetAdapterMetadata()
```

## 快速开始

### 1. 初始化迁移项目

```bash
eit-db-cli init
```

这会创建以下结构：
```
migrations/
├── main.go          # 迁移运行器入口
├── .env.example     # 环境变量示例
└── README.md        # 使用说明
```

### 2. 配置数据库连接

```bash
cd migrations
cp .env.example .env
# 编辑 .env 文件配置数据库连接
```

### 3. 生成迁移文件

**基于 Schema 的迁移：**
```bash
eit-db-cli generate create_users_table
```

**原始 SQL 迁移：**
```bash
eit-db-cli generate create_users_table --type sql
```

### 4. 编辑迁移文件

生成的迁移文件示例：

**Schema-based Migration:**
```go
package main

import (
    "context"
    db "github.com/eit-cms/eit-db"
)

func NewMigration_20260203150405_CreateUsersTable() db.MigrationInterface {
    migration := db.NewSchemaMigration("20260203150405", "create_users_table")

    // 定义 users 表的 schema
    userSchema := db.NewBaseSchema("users")
    userSchema.AddField(&db.Field{
        Name:    "id",
        Type:    db.TypeInteger,
        Primary: true,
        Autoinc: true,
    })
    userSchema.AddField(&db.Field{
        Name: "name",
        Type: db.TypeString,
        Null: false,
    })
    userSchema.AddField(&db.Field{
        Name:   "email",
        Type:   db.TypeString,
        Null:   false,
        Unique: true,
    })
    userSchema.AddField(&db.Field{
        Name:    "created_at",
        Type:    db.TypeTime,
        Default: "CURRENT_TIMESTAMP",
    })

    migration.CreateTable(userSchema)

    return migration
}
```

**Raw SQL Migration:**
```go
package main

import (
    db "github.com/eit-cms/eit-db"
)

func NewMigration_20260203150405_CreateUsersTable() db.MigrationInterface {
    migration := db.NewRawSQLMigration("20260203150405", "create_users_table")

    migration.AddUpSQL(`
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            email VARCHAR(255) NOT NULL UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    `)

    migration.AddDownSQL(`DROP TABLE users`)

    // 可选：指定特定的 adapter
    // migration.ForAdapter("postgres")

    return migration
}
```

### 5. 运行迁移

```bash
cd migrations
source .env  # 加载环境变量
go run . up
```

### 6. 回滚迁移

```bash
go run . down
```

### 7. 查看迁移状态

```bash
go run . status
```

输出示例：
```
Migration Status:
================
[✓] 20260203150405 - create_users_table (applied at 2026-02-03 15:05:30)
[✓] 20260203160000 - add_posts_table (applied at 2026-02-03 16:01:15)
[ ] 20260203170000 - add_comments_table
```

## 高级用法

### 复杂的 Schema 迁移

```go
func NewMigration_20260203160000_AddPostsTable() db.MigrationInterface {
    migration := db.NewSchemaMigration("20260203160000", "add_posts_table")

    postSchema := db.NewBaseSchema("posts")
    postSchema.AddField(&db.Field{
        Name:    "id",
        Type:    db.TypeInteger,
        Primary: true,
        Autoinc: true,
    })
    postSchema.AddField(&db.Field{
        Name: "user_id",
        Type: db.TypeInteger,
        Null: false,
        Index: true,  // 添加索引
    })
    postSchema.AddField(&db.Field{
        Name: "title",
        Type: db.TypeString,
        Null: false,
    })
    postSchema.AddField(&db.Field{
        Name: "content",
        Type: db.TypeString,
    })
    postSchema.AddField(&db.Field{
        Name:    "status",
        Type:    db.TypeString,
        Default: "'draft'",
    })
    postSchema.AddField(&db.Field{
        Name:    "published_at",
        Type:    db.TypeTime,
    })
    postSchema.AddField(&db.Field{
        Name:    "created_at",
        Type:    db.TypeTime,
        Default: "CURRENT_TIMESTAMP",
    })

    migration.CreateTable(postSchema)

    return migration
}
```

### 多表迁移

```go
func NewMigration_20260203170000_AddMultipleTables() db.MigrationInterface {
    migration := db.NewSchemaMigration("20260203170000", "add_multiple_tables")

    // 创建 categories 表
    categorySchema := db.NewBaseSchema("categories")
    categorySchema.AddField(&db.Field{Name: "id", Type: db.TypeInteger, Primary: true, Autoinc: true})
    categorySchema.AddField(&db.Field{Name: "name", Type: db.TypeString, Null: false})
    migration.CreateTable(categorySchema)

    // 创建 tags 表
    tagSchema := db.NewBaseSchema("tags")
    tagSchema.AddField(&db.Field{Name: "id", Type: db.TypeInteger, Primary: true, Autoinc: true})
    tagSchema.AddField(&db.Field{Name: "name", Type: db.TypeString, Null: false})
    migration.CreateTable(tagSchema)

    // 创建关联表
    postTagSchema := db.NewBaseSchema("post_tags")
    postTagSchema.AddField(&db.Field{Name: "post_id", Type: db.TypeInteger, Null: false})
    postTagSchema.AddField(&db.Field{Name: "tag_id", Type: db.TypeInteger, Null: false})
    migration.CreateTable(postTagSchema)

    return migration
}
```

### 混合 Schema 和 Raw SQL

对于复杂的约束或触发器，可以混合使用：

```go
// 先用 Schema 创建表
func NewMigration_20260203180000_CreateOrdersTable() db.MigrationInterface {
    // 创建一个自定义的 migration 结构
    return &CustomOrdersMigration{
        BaseMigration: db.NewBaseMigration("20260203180000", "create_orders_table"),
    }
}

type CustomOrdersMigration struct {
    *db.BaseMigration
}

func (m *CustomOrdersMigration) Up(ctx context.Context, repo *db.Repository) error {
    // 1. 使用 Schema 创建基础表
    orderSchema := db.NewBaseSchema("orders")
    orderSchema.AddField(&db.Field{Name: "id", Type: db.TypeInteger, Primary: true, Autoinc: true})
    orderSchema.AddField(&db.Field{Name: "total", Type: db.TypeDecimal, Null: false})
    
    if err := repo.CreateTable(ctx, orderSchema); err != nil {
        return err
    }

    // 2. 使用 Raw SQL 添加复杂约束
    if err := repo.Exec(ctx, `
        ALTER TABLE orders 
        ADD CONSTRAINT check_total_positive 
        CHECK (total > 0)
    `); err != nil {
        return err
    }

    // 3. 创建触发器
    if err := repo.Exec(ctx, `
        CREATE TRIGGER update_order_timestamp 
        BEFORE UPDATE ON orders 
        FOR EACH ROW 
        EXECUTE FUNCTION update_updated_at_column()
    `); err != nil {
        return err
    }

    return nil
}

func (m *CustomOrdersMigration) Down(ctx context.Context, repo *db.Repository) error {
    return repo.DropTable(ctx, "orders")
}
```

### 特定数据库的 SQL

```go
func NewMigration_20260203190000_AddFullTextSearch() db.MigrationInterface {
    migration := db.NewRawSQLMigration("20260203190000", "add_full_text_search")

    // PostgreSQL 特定的全文搜索
    migration.AddUpSQL(`
        ALTER TABLE posts 
        ADD COLUMN search_vector tsvector
    `)
    
    migration.AddUpSQL(`
        CREATE INDEX posts_search_idx 
        ON posts 
        USING GIN(search_vector)
    `)

    migration.AddDownSQL(`DROP INDEX IF EXISTS posts_search_idx`)
    migration.AddDownSQL(`ALTER TABLE posts DROP COLUMN IF EXISTS search_vector`)

    // 指定只在 PostgreSQL 上运行
    migration.ForAdapter("postgres")

    return migration
}
```

## 最佳实践

1. **迁移文件命名**：使用描述性的名称，如 `create_users_table`、`add_email_to_users`
2. **一个迁移一个目的**：每个迁移只做一件事，便于回滚
3. **总是写 Down**：确保每个迁移都可以回滚
4. **测试迁移**：在开发环境测试 `up` 和 `down` 都能正常工作
5. **不要修改已应用的迁移**：创建新的迁移来修改 schema
6. **使用事务**：Repository 的操作会自动包装在事务中
7. **版本控制**：将迁移文件纳入版本控制

## 与非关系型数据库一起使用

对于非关系型数据库（如 MongoDB、Elasticsearch），使用 Raw SQL 方式并指定 adapter：

```go
func NewMigration_20260203200000_CreateElasticsearchIndex() db.MigrationInterface {
    migration := db.NewRawSQLMigration("20260203200000", "create_elasticsearch_index")

    migration.AddUpSQL(`
        {
            "mappings": {
                "properties": {
                    "title": { "type": "text" },
                    "content": { "type": "text" },
                    "created_at": { "type": "date" }
                }
            }
        }
    `)

    migration.ForAdapter("elasticsearch")

    return migration
}
```

注意：非关系型数据库的错误处理由数据库客户端返回。

## 故障排查

### 迁移失败后如何恢复？

如果迁移失败，数据库会回滚该迁移，但迁移记录可能已经写入。手动清理：

```sql
DELETE FROM schema_migrations WHERE version = '20260203150405';
```

### 如何重置所有迁移？

```sql
DROP TABLE schema_migrations;
```

然后重新运行 `go run . up`

### 如何跳过某个迁移？

手动添加记录到 schema_migrations 表：

```sql
INSERT INTO schema_migrations (version) VALUES ('20260203150405');
```

## 命令参考

| 命令 | 描述 |
|------|------|
| `eit-db-cli init` | 初始化迁移项目 |
| `eit-db-cli generate <name>` | 生成新的 Schema 迁移 |
| `eit-db-cli generate <name> --type sql` | 生成新的 SQL 迁移 |
| `eit-db-cli version` | 显示工具版本 |

兼容别名（不建议新文档继续使用）：

- `eit-migrate init`
- `eit-migrate generate <name>`
- `eit-migrate generate <name> --type sql`
- `eit-migrate version`

在 migrations 目录中：
| 命令 | 描述 |
|------|------|
| `go run . up` | 运行所有待执行的迁移 |
| `go run . down` | 回滚最后一个迁移 |
| `go run . status` | 显示迁移状态 |
