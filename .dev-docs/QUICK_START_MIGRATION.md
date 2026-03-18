# Quick Start Example

本示例演示如何使用新的 migration 系统。

命令入口说明：

- 推荐使用 `eit-db-cli`
- `eit-migrate` 为兼容别名，旧脚本可继续运行
- 新功能不会继续为旧别名单独适配

## 1. 初始化项目

```bash
cd /tmp
mkdir test-migration && cd test-migration
/Users/huyingjie/dev/go/eit-db/bin/eit-db-cli init
```

## 2. 配置数据库

```bash
cd migrations
cp .env.example .env
```

编辑 `.env`:
```env
DB_ADAPTER=sqlite
DB_HOST=
DB_PORT=0
DB_NAME=test.db
DB_USER=
DB_PASSWORD=
```

## 3. 生成第一个迁移

```bash
cd /tmp/test-migration
/Users/huyingjie/dev/go/eit-db/bin/eit-db-cli generate create_users_table
```

## 4. 编辑迁移文件

生成的文件会在 `migrations/` 目录中，例如 `migrations/20260203150405_create_users_table.go`

编辑该文件，使用 Schema 定义表结构：

```go
package main

import (
    "context"
    db "github.com/eit-cms/eit-db"
)

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

    migration.CreateTable(userSchema)

    return migration
}
```

## 5. 运行迁移

```bash
cd migrations
go mod init migrations
go mod tidy
source .env
go run . up
```

## 6. 查看状态

```bash
go run . status
```

## 7. 生成 SQL 迁移

```bash
cd /tmp/test-migration
/Users/huyingjie/dev/go/eit-db/bin/eit-db-cli generate add_posts_table --type sql
```

编辑生成的文件，使用原始 SQL：

```go
package main

import (
    db "github.com/eit-cms/eit-db"
)

func NewMigration_20260203160000_AddPostsTable() db.MigrationInterface {
    migration := db.NewRawSQLMigration("20260203160000", "add_posts_table")

    migration.AddUpSQL(`
        CREATE TABLE posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            content TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    `)

    migration.AddDownSQL(`DROP TABLE posts`)

    return migration
}
```

## 8. 再次运行迁移

```bash
cd migrations
go run . up
```

## 9. 回滚迁移

```bash
go run . down
```

## 特性演示

### Schema-based Migration 的优势
- 类型安全
- 跨数据库兼容
- 与 eit-db Schema 系统集成

### Raw SQL Migration 的优势
- 完全控制 SQL
- 支持特定数据库特性（如触发器、函数）
- 可以为不同 adapter 编写不同的 SQL

### 混合使用
可以在同一个项目中同时使用两种方式，根据需要选择最合适的方式。
