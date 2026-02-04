# Repository 反射功能使用指南

## 概述

eit-db 现已支持通过反射自动从 Go struct 推导 Schema，以及自动将查询结果映射到结构体。这大大简化了代码编写，避免了手动的字段映射和扫描。

## 核心功能

### 1. 从 Struct 推导 Schema

```go
// 定义结构体，使用 db tag 标注
type User struct {
    ID        int       `db:"id,primary_key,auto_increment"`
    Username  string    `db:"username,not_null,unique"`
    Email     string    `db:"email,not_null,unique"`
    Age       *int      `db:"age"`  // 指针表示可空字段
    IsActive  bool      `db:"is_active,not_null"`
    CreatedAt time.Time `db:"created_at"`
}

// 自动推导 Schema
schema, err := db.InferSchema(User{})
// schema.TableName() => "user"
// schema.Fields() => [id, username, email, age, is_active, created_at]
```

### 2. DB Tag 语法

```go
`db:"column_name,option1,option2,..."`
```

**支持的选项：**
- `primary_key` / `primarykey` / `pk` - 主键
- `not_null` / `notnull` - 非空
- `unique` - 唯一约束
- `index` - 索引
- `auto_increment` / `autoincrement` - 自增
- `-` - 忽略此字段（不映射到数据库）

**示例：**
```go
type Product struct {
    ID          int     `db:"id,primary_key,auto_increment"`
    SKU         string  `db:"sku,not_null,unique,index"`
    Name        string  `db:"name,not_null"`
    Price       float64 `db:"price,not_null"`
    Stock       int     `db:"stock"`
    IsAvailable bool    `db:"is_available,not_null"`
    Internal    string  `db:"-"` // 不映射到数据库
}
```

### 3. 自动类型映射

Go 类型会自动映射到 FieldType：

| Go 类型 | FieldType |
|---------|-----------|
| string | TypeString |
| int, int32, int64, uint, uint32, uint64 | TypeInteger |
| float32, float64 | TypeFloat |
| bool | TypeBoolean |
| time.Time | TypeTime |
| []byte | TypeBinary |
| []T | TypeArray |
| map[K]V | TypeMap |
| struct | TypeJSON |

**可空类型支持：**
```go
type User struct {
    Name  string  `db:"name,not_null"`
    Age   *int    `db:"age"`            // 使用指针表示可空
    Email *string `db:"email"`          // 可空
}

// 或使用 sql.Null* 类型
type User struct {
    Name  string         `db:"name,not_null"`
    Age   sql.NullInt64  `db:"age"`
    Email sql.NullString `db:"email"`
}
```

### 4. QueryStruct - 查询单个结构体

```go
type User struct {
    ID       int    `db:"id,primary_key"`
    Username string `db:"username"`
    Email    string `db:"email"`
}

// 查询单个用户
var user User
err := repo.QueryStruct(ctx, &user, 
    "SELECT id, username, email FROM users WHERE id = ?", 1)
if err != nil {
    log.Fatal(err)
}

fmt.Printf("User: %+v\n", user)
// Output: User: {ID:1 Username:john Email:john@example.com}
```

### 5. QueryStructs - 查询多个结构体

```go
// 查询多个用户
var users []User
err := repo.QueryStructs(ctx, &users, 
    "SELECT id, username, email FROM users WHERE is_active = ?", true)
if err != nil {
    log.Fatal(err)
}

for _, user := range users {
    fmt.Printf("%d: %s (%s)\n", user.ID, user.Username, user.Email)
}
```

### 6. 辅助函数

#### GetStructFields - 获取字段名列表

```go
type User struct {
    ID       int    `db:"id"`
    Username string `db:"username"`
    Email    string `db:"email"`
}

fields := db.GetStructFields(User{})
// fields => ["id", "username", "email"]

// 用于构建 SQL
sql := fmt.Sprintf("SELECT %s FROM users", strings.Join(fields, ", "))
```

#### GetStructValues - 获取字段值列表

```go
user := User{ID: 1, Username: "john", Email: "john@example.com"}
values := db.GetStructValues(user)
// values => [1, "john", "john@example.com"]

// 用于参数化查询
placeholders := []string{"?", "?", "?"}
sql := fmt.Sprintf("INSERT INTO users VALUES (%s)", strings.Join(placeholders, ", "))
repo.Exec(ctx, sql, values...)
```

## 完整示例

### 示例 1：CRUD 操作

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"
    
    "github.com/eit-cms/eit-db"
)

type Article struct {
    ID        int       `db:"id,primary_key,auto_increment"`
    Title     string    `db:"title,not_null"`
    Content   string    `db:"content,not_null"`
    Author    string    `db:"author,not_null"`
    ViewCount int       `db:"view_count"`
    Published bool      `db:"published,not_null"`
    CreatedAt time.Time `db:"created_at"`
}

func main() {
    // 创建 Repository
    repo, err := db.NewRepository(&db.Config{
        Adapter:  "sqlite",
        Database: "articles.db",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer repo.Close()

    ctx := context.Background()

    // 从 struct 推导 schema
    schema, _ := db.InferSchema(Article{})
    fmt.Printf("Table: %s\n", schema.TableName())
    
    // 创建表（实际中可以使用 migration）
    createTable(repo, ctx)

    // 插入文章
    insertArticle(repo, ctx, "Go 反射教程", "学习 Go 反射...", "Alice")
    insertArticle(repo, ctx, "数据库设计", "数据库设计最佳实践...", "Bob")

    // 查询单篇文章
    var article Article
    err = repo.QueryStruct(ctx, &article, 
        "SELECT * FROM article WHERE id = ?", 1)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Article: %+v\n", article)

    // 查询所有文章
    var articles []Article
    err = repo.QueryStructs(ctx, &articles, 
        "SELECT * FROM article ORDER BY created_at DESC")
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Printf("Total articles: %d\n", len(articles))
    for _, a := range articles {
        fmt.Printf("- %s by %s\n", a.Title, a.Author)
    }
}

func createTable(repo *db.Repository, ctx context.Context) {
    sql := `
        CREATE TABLE IF NOT EXISTS article (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            content TEXT NOT NULL,
            author TEXT NOT NULL,
            view_count INTEGER DEFAULT 0,
            published INTEGER DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    `
    repo.Exec(ctx, sql)
}

func insertArticle(repo *db.Repository, ctx context.Context, title, content, author string) {
    sql := `INSERT INTO article (title, content, author, view_count, published, created_at) 
            VALUES (?, ?, ?, ?, ?, ?)`
    repo.Exec(ctx, sql, title, content, author, 0, 1, time.Now())
}
```

### 示例 2：与 Changeset 结合

```go
type User struct {
    ID       int    `db:"id,primary_key,auto_increment"`
    Username string `db:"username,not_null,unique"`
    Email    string `db:"email,not_null,unique"`
    Age      *int   `db:"age"`
}

// 从 struct 推导 schema
schema, _ := db.InferSchema(User{})

// 创建 changeset 进行验证
user := User{Username: "john", Email: "john@example.com"}
changeset := db.NewChangeset(user, schema)
changeset.ValidateRequired("username", "email")
changeset.ValidateLength("username", 3, 20)

if !changeset.IsValid() {
    fmt.Println("Validation errors:", changeset.Errors())
    return
}

// 使用 QueryStruct 查询
var savedUser User
repo.QueryStruct(ctx, &savedUser, 
    "SELECT * FROM user WHERE username = ?", "john")
```

### 示例 3：处理可空字段

```go
type Profile struct {
    ID        int     `db:"id,primary_key"`
    UserID    int     `db:"user_id,not_null"`
    Bio       *string `db:"bio"`        // 可空
    Website   *string `db:"website"`    // 可空
    AvatarURL *string `db:"avatar_url"` // 可空
}

// 插入时
bio := "Software engineer"
profile := Profile{
    UserID:  1,
    Bio:     &bio,
    Website: nil, // 空值
}

// 查询时自动处理 NULL
var profiles []Profile
repo.QueryStructs(ctx, &profiles, "SELECT * FROM profile")

for _, p := range profiles {
    if p.Bio != nil {
        fmt.Println("Bio:", *p.Bio)
    }
    if p.Website != nil {
        fmt.Println("Website:", *p.Website)
    }
}
```

## 向后兼容

所有新功能都是**可选的**。现有代码无需修改：

```go
// 老方法仍然有效
rows, err := repo.Query(ctx, "SELECT * FROM users")
defer rows.Close()
for rows.Next() {
    var id int
    var name string
    rows.Scan(&id, &name)
    // ...
}

// 新方法更简洁
var users []User
repo.QueryStructs(ctx, &users, "SELECT * FROM users")
```

## 性能说明

- **反射开销**：InferSchema 只需调用一次，可以缓存结果
- **扫描性能**：ScanStruct/ScanStructs 与手动 Scan 性能相近
- **内存分配**：自动管理，无额外内存泄漏风险

## 最佳实践

1. **定义一次，到处使用**
```go
// 在包级别定义 schema
var userSchema, _ = db.InferSchema(User{})

// 在多处使用
func getUser(id int) User { /* ... */ }
func listUsers() []User { /* ... */ }
```

2. **使用指针表示可空**
```go
// ✅ 好的做法
type User struct {
    Age *int `db:"age"`
}

// ❌ 避免这样（无法区分 0 和 NULL）
type User struct {
    Age int `db:"age"` // 0 还是 NULL?
}
```

3. **Tag 命名规范**
```go
// ✅ 推荐：使用 snake_case
type User struct {
    FirstName string `db:"first_name"`
}

// ⚠️ 不推荐但支持：驼峰会自动转换
type User struct {
    FirstName string `db:"FirstName"` // 自动转为 first_name
}

// ✅ 最简洁：省略 tag，自动推导
type User struct {
    FirstName string // 自动推导为 first_name
}
```

## 常见问题

**Q: 如何映射非标准列名？**
```go
type Legacy struct {
    ID int `db:"UserID"`  // 映射到 UserID 列
}
```

**Q: 如何忽略某些字段？**
```go
type User struct {
    Password string `db:"-"`  // 不映射到数据库
    Internal string `db:"-"`
}
```

**Q: 支持嵌套结构体吗？**
```go
// 当前版本：嵌套结构体会被序列化为 JSON
type User struct {
    Profile struct {
        Bio string
    } `db:"profile"` // 存储为 JSON
}
```

**Q: 如何与事务结合？**
```go
tx, _ := repo.Begin(ctx)
defer tx.Rollback()

var users []User
// 事务中也可以使用 QueryStructs
repo.QueryStructs(ctx, &users, "SELECT * FROM users FOR UPDATE")

tx.Commit()
```
