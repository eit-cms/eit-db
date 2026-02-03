# Relationship Support Design (v0.5.0+)

## 概述

EIT-DB 的关系支持采用**能力声明 + 自适应策略**的设计，让各种数据库（关系型、非关系型、图数据库）都能以统一的 API 表达数据之间的关系。

## 核心设计原则

### 1. **Adapter 能力声明**

每个 Adapter 声明它支持的关系类型：

```go
type RelationshipSupport struct {
    OneToOne   bool                    // 一对一
    OneToMany  bool                    // 一对多
    ManyToMany bool                    // 多对多
    
    SupportsForeignKey bool            // 是否支持外键
    SupportsJoin       bool            // 是否支持 JOIN
    SupportsNested     bool            // 是否支持嵌套查询
    
    Strategy RelationshipStrategy      // 实现策略
}
```

### 2. **关系定义方式**

在 Schema 中定义关系（类似 Rails 的 ActiveRecord）：

```go
// 定义用户和文章的关系
userSchema := db.NewBaseSchema("users")
relationships := db.NewSchemaRelationshipBuilder(userSchema).
    HasMany("articles", articleSchema, "user_id", "id").
    HasAndBelongsToMany("roles", roleSchema, "user_roles", "user_id", "role_id")

user.Relationships = relationships.GetRelationships()
```

### 3. **自适应策略**

根据 Adapter 的能力自动调整实现策略：

| 数据库 | OneToOne | OneToMany | ManyToMany | 策略 |
|--------|----------|-----------|------------|------|
| PostgreSQL | ✅ | ✅ | ✅ | Native (外键) |
| SQL Server | ✅ | ✅ | ✅ | Native (外键) |
| MySQL | ✅ | ✅ | ❌ → ✅ | JoinTable (中间表) |
| SQLite | ✅ | ✅ | ❌ → ✅ | JoinTable (中间表) |
| MongoDB | ✅ | ✅ | ✅ | Application (ObjectID 数组) |
| 图数据库 | ✅ | ✅ | ✅ | Native (边) |
| 缓存数据库 | ❌ | ❌ | ❌ | NotSupported |

## 实现策略详解

### 策略 1: 原生支持 (StrategyNative)

**应用场景**: PostgreSQL, SQL Server, 图数据库

```sql
-- PostgreSQL 中的外键
CREATE TABLE articles (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id)
);

-- 多对多通过约束
CREATE TABLE user_roles (
    user_id INTEGER REFERENCES users(id),
    role_id INTEGER REFERENCES roles(id),
    PRIMARY KEY (user_id, role_id)
);
```

**查询方式**:
```go
// 直接 JOIN
users, _ := repo.Query(userSchema).
    Join("articles", "user_id", "id").
    All(ctx)
```

### 策略 2: 中间表模拟 (StrategyJoinTable)

**应用场景**: MySQL, SQLite 不支持多对多

当数据库支持一对多但不支持多对多时，自动创建中间表：

```go
// 用户定义多对多关系
relationships.HasAndBelongsToMany("roles", roleSchema, "user_roles", "user_id", "role_id")

// Adapter 自动:
// 1. 创建中间表 user_roles
// 2. 创建两个一对多关系
// 3. 通过中间表查询关联数据
```

**自动创建的表**:
```sql
CREATE TABLE user_roles (
    user_id INTEGER NOT NULL,
    role_id INTEGER NOT NULL,
    PRIMARY KEY (user_id, role_id),
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (role_id) REFERENCES roles(id)
);
```

### 策略 3: 应用层实现 (StrategyApplication)

**应用场景**: MongoDB, Elasticsearch 等非关系型数据库

```go
// MongoDB 中存储引用
{
    "_id": ObjectId("..."),
    "name": "Alice",
    "article_ids": [ObjectId("..."), ObjectId("...")]  // 数组引用
}

// 查询时由应用层进行 lookup
```

**查询方式**:
```go
// Repository 自动处理应用层 JOIN
// 底层调用 MongoDB lookup 或多次查询
users, _ := repo.Query(userSchema).
    WithRelation("articles", articleSchema).  // 标记需要加载关联
    All(ctx)
```

### 策略 4: 完全不支持 (StrategyNotSupported)

**应用场景**: 纯缓存数据库、事件流等

```go
// 禁用关系操作
validator := db.NewRelationshipValidator(support)

if !validator.CanJoin() {
    // 返回错误或建议
    return fmt.Errorf("adapter does not support JOIN operations")
}
```

## API 使用示例

### 定义关系

```go
// 创建 Schemas
userSchema := db.NewBaseSchema("users")
articleSchema := db.NewBaseSchema("articles")
roleSchema := db.NewBaseSchema("roles")

// 定义字段
userSchema.AddField(&db.Field{
    Name: "id",
    Type: db.TypeInteger,
    Primary: true,
})
userSchema.AddField(&db.Field{
    Name: "name",
    Type: db.TypeString,
})

articleSchema.AddField(&db.Field{
    Name: "id",
    Type: db.TypeInteger,
    Primary: true,
})
articleSchema.AddField(&db.Field{
    Name: "user_id",
    Type: db.TypeInteger,  // 外键
})

// 定义关系
relBuilder := db.NewSchemaRelationshipBuilder(userSchema)
relBuilder.
    HasMany("articles", articleSchema, "user_id", "id").           // 一对多
    HasOne("profile", profileSchema, "user_id", "id").             // 一对一
    HasAndBelongsToMany("roles", roleSchema, "user_roles", "user_id", "role_id")  // 多对多

userSchema.Relationships = relBuilder.GetRelationships()
```

### 创建关系

```go
// 迁移中创建关系
func NewMigration_CreateUserArticles() db.MigrationInterface {
    migration := db.NewSchemaMigration("20260203000000", "create_user_articles")
    
    // 定义关系
    rel := &db.Relationship{
        Name: "articles",
        FromSchema: userSchema,
        ToSchema: articleSchema,
        Type: db.OneToMany,
        ForeignKey: &db.ForeignKeyDef{
            FromColumn: "id",
            ToColumn: "user_id",
            OnDelete: db.ActionCascade,
            OnUpdate: db.ActionCascade,
        },
    }
    
    migration.CreateRelationship(rel)
    return migration
}
```

### 查询关联数据

```go
// 加载关联的文章
user, _ := repo.Get(ctx, userSchema, 1)

// 方式 1: 显式 Preload
articles, _ := repo.Query(articleSchema).
    Where("user_id", "=", user.ID).
    All(ctx)

// 方式 2: 通过关系名 (v0.5.0+)
articles, _ := repo.Query(userSchema).
    Preload("articles").  // 自动加载关联
    Get(ctx, 1)

// 方式 3: JOIN 查询 (支持的数据库)
users, _ := repo.Query(userSchema).
    Join("articles", "user_id", "id").
    Where("articles.published", "=", true).
    All(ctx)
```

### 验证关系支持

```go
repo, _ := db.NewRepository(config)
support := repo.Adapter().(db.RelationshipManager).GetRelationshipSupport()

validator := db.NewRelationshipValidator(support)

// 检查支持情况
if !validator.CanJoin() {
    fmt.Println("该数据库不支持 JOIN 操作")
}

// 检查多对多是否需要转发
if validator.NeedsManyToManyEmulation(userRolesRel) {
    fmt.Println("多对多关系将通过中间表转发")
}

// 获取支持摘要
summary := validator.GetSupportSummary()
// {
//     "OneToOne": true,
//     "OneToMany": true,
//     "ManyToMany": false,
//     "ForeignKey": true,
//     "Join": true,
//     "Nested": false,
// }
```

## 迁移工具中的使用

### Schema-based Migration 中定义关系

```go
func NewMigration_CreateUserArticleRelation() db.MigrationInterface {
    migration := db.NewSchemaMigration("20260204000000", "create_user_article_relation")
    
    userSchema := db.NewBaseSchema("users")
    articleSchema := db.NewBaseSchema("articles")
    
    // 自动创建表和外键/中间表
    migration.CreateTable(userSchema)
    migration.CreateTable(articleSchema)
    
    // 创建关系
    rel := &db.Relationship{
        Name: "articles",
        FromSchema: userSchema,
        ToSchema: articleSchema,
        Type: db.OneToMany,
        ForeignKey: &db.ForeignKeyDef{
            FromColumn: "id",
            ToColumn: "user_id",
            OnDelete: db.ActionCascade,
        },
    }
    migration.CreateRelationship(rel)
    
    return migration
}
```

### Raw SQL Migration 中使用特定 adapter 的关系

```go
func NewMigration_PostgreSQLFullTextSearch() db.MigrationInterface {
    migration := db.NewRawSQLMigration("20260204010000", "postgres_full_text_search")
    
    // PostgreSQL 特定的 JSON 关系
    migration.AddUpSQL(`
        CREATE TABLE user_metadata (
            user_id INTEGER PRIMARY KEY REFERENCES users(id),
            tags JSONB,
            preferences JSONB
        )
    `)
    
    migration.AddDownSQL(`DROP TABLE user_metadata`)
    migration.ForAdapter("postgres")
    
    return migration
}
```

## 特殊情况处理

### 1. 多对多关系禁用时

```go
validator := db.NewRelationshipValidator(support)

if support.ManyToMany {
    // 原生支持，使用外键
} else if support.OneToMany && support.JoinTable {
    // 通过中间表转发
    // Adapter 自动创建中间表
} else {
    // 完全不支持
    return fmt.Errorf("database does not support many-to-many relationships")
}
```

### 2. JOIN 操作禁用时

```go
if !support.SupportsJoin {
    // 选项 1: 返回错误提示用户使用 Preload
    return fmt.Errorf("adapter does not support JOIN, use Preload instead")
    
    // 选项 2: 自动转换为 N+1 查询 + 应用层 JOIN（性能低）
    // 不推荐，但对于某些场景可能可接受
}
```

### 3. 嵌套查询不支持时

```go
if !support.SupportsNested {
    // 提示用户需要多步骤查询
    fmt.Println("nested queries not supported, use Preload with multiple queries")
}
```

## 性能考虑

### 1. 中间表转发的性能影响

多对多关系如果通过中间表转发，会产生额外的连接开销：

```
原生支持: SELECT * FROM user_roles WHERE user_id = ?
中间表: SELECT r.* FROM roles r 
       JOIN user_roles ur ON r.id = ur.role_id 
       WHERE ur.user_id = ?
```

**优化方案**:
- 在迁移时主动选择原生支持的数据库
- 或在应用层缓存关系数据

### 2. 应用层 JOIN 的性能影响

MongoDB/Elasticsearch 等非关系型数据库的关系查询会产生多次请求：

```
步骤 1: SELECT users WHERE condition
步骤 2: SELECT articles WHERE id IN (user_ids)
步骤 3: 应用层合并
```

**优化方案**:
- 使用数据库原生的 lookup/聚合操作（如 MongoDB 的 $lookup）
- 或使用专门的查询语言（如 Elasticsearch 的嵌套查询）

## 扩展性

### 自定义 Adapter 的关系支持

实现 `RelationshipManager` 接口：

```go
type CustomAdapter struct {
    // ...
}

func (a *CustomAdapter) GetRelationshipSupport() *RelationshipSupport {
    return &RelationshipSupport{
        OneToOne:       true,
        OneToMany:      true,
        ManyToMany:     false,
        SupportsForeignKey: true,
        SupportsJoin:   true,
        SupportsNested: false,
        Strategy:       StrategyJoinTable,
    }
}

func (a *CustomAdapter) CreateRelationship(ctx context.Context, rel *Relationship) error {
    // 自定义实现
}

// ... 其他方法
```

## 总结

这个设计的优势：

1. ✅ **统一的 API** - 所有数据库使用相同的关系定义方式
2. ✅ **自适应策略** - 自动根据数据库能力调整实现
3. ✅ **可查询能力** - 应用层可以查询数据库支持哪些操作
4. ✅ **渐进式支持** - 不支持的功能可以转发或禁用
5. ✅ **性能可控** - 开发者可以根据能力选择最优策略
6. ✅ **易于扩展** - 新的数据库只需声明支持情况即可

---

**相关版本**:
- v0.4.0: Migration 工具基础
- v0.5.0: 关系支持 (规划中)
- v1.0.0: 完整的关系查询和优化
