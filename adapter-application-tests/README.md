# EIT-DB 适配器集成测试

本目录包含适用于所有数据库适配器的集成测试套件。

## 项目结构

- `sqlite_integration_test.go` - SQLite适配器集成测试（无依赖，可直接运行）
- `postgres_integration_test.go` - PostgreSQL适配器集成测试（需要PostgreSQL实例）
- `mysql_integration_test.go` - MySQL适配器集成测试（需要MySQL实例）

## SQLite测试

SQLite是文件型数据库，无需外部服务，可以直接运行：

```bash
go test -v -run SQLite
```

### 测试覆盖范围

- **基础CRUD**：Create, Read, Update, Delete操作
- **查询条件**：WHERE、IN、BETWEEN、LIKE等
- **聚合**：GROUP BY、HAVING、DISTINCT
- **窗口函数**：ROW_NUMBER、RANK、LAG/LEAD等（SQLite 3.25.0+）
- **CTE**：通用表表达式（SQLite 3.8.4+）
- **递归CTE**：层级数据查询（SQLite 3.8.4+）
- **JSON操作**：JSON_EXTRACT、JSON_ARRAY等
- **UPSERT**：INSERT OR REPLACE、ON CONFLICT等
- **事务**：BEGIN、COMMIT、ROLLBACK

### SQLite版本要求

| 特性 | 最小版本 |
|------|---------|
| 基础功能 | 3.0+ |
| JSON支持 | 3.9+ |
| CTE支持 | 3.8.4+ |
| 递归CTE | 3.8.4+ |
| 窗口函数 | 3.25.0+ |
| ON CONFLICT | 3.24.0+ |

## PostgreSQL测试

需要运行PostgreSQL实例，通过环境变量配置连接：

```bash
# 使用Docker快速启动PostgreSQL
docker run --name postgres-test \
  -e POSTGRES_USER=testuser \
  -e POSTGRES_PASSWORD=testpass \
  -e POSTGRES_DB=testdb \
  -p 5432:5432 \
  postgres:15-alpine

# 设置环境变量
export POSTGRES_USER=testuser
export POSTGRES_PASSWORD=testpass
export POSTGRES_DB=testdb
export POSTGRES_DSN="postgres://testuser:testpass@localhost:5432/testdb?sslmode=disable"

# 运行测试
go test -v -run Postgres
```

### 测试覆盖范围

- **基础CRUD**：Create, Read, Update, Delete
- **物化视图**：CREATE MATERIALIZED VIEW、REFRESH
- **数组类型**：PostgreSQL ARRAY数据类型
- **全文搜索**：to_tsvector、plainto_tsquery
- **JSONB类型**：JSON操作符（->, ->>）
- **递归CTE**：层级数据查询

## MySQL测试

需要运行MySQL实例，通过环境变量配置连接：

```bash
# 使用Docker快速启动MySQL
docker run --name mysql-test \
  -e MYSQL_ROOT_PASSWORD=rootpass \
  -e MYSQL_USER=testuser \
  -e MYSQL_PASSWORD=testpass \
  -e MYSQL_DATABASE=testdb \
  -p 3306:3306 \
  mysql:8.0

# 设置环境变量
export MYSQL_HOST=localhost
export MYSQL_USER=testuser
export MYSQL_PASSWORD=testpass
export MYSQL_DB=testdb
export MYSQL_DSN="testuser:testpass@tcp(localhost:3306)/testdb"

# 运行测试
go test -v -run MySQL
```

### 测试覆盖范围

- **基础CRUD**：Create, Read, Update, Delete
- **全文搜索**：MATCH...AGAINST（需要FULLTEXT索引）
- **JSON类型**：JSON_EXTRACT、JSON_UNQUOTE操作
- **窗口函数**：ROW_NUMBER、RANK等（MySQL 8.0+）
- **CTE**：通用表表达式（MySQL 8.0+）
- **ON DUPLICATE KEY UPDATE**：Upsert操作
- **INSERT IGNORE**：忽略冲突插入
- **批量插入**：CreateInBatches

### MySQL版本要求

| 特性 | 最小版本 |
|------|---------|
| 基础功能 | 5.7+ |
| JSON支持 | 5.7+ |
| 窗口函数 | 8.0+ |
| CTE支持 | 8.0+ |
| ON CONFLICT | 5.7+（ON DUPLICATE KEY UPDATE） |

## 运行所有测试

### 仅SQLite（不需要外部依赖）

```bash
go test -v
```

### SQLite + PostgreSQL + MySQL

首先启动所有数据库容器，然后：

```bash
# 使用docker-compose
docker-compose up -d

# 设置环境变量
source .env

# 运行所有测试
go test -v
```

## 测试结果示例

```
=== RUN   TestBasicCRUD
--- PASS: TestBasicCRUD (0.01s)
=== RUN   TestQueryWhere
--- PASS: TestQueryWhere (0.00s)
=== RUN   TestWhereIN
--- PASS: TestWhereIN (0.00s)
=== RUN   TestBetween
--- PASS: TestBetween (0.00s)
=== RUN   TestDistinct
--- PASS: TestDistinct (0.00s)
=== RUN   TestWindowFunction
--- PASS: TestWindowFunction (0.00s)
=== RUN   TestCTE
--- PASS: TestCTE (0.00s)
=== RUN   TestUpsert
--- PASS: TestUpsert (0.00s)
=== RUN   TestTransaction
--- PASS: TestTransaction (0.00s)
PASS
ok      adapter-application-tests       1.614s
```

## Docker Compose 配置

创建 `docker-compose.yml`：

```yaml
version: '3.8'

services:
  postgres:
    image: postgres:15-alpine
    environment:
      POSTGRES_USER: testuser
      POSTGRES_PASSWORD: testpass
      POSTGRES_DB: testdb
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U testuser"]
      interval: 10s
      timeout: 5s
      retries: 5

  mysql:
    image: mysql:8.0
    environment:
      MYSQL_ROOT_PASSWORD: rootpass
      MYSQL_USER: testuser
      MYSQL_PASSWORD: testpass
      MYSQL_DATABASE: testdb
    ports:
      - "3306:3306"
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "localhost"]
      interval: 10s
      timeout: 5s
      retries: 5
```

创建 `.env`：

```bash
# PostgreSQL
POSTGRES_USER=testuser
POSTGRES_PASSWORD=testpass
POSTGRES_DB=testdb
POSTGRES_DSN=postgres://testuser:testpass@localhost:5432/testdb?sslmode=disable

# MySQL
MYSQL_HOST=localhost
MYSQL_USER=testuser
MYSQL_PASSWORD=testpass
MYSQL_DB=testdb
MYSQL_DSN=testuser:testpass@tcp(localhost:3306)/testdb
```

## 测试设计原则

1. **逐数据库增进**：从SQLite开始，逐步支持PostgreSQL、MySQL等
2. **功能覆盖**：覆盖QueryFeatures中声明的所有特性
3. **版本敏感**：标记每个特性的最小版本要求
4. **错误处理**：处理环境配置缺失时的skip行为
5. **隔离清理**：每个测试都清理自己创建的数据和对象

## 待办事项

- [ ] SQL Server适配器集成测试
- [ ] MongoDB适配器集成测试
- [ ] 性能基准测试
- [ ] 并发访问测试
- [ ] 连接池测试
