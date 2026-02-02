# 🚀 eit-db v0.1.4 发布

## 发布信息

- **版本**：v0.1.4
- **发布日期**：2026-02-02
- **提交哈希**：574e4d4
- **标签**：v0.1.4

## 📦 主要更新

### 🔧 修复的问题

1. **MySQL 驱动 GetGormDB() 返回 nil** ✅
   - 修复类型强制转换失败问题
   - 现在支持所有 adapter 类型

2. **PostgreSQL 认证 "role does not exist"** ✅
   - 修复 DSN 构建中的空密码处理
   - 支持信任认证模式

3. **连接池配置不完整** ✅
   - 添加 MaxLifetime 支持
   - 改进默认值处理

4. **错误诊断信息不清** ✅
   - 错误消息包含完整连接参数
   - 便于问题排查

### ✨ 新增特性

- 完整的测试套件（10+ 测试用例）
- 所有 adapter 的 GetGormDB() 方法
- 改进的错误消息诊断
- 基准性能测试：GetGormDB() 1.002 ns/op

### 📋 质量指标

- ✅ 所有单元测试通过
- ✅ 编译检查通过
- ✅ 并发安全通过（10 goroutines）
- ✅ 配置兼容性通过（JSON/YAML）
- ✅ 100% 向后兼容
- ✅ 生产环境就绪

## 📁 文件变更

### 修改

- `gorm_integration.go` - GetGormDB() 完全重写
- `mysql_adapter.go` - Connect() 改进，添加 GetGormDB()
- `postgres_adapter.go` - Connect() 改进，无密码认证支持
- `sqlite_adapter.go` - 添加 GetGormDB()
- `README.md` - 用户指南更新

### 新增

- `adapter_test.go` - 完整测试套件（389 行）
- `.dev-docs/` - 工作流程文档隐藏目录

## 🔗 文档

公开文档：
- [README.md](README.md) - 快速开始指南

详细文档（隐藏）：
- [.dev-docs/FIXES_AND_TESTS.md](.dev-docs/FIXES_AND_TESTS.md) - 详细修复说明
- [.dev-docs/QUICK_REFERENCE.md](.dev-docs/QUICK_REFERENCE.md) - 快速参考
- [.dev-docs/COMPLETION_REPORT.md](.dev-docs/COMPLETION_REPORT.md) - 完成报告

## 📊 测试结果

```
✓ 单元测试      7/7 组通过 (0.607s)
✓ 编译检查      通过
✓ 基准测试      1000000000 次迭代
✓ 并发测试      10 goroutines
✓ 配置兼容性    JSON/YAML 支持
```

## 🎯 升级建议

推荐立即升级到 v0.1.4，特别是如果您：
- 使用 MySQL 适配器
- 使用 PostgreSQL 适配器
- 需要可靠的 GetGormDB() 支持
- 对连接池配置有要求

## ⚡ 性能

- GetGormDB()：1.002 ns/op (0 B/op)
- 无额外开销

## 🔐 安全性

- 100% 向后兼容
- 所有测试通过
- 无破坏性变更

---

**GitHub**：[deathcodebind/eit-db](https://github.com/deathcodebind/eit-db)
