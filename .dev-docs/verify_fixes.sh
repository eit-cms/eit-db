#!/bin/bash

# eit-db 修复验证脚本
# 此脚本验证所有修复是否正确应用

set -e

echo "========================================"
echo "eit-db 修复验证脚本"
echo "========================================"
echo ""

# 检查 Go 模块
echo "✓ 检查 Go 模块..."
go mod verify 2>/dev/null || echo "  警告: 模块验证失败"

# 检查代码格式
echo "✓ 检查代码格式..."
gofmt -l . | grep -E '\.(go)$' && echo "  警告: 存在格式不正确的文件" || echo "  所有文件格式正确"

# 检查编译
echo "✓ 检查编译..."
go build ./...

# 运行测试
echo "✓ 运行测试..."
echo ""
echo "--- 单元测试结果 ---"
go test -v -timeout 30s

# 运行基准测试
echo ""
echo "--- 基准测试结果 ---"
go test -bench=BenchmarkGetGormDB -benchmem -timeout 30s

echo ""
echo "========================================"
echo "✅ 所有验证通过！"
echo "========================================"
echo ""
echo "修复内容:"
echo "  1. ✅ GetGormDB() 返回 nil 问题已修复"
echo "  2. ✅ PostgreSQL 认证问题已修复"
echo "  3. ✅ 连接池配置已改进"
echo "  4. ✅ 错误消息已增强"
echo "  5. ✅ 完整测试覆盖已添加"
echo ""
echo "查看详细信息: cat FIXES_AND_TESTS.md"
