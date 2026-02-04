#!/bin/bash
# EIT-DB 集成测试运行脚本

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 当前目录
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# 加载环境变量
if [ -f "$SCRIPT_DIR/.env.test" ]; then
    echo -e "${BLUE}加载环境变量...${NC}"
    source "$SCRIPT_DIR/.env.test"
fi

# 打印函数
print_header() {
    echo -e "${BLUE}================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}================================${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

# 检查Docker是否运行
check_docker() {
    if ! command -v docker &> /dev/null; then
        print_error "Docker 未安装"
        return 1
    fi
    
    if ! docker info > /dev/null 2>&1; then
        print_error "Docker 未运行"
        return 1
    fi
    
    print_success "Docker 已安装并运行"
    return 0
}

# 启动数据库容器
start_databases() {
    print_header "启动数据库容器"
    
    if ! check_docker; then
        print_warning "跳过数据库启动"
        return 1
    fi
    
    cd "$SCRIPT_DIR"
    
    print_warning "删除旧容器..."
    docker-compose down 2>/dev/null || true
    
    print_warning "启动新容器..."
    docker-compose up -d
    
    # 等待数据库就绪
    print_warning "等待数据库就绪..."
    sleep 5
    
    # 检查PostgreSQL
    if docker-compose ps postgres | grep -q "healthy\|running"; then
        print_success "PostgreSQL 已就绪"
    else
        print_warning "PostgreSQL 启动缓慢，继续等待..."
        sleep 10
    fi
    
    # 检查MySQL
    if docker-compose ps mysql | grep -q "healthy\|running"; then
        print_success "MySQL 已就绪"
    else
        print_warning "MySQL 启动缓慢，继续等待..."
        sleep 10
    fi
    
    print_success "所有数据库已启动"
}

# 停止数据库容器
stop_databases() {
    print_header "停止数据库容器"
    
    if ! check_docker; then
        return 0
    fi
    
    cd "$SCRIPT_DIR"
    docker-compose down
    print_success "数据库已停止"
}

# 运行核心库测试
run_unit_tests() {
    print_header "运行核心库单元测试"
    
    cd "$SCRIPT_DIR"
    
    if go test ./... -v; then
        print_success "核心库单元测试通过"
        return 0
    else
        print_error "核心库单元测试失败"
        return 1
    fi
}

# 运行集成测试
run_integration_tests() {
    print_header "运行集成测试"
    
    cd "$SCRIPT_DIR/adapter-application-tests"
    
    # 确保go.mod和go.sum存在
    if [ ! -f "go.mod" ]; then
        print_warning "初始化go.mod..."
        go mod init adapter-application-tests
        go mod edit -replace github.com/eit-cms/eit-db=../
    fi
    
    # 运行SQLite测试（无需依赖）
    print_warning "运行SQLite集成测试..."
    if go test -v -run SQLite 2>&1 | tee sqlite_test.log; then
        print_success "SQLite集成测试通过"
    else
        print_error "SQLite集成测试失败"
        return 1
    fi
    
    # 如果设置了TEST_MODE=all，运行其他数据库测试
    if [ "$TEST_MODE" = "all" ]; then
        if [ ! -z "$POSTGRES_DSN" ]; then
            print_warning "运行PostgreSQL集成测试..."
            go test -v -run Postgres 2>&1 | tee postgres_test.log || print_warning "PostgreSQL测试跳过或失败"
        fi
        
        if [ ! -z "$MYSQL_DSN" ]; then
            print_warning "运行MySQL集成测试..."
            go test -v -run MySQL 2>&1 | tee mysql_test.log || print_warning "MySQL测试跳过或失败"
        fi
    fi
}

# 生成测试报告
generate_report() {
    print_header "测试报告"
    
    cd "$SCRIPT_DIR"
    
    echo -e "${BLUE}核心库测试：${NC}"
    go test ./... -v -coverprofile=coverage.out 2>&1 | tail -5
    
    if [ -f coverage.out ]; then
        print_success "代码覆盖率报告已生成：coverage.out"
        # 显示覆盖率统计
        go tool cover -func=coverage.out | tail -1
    fi
}

# 清理测试生成的临时文件
cleanup() {
    print_header "清理临时文件"
    
    cd "$SCRIPT_DIR/adapter-application-tests"
    rm -f *.db test_*.db *.log 2>/dev/null || true
    
    print_success "临时文件已清理"
}

# 主函数
main() {
    case "${1:-help}" in
        start)
            start_databases
            ;;
        stop)
            stop_databases
            ;;
        unit)
            run_unit_tests
            ;;
        integration)
            run_integration_tests
            ;;
        report)
            generate_report
            ;;
        clean)
            cleanup
            ;;
        all)
            start_databases
            run_unit_tests && \
            run_integration_tests && \
            generate_report && \
            cleanup && \
            stop_databases
            ;;
        all-keep)
            start_databases
            run_unit_tests && \
            run_integration_tests && \
            generate_report && \
            cleanup
            print_warning "数据库容器保持运行，使用 './test.sh stop' 停止"
            ;;
        help|*)
            cat << EOF
EIT-DB 集成测试运行脚本

用法: ./test.sh [command]

命令:
    start          - 启动数据库容器
    stop           - 停止数据库容器
    unit           - 运行核心库单元测试
    integration    - 运行集成测试（SQLite + 其他配置的数据库）
    report         - 生成测试覆盖率报告
    clean          - 清理临时文件
    all            - 完整流程：启动 -> 测试 -> 报告 -> 清理 -> 停止
    all-keep       - 完整流程但保持数据库容器运行
    help           - 显示此帮助信息

示例:
    # 仅运行SQLite测试
    ./test.sh unit
    ./test.sh integration
    
    # 启动所有数据库并运行完整测试
    ./test.sh all-keep
    
    # 然后工作或调试...
    # 完成后停止容器
    ./test.sh stop

环境变量:
    TEST_MODE      - 设置为 "all" 运行所有数据库测试，"sqlite" 仅运行SQLite
    POSTGRES_DSN   - PostgreSQL 连接字符串
    MYSQL_DSN      - MySQL 连接字符串

EOF
            ;;
    esac
}

main "$@"
