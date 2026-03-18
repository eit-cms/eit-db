package db

import (
	"context"
	"fmt"
	"strings"
)

// ViewFeatures 提供跨数据库的 VIEW 功能统一入口。
//
// 目标是把视图能力放在通用特性层：
//   - SQL Server / PostgreSQL: 增强支持（replace/alter 更完整）
//   - MySQL / SQLite: 基础支持（创建/删除）
//   - MongoDB: 不支持
//
// 使用示例：
//
//	vf, ok := db.GetViewFeatures(repo.GetAdapter())
//	if !ok {
//	    return errors.New("adapter does not support view features")
//	}
//	err := vf.View("report_user_stats").
//	    As("SELECT id, name FROM users WHERE active = 1").
//	    ExecuteCreate(ctx)
type ViewFeatures struct {
	adapter Adapter
	dialect string
}

// GetViewFeatures 从 Adapter 中提取通用 ViewFeatures。
func GetViewFeatures(adapter Adapter) (*ViewFeatures, bool) {
	switch adapter.(type) {
	case *PostgreSQLAdapter:
		return &ViewFeatures{adapter: adapter, dialect: "postgres"}, true
	case *SQLServerAdapter:
		return &ViewFeatures{adapter: adapter, dialect: "sqlserver"}, true
	case *MySQLAdapter:
		return &ViewFeatures{adapter: adapter, dialect: "mysql"}, true
	case *SQLiteAdapter:
		return &ViewFeatures{adapter: adapter, dialect: "sqlite"}, true
	default:
		return nil, false
	}
}

// View 开始构建一个通用视图。
func (f *ViewFeatures) View(name string) *ViewBuilder {
	b := &ViewBuilder{
		adapter:       f.adapter,
		dialect:       f.dialect,
		name:          strings.TrimSpace(name),
		dropIfExists:  true,
		createOrAlter: true,
	}

	// SQLite 不支持 CREATE OR REPLACE VIEW，默认回退为 CREATE VIEW。
	if f.dialect == "sqlite" {
		b.createOrAlter = false
	}

	return b
}

// ViewBuilder 构建通用 VIEW DDL。
type ViewBuilder struct {
	adapter Adapter
	dialect string

	name          string
	selectSQL     string
	materialized  bool
	createOrAlter bool
	withCheckOpt  bool
	dropIfExists  bool
	args          []interface{}
}

// As 设置视图定义 SQL（AS 后的 SELECT 语句）。
func (b *ViewBuilder) As(selectSQL string) *ViewBuilder {
	b.selectSQL = strings.TrimSpace(selectSQL)
	return b
}

// Materialized 切换为物化视图（仅 PostgreSQL）。
func (b *ViewBuilder) Materialized() *ViewBuilder {
	b.materialized = true
	// PostgreSQL 不支持 CREATE OR REPLACE MATERIALIZED VIEW。
	b.createOrAlter = false
	return b
}

// CreateOnly 强制使用 CREATE VIEW。
func (b *ViewBuilder) CreateOnly() *ViewBuilder {
	b.createOrAlter = false
	return b
}

// CreateOrAlter 使用各数据库等价语法：
//   - SQL Server: CREATE OR ALTER VIEW
//   - PostgreSQL/MySQL: CREATE OR REPLACE VIEW
//   - SQLite: 不支持（会在 BuildCreate 报错）
func (b *ViewBuilder) CreateOrAlter() *ViewBuilder {
	b.createOrAlter = true
	return b
}

// WithCheckOption 追加 WITH CHECK OPTION（SQLite 不支持）。
func (b *ViewBuilder) WithCheckOption() *ViewBuilder {
	b.withCheckOpt = true
	return b
}

// DropIfExists 删除视图时使用 IF EXISTS（默认）。
func (b *ViewBuilder) DropIfExists() *ViewBuilder {
	b.dropIfExists = true
	return b
}

// DropStrict 删除视图时不使用 IF EXISTS。
func (b *ViewBuilder) DropStrict() *ViewBuilder {
	b.dropIfExists = false
	return b
}

// Args 设置 ExecuteCreate 时传入的参数。
func (b *ViewBuilder) Args(args ...interface{}) *ViewBuilder {
	b.args = args
	return b
}

// BuildCreate 生成 CREATE/ALTER VIEW DDL。
func (b *ViewBuilder) BuildCreate() (string, error) {
	if err := b.validateCreate(); err != nil {
		return "", err
	}

	viewName := normalizeIdentifierByDialect(b.name, b.dialect)
	var sb strings.Builder

	if b.materialized {
		sb.WriteString("CREATE MATERIALIZED VIEW ")
		sb.WriteString(viewName)
		sb.WriteString(" AS\n")
		sb.WriteString(b.selectSQL)
		sb.WriteString(";")
		return sb.String(), nil
	}

	switch b.dialect {
	case "sqlserver":
		if b.createOrAlter {
			sb.WriteString("CREATE OR ALTER VIEW ")
		} else {
			sb.WriteString("CREATE VIEW ")
		}
	case "postgres", "mysql":
		if b.createOrAlter {
			sb.WriteString("CREATE OR REPLACE VIEW ")
		} else {
			sb.WriteString("CREATE VIEW ")
		}
	case "sqlite":
		if b.createOrAlter {
			return "", fmt.Errorf("sqlite view %q: CREATE OR REPLACE VIEW is not supported", b.name)
		}
		sb.WriteString("CREATE VIEW ")
	default:
		return "", fmt.Errorf("view feature: unsupported dialect %q", b.dialect)
	}

	sb.WriteString(viewName)
	sb.WriteString(" AS\n")
	sb.WriteString(b.selectSQL)

	if b.withCheckOpt {
		if b.dialect == "sqlite" {
			return "", fmt.Errorf("sqlite view %q: WITH CHECK OPTION is not supported", b.name)
		}
		sb.WriteString("\nWITH CHECK OPTION")
	}

	sb.WriteString(";")
	return sb.String(), nil
}

// BuildDrop 生成 DROP VIEW DDL。
func (b *ViewBuilder) BuildDrop() (string, error) {
	if strings.TrimSpace(b.name) == "" {
		return "", fmt.Errorf("view: name is required")
	}
	if b.materialized && b.dialect != "postgres" {
		return "", fmt.Errorf("materialized view is only supported by PostgreSQL in unified builder")
	}

	viewName := normalizeIdentifierByDialect(b.name, b.dialect)
	if b.materialized {
		if b.dropIfExists {
			return "DROP MATERIALIZED VIEW IF EXISTS " + viewName + ";", nil
		}
		return "DROP MATERIALIZED VIEW " + viewName + ";", nil
	}

	if b.dropIfExists {
		return "DROP VIEW IF EXISTS " + viewName + ";", nil
	}
	return "DROP VIEW " + viewName + ";", nil
}

// ExecuteCreate 执行创建视图。
func (b *ViewBuilder) ExecuteCreate(ctx context.Context) error {
	query, err := b.BuildCreate()
	if err != nil {
		return err
	}
	_, err = b.adapter.Exec(ctx, query, b.args...)
	return err
}

// Drop 执行删除视图。
func (b *ViewBuilder) Drop(ctx context.Context) error {
	query, err := b.BuildDrop()
	if err != nil {
		return err
	}
	_, err = b.adapter.Exec(ctx, query)
	return err
}

func (b *ViewBuilder) validateCreate() error {
	if strings.TrimSpace(b.name) == "" {
		return fmt.Errorf("view: name is required")
	}
	if strings.TrimSpace(b.selectSQL) == "" {
		return fmt.Errorf("view %q: select SQL is required", b.name)
	}

	qf := b.adapter.GetQueryFeatures()
	if qf == nil {
		return fmt.Errorf("view %q: query features not available", b.name)
	}
	if b.materialized {
		if !qf.SupportsMaterializedView {
			return fmt.Errorf("view %q: materialized view is not supported by this adapter", b.name)
		}
	} else if !qf.SupportsView {
		return fmt.Errorf("view %q: view is not supported by this adapter", b.name)
	}

	if b.materialized && b.dialect != "postgres" {
		return fmt.Errorf("materialized view is only supported by PostgreSQL in unified builder")
	}

	return nil
}

func normalizeIdentifierByDialect(name, dialect string) string {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return trimmed
	}

	if strings.ContainsAny(trimmed, "[]`\"") {
		return trimmed
	}

	parts := strings.Split(trimmed, ".")
	for i, p := range parts {
		p = strings.TrimSpace(p)
		switch dialect {
		case "sqlserver":
			parts[i] = "[" + strings.ReplaceAll(p, "]", "]]") + "]"
		case "mysql":
			parts[i] = "`" + strings.ReplaceAll(p, "`", "``") + "`"
		default:
			parts[i] = "\"" + strings.ReplaceAll(p, "\"", "\"\"") + "\""
		}
	}

	return strings.Join(parts, ".")
}
