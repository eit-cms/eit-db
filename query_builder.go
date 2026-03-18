package db

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
)

var legacyQueryBuilderWarningEnabled atomic.Bool
var legacyQueryBuilderWarningOnce sync.Once

func init() {
	legacyQueryBuilderWarningEnabled.Store(true)
}

// SetLegacyQueryBuilderWarningEnabled 设置 v1 QueryBuilder 弃用提示开关。
func SetLegacyQueryBuilderWarningEnabled(enabled bool) {
	legacyQueryBuilderWarningEnabled.Store(enabled)
}

func maybeWarnLegacyQueryBuilder() {
	if !legacyQueryBuilderWarningEnabled.Load() {
		return
	}

	legacyQueryBuilderWarningOnce.Do(func() {
		log.Printf("[eit-db] QueryBuilder(v1) 已进入兼容模式，建议迁移到 Repository.NewQueryConstructor() 使用 v2 查询构造器")
	})
}

func quoteCommaSeparatedIdentifiers(dialect SQLDialect, raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || trimmed == "*" {
		return trimmed
	}

	parts := strings.Split(trimmed, ",")
	for i, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		parts[i] = dialect.QuoteIdentifier(part)
	}

	return strings.Join(parts, ", ")
}

func quoteOrderByClause(dialect SQLDialect, raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return trimmed
	}

	items := strings.Split(trimmed, ",")
	for i, item := range items {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}

		fields := strings.Fields(item)
		if len(fields) == 0 {
			continue
		}

		quoted := dialect.QuoteIdentifier(fields[0])
		if len(fields) > 1 {
			quoted = quoted + " " + strings.ToUpper(fields[1])
		}
		items[i] = quoted
	}

	return strings.Join(items, ", ")
}

// QueryBuilder 查询构建器 (使用 Changeset 进行数据操作)
// Deprecated: QueryBuilder(v1) 仅保留兼容，建议使用 Repository.NewQueryConstructor() 创建 v2 查询构造器。
type QueryBuilder struct {
	schema  Schema
	repo    *Repository
	context context.Context
}

// NewQueryBuilder 创建查询构建器
// Deprecated: 使用 Repository.NewQueryConstructor(schema) 获取 v2 查询构造器。
func NewQueryBuilder(schema Schema, repo *Repository) *QueryBuilder {
	maybeWarnLegacyQueryBuilder()

	return &QueryBuilder{
		schema:  schema,
		repo:    repo,
		context: context.Background(),
	}
}

// WithContext 设置上下文
func (qb *QueryBuilder) WithContext(ctx context.Context) *QueryBuilder {
	qb.context = ctx
	return qb
}

func (qb *QueryBuilder) dialect() SQLDialect {
	if qb == nil || qb.repo == nil || qb.repo.adapter == nil {
		return NewMySQLDialect()
	}

	provider := qb.repo.adapter.GetQueryBuilderProvider()
	if p, ok := provider.(*DefaultSQLQueryConstructorProvider); ok && p.dialect != nil {
		return p.dialect
	}

	return NewMySQLDialect()
}

func (qb *QueryBuilder) quoteIdentifier(name string) string {
	return qb.dialect().QuoteIdentifier(name)
}

// ==================== INSERT 操作 ====================

// Insert 插入数据
func (qb *QueryBuilder) Insert(cs *Changeset) (sql.Result, error) {
	if !cs.IsValid() {
		return nil, fmt.Errorf("changeset 验证失败: %v", cs.Errors())
	}

	// 确保所有数据都标记为变更（用于插入）
	cs.ForceChanges()

	// 构建 INSERT SQL
	fields := make([]string, 0)
	placeholders := make([]string, 0)
	values := make([]interface{}, 0)

	for fieldName, value := range cs.Changes() {
		fields = append(fields, qb.quoteIdentifier(fieldName))
		placeholders = append(placeholders, "?")
		values = append(values, value)
	}

	sql := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		qb.quoteIdentifier(qb.schema.TableName()),
		strings.Join(fields, ", "),
		strings.Join(placeholders, ", "),
	)

	return qb.repo.Exec(qb.context, sql, values...)
}

// ==================== UPDATE 操作 ====================

// Update 更新数据
func (qb *QueryBuilder) Update(cs *Changeset, whereClause string, whereArgs ...interface{}) (sql.Result, error) {
	if !cs.IsValid() {
		return nil, fmt.Errorf("changeset 验证失败: %v", cs.Errors())
	}

	changes := cs.Changes()
	if len(changes) == 0 {
		return nil, fmt.Errorf("没有要更新的字段")
	}

	// 构建 UPDATE SQL
	setClauses := make([]string, 0)
	values := make([]interface{}, 0)

	for fieldName, value := range changes {
		setClauses = append(setClauses, qb.quoteIdentifier(fieldName)+" = ?")
		values = append(values, value)
	}

	// 添加 WHERE 条件
	if whereClause != "" {
		values = append(values, whereArgs...)
	}

	sql := fmt.Sprintf(
		"UPDATE %s SET %s",
		qb.quoteIdentifier(qb.schema.TableName()),
		strings.Join(setClauses, ", "),
	)

	if whereClause != "" {
		sql += " WHERE " + whereClause
	}

	return qb.repo.Exec(qb.context, sql, values...)
}

// UpdateByID 按 ID 更新数据
func (qb *QueryBuilder) UpdateByID(id interface{}, cs *Changeset) (sql.Result, error) {
	return qb.Update(cs, qb.quoteIdentifier("id")+" = ?", id)
}

// ==================== DELETE 操作 ====================

// Delete 删除数据
func (qb *QueryBuilder) Delete(whereClause string, whereArgs ...interface{}) (sql.Result, error) {
	sql := fmt.Sprintf("DELETE FROM %s", qb.quoteIdentifier(qb.schema.TableName()))

	if whereClause != "" {
		sql += " WHERE " + whereClause
		return qb.repo.Exec(qb.context, sql, whereArgs...)
	}

	return qb.repo.Exec(qb.context, sql)
}

// DeleteByID 按 ID 删除数据
func (qb *QueryBuilder) DeleteByID(id interface{}) (sql.Result, error) {
	return qb.Delete(qb.quoteIdentifier("id")+" = ?", id)
}

// SoftDelete 软删除数据 (仅适用于有 deleted_at 字段的表)
func (qb *QueryBuilder) SoftDelete(whereClause string, whereArgs ...interface{}) (sql.Result, error) {
	schema := NewBaseSchema(qb.schema.TableName())
	if schema.GetField("deleted_at") == nil {
		return nil, fmt.Errorf("表 %s 不支持软删除（没有 deleted_at 字段）", qb.schema.TableName())
	}

	cs := NewChangeset(qb.schema)
	cs.PutChange("deleted_at", Timestamp())

	return qb.Update(cs, whereClause, whereArgs...)
}

// SoftDeleteByID 按 ID 软删除数据
func (qb *QueryBuilder) SoftDeleteByID(id interface{}) (sql.Result, error) {
	return qb.SoftDelete(qb.quoteIdentifier("id")+" = ?", id)
}

// ==================== SELECT 操作 ====================

// Select 查询数据
func (qb *QueryBuilder) Select(columns string, whereClause string, whereArgs ...interface{}) (*sql.Rows, error) {
	if columns == "" {
		columns = "*"
	}

	sql := fmt.Sprintf(
		"SELECT %s FROM %s",
		quoteCommaSeparatedIdentifiers(qb.dialect(), columns),
		qb.quoteIdentifier(qb.schema.TableName()),
	)

	if whereClause != "" {
		sql += " WHERE " + whereClause
		return qb.repo.Query(qb.context, sql, whereArgs...)
	}

	return qb.repo.Query(qb.context, sql)
}

// SelectAll 查询所有数据
func (qb *QueryBuilder) SelectAll() (*sql.Rows, error) {
	return qb.Select("*", "")
}

// SelectByID 按 ID 查询单条数据
func (qb *QueryBuilder) SelectByID(id interface{}) (*sql.Row, error) {
	sql := fmt.Sprintf(
		"SELECT * FROM %s WHERE %s = ? LIMIT 1",
		qb.quoteIdentifier(qb.schema.TableName()),
		qb.quoteIdentifier("id"),
	)
	return qb.repo.QueryRow(qb.context, sql, id), nil
}

// SelectOne 查询单条数据
func (qb *QueryBuilder) SelectOne(whereClause string, whereArgs ...interface{}) (*sql.Row, error) {
	sql := fmt.Sprintf(
		"SELECT * FROM %s WHERE %s LIMIT 1",
		qb.quoteIdentifier(qb.schema.TableName()),
		whereClause,
	)
	return qb.repo.QueryRow(qb.context, sql, whereArgs...), nil
}

// SelectCount 查询数据总数
func (qb *QueryBuilder) SelectCount(whereClause string, whereArgs ...interface{}) (int64, error) {
	sql := fmt.Sprintf("SELECT COUNT(*) FROM %s", qb.quoteIdentifier(qb.schema.TableName()))
	if whereClause != "" {
		sql += " WHERE " + whereClause
	}

	row := qb.repo.QueryRow(qb.context, sql, whereArgs...)
	var count int64
	err := row.Scan(&count)
	return count, err
}

// ==================== 事务操作 ====================

// Transaction 事务操作
func (qb *QueryBuilder) Transaction(fn func(*QueryBuilder) error) error {
	tx, err := qb.repo.Begin(qb.context)
	if err != nil {
		return err
	}

	// 创建事务内的查询构建器
	txQB := &QueryBuilder{
		schema:  qb.schema,
		repo:    &Repository{adapter: &txAdapter{tx: tx, provider: qb.repo.adapter.GetQueryBuilderProvider()}},
		context: qb.context,
	}

	if err := fn(txQB); err != nil {
		tx.Rollback(qb.context)
		return err
	}

	return tx.Commit(qb.context)
}

// ==================== 辅助适配器用于事务 ====================

// txAdapter 事务适配器
type txAdapter struct {
	tx       Tx
	provider QueryConstructorProvider
}

func (ta *txAdapter) Connect(ctx context.Context, config *Config) error {
	return nil
}

func (ta *txAdapter) Close() error {
	return nil
}

func (ta *txAdapter) Ping(ctx context.Context) error {
	return nil
}

func (ta *txAdapter) Query(ctx context.Context, sql string, args ...interface{}) (*sql.Rows, error) {
	return ta.tx.Query(ctx, sql, args...)
}

func (ta *txAdapter) QueryRow(ctx context.Context, sql string, args ...interface{}) *sql.Row {
	return ta.tx.QueryRow(ctx, sql, args...)
}

func (ta *txAdapter) Exec(ctx context.Context, sql string, args ...interface{}) (sql.Result, error) {
	return ta.tx.Exec(ctx, sql, args...)
}

func (ta *txAdapter) Begin(ctx context.Context, opts ...interface{}) (Tx, error) {
	return nil, fmt.Errorf("不支持嵌套事务")
}

func (ta *txAdapter) GetRawConn() interface{} {
	return ta.tx
}

func (ta *txAdapter) RegisterScheduledTask(ctx context.Context, task *ScheduledTaskConfig) error {
	return fmt.Errorf("scheduled tasks cannot be registered within a transaction")
}

func (ta *txAdapter) UnregisterScheduledTask(ctx context.Context, taskName string) error {
	return fmt.Errorf("scheduled tasks cannot be unregistered within a transaction")
}

func (ta *txAdapter) ListScheduledTasks(ctx context.Context) ([]*ScheduledTaskStatus, error) {
	return nil, fmt.Errorf("cannot list scheduled tasks within a transaction")
}

func (ta *txAdapter) GetQueryBuilderProvider() QueryConstructorProvider {
	if ta.provider != nil {
		return ta.provider
	}
	return NewDefaultSQLQueryConstructorProvider(NewMySQLDialect())
}

// GetQueryFeatures 返回事务适配器的查询特性 (默认 MySQL)
func (ta *txAdapter) GetQueryFeatures() *QueryFeatures {
	return NewMySQLQueryFeatures()
}

func (ta *txAdapter) GetDatabaseFeatures() *DatabaseFeatures {
	// 事务适配器返回基本的特性集，与 GORM adapter 保持一致
	return &DatabaseFeatures{
		SupportsCompositeKeys:        true,
		SupportsForeignKeys:          true,
		SupportsCompositeForeignKeys: true,
		SupportsCompositeIndexes:     true,
		SupportsPartialIndexes:       false,
		SupportsDeferrable:           false,

		SupportsEnumType:      true,
		SupportsCompositeType: false,
		SupportsDomainType:    false,
		SupportsUDT:           false,

		SupportsStoredProcedures: true,
		SupportsFunctions:        true,
		SupportsAggregateFuncs:   false,
		FunctionLanguages:        []string{"sql"},

		SupportsWindowFunctions: true,
		SupportsCTE:             true,
		SupportsRecursiveCTE:    true,
		SupportsMaterializedCTE: false,

		HasNativeJSON:     true,
		SupportsJSONPath:  true,
		SupportsJSONIndex: true,

		SupportsFullTextSearch: true,
		FullTextLanguages:      []string{"english"},

		SupportsArrays:       false,
		SupportsGenerated:    true,
		SupportsReturning:    false,
		SupportsUpsert:       true,
		SupportsListenNotify: false,

		DatabaseName:    "Transaction Adapter",
		DatabaseVersion: "Unknown",
		Description:     "Transaction wrapper adapter",
	}
}

// ==================== 链式操作辅助类 ====================

// QueryChain 链式查询构建器
// Deprecated: QueryChain 属于 v1 兼容层，建议迁移到 v2 QueryConstructor 链式 API。
type QueryChain struct {
	builder   *QueryBuilder
	whereSQL  string
	whereArgs []interface{}
	limit     int
	offset    int
	orderBy   string
}

// Where 添加 WHERE 条件
func (qc *QueryChain) Where(condition string, args ...interface{}) *QueryChain {
	if qc.whereSQL != "" {
		qc.whereSQL += " AND "
	}
	qc.whereSQL += condition
	qc.whereArgs = append(qc.whereArgs, args...)
	return qc
}

// Limit 设置 LIMIT
func (qc *QueryChain) Limit(limit int) *QueryChain {
	qc.limit = limit
	return qc
}

// Offset 设置 OFFSET
func (qc *QueryChain) Offset(offset int) *QueryChain {
	qc.offset = offset
	return qc
}

// OrderBy 设置排序
func (qc *QueryChain) OrderBy(orderBy string) *QueryChain {
	qc.orderBy = orderBy
	return qc
}

// First 查询第一条
func (qc *QueryChain) First() (*sql.Row, error) {
	sql := fmt.Sprintf("SELECT * FROM %s", qc.builder.quoteIdentifier(qc.builder.schema.TableName()))

	if qc.whereSQL != "" {
		sql += " WHERE " + qc.whereSQL
	}

	if qc.orderBy != "" {
		sql += " ORDER BY " + quoteOrderByClause(qc.builder.dialect(), qc.orderBy)
	}

	sql += " LIMIT 1"

	return qc.builder.repo.QueryRow(qc.builder.context, sql, qc.whereArgs...), nil
}

// All 查询所有
func (qc *QueryChain) All() (*sql.Rows, error) {
	sql := fmt.Sprintf("SELECT * FROM %s", qc.builder.quoteIdentifier(qc.builder.schema.TableName()))

	if qc.whereSQL != "" {
		sql += " WHERE " + qc.whereSQL
	}

	if qc.orderBy != "" {
		sql += " ORDER BY " + quoteOrderByClause(qc.builder.dialect(), qc.orderBy)
	}

	if qc.limit > 0 {
		sql += fmt.Sprintf(" LIMIT %d", qc.limit)
	}

	if qc.offset > 0 {
		sql += fmt.Sprintf(" OFFSET %d", qc.offset)
	}

	return qc.builder.repo.Query(qc.builder.context, sql, qc.whereArgs...)
}

// Count 统计数量
func (qc *QueryChain) Count() (int64, error) {
	sql := fmt.Sprintf("SELECT COUNT(*) FROM %s", qc.builder.quoteIdentifier(qc.builder.schema.TableName()))

	if qc.whereSQL != "" {
		sql += " WHERE " + qc.whereSQL
	}

	row := qc.builder.repo.QueryRow(qc.builder.context, sql, qc.whereArgs...)
	var count int64
	err := row.Scan(&count)
	return count, err
}

// Query 开始链式查询
func (qb *QueryBuilder) Query() *QueryChain {
	return &QueryChain{
		builder:   qb,
		whereArgs: make([]interface{}, 0),
	}
}
