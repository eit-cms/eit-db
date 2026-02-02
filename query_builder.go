package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// QueryBuilder 查询构建器 (使用 Changeset 进行数据操作)
type QueryBuilder struct {
	schema   Schema
	repo     *Repository
	context  context.Context
}

// NewQueryBuilder 创建查询构建器
func NewQueryBuilder(schema Schema, repo *Repository) *QueryBuilder {
	return &QueryBuilder{
		schema:   schema,
		repo:     repo,
		context:  context.Background(),
	}
}

// WithContext 设置上下文
func (qb *QueryBuilder) WithContext(ctx context.Context) *QueryBuilder {
	qb.context = ctx
	return qb
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
		fields = append(fields, fieldName)
		placeholders = append(placeholders, "?")
		values = append(values, value)
	}

	sql := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		qb.schema.TableName(),
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
		setClauses = append(setClauses, fieldName+" = ?")
		values = append(values, value)
	}

	// 添加 WHERE 条件
	if whereClause != "" {
		values = append(values, whereArgs...)
	}

	sql := fmt.Sprintf(
		"UPDATE %s SET %s",
		qb.schema.TableName(),
		strings.Join(setClauses, ", "),
	)

	if whereClause != "" {
		sql += " WHERE " + whereClause
	}

	return qb.repo.Exec(qb.context, sql, values...)
}

// UpdateByID 按 ID 更新数据
func (qb *QueryBuilder) UpdateByID(id interface{}, cs *Changeset) (sql.Result, error) {
	return qb.Update(cs, "id = ?", id)
}

// ==================== DELETE 操作 ====================

// Delete 删除数据
func (qb *QueryBuilder) Delete(whereClause string, whereArgs ...interface{}) (sql.Result, error) {
	sql := fmt.Sprintf("DELETE FROM %s", qb.schema.TableName())
	
	if whereClause != "" {
		sql += " WHERE " + whereClause
		return qb.repo.Exec(qb.context, sql, whereArgs...)
	}
	
	return qb.repo.Exec(qb.context, sql)
}

// DeleteByID 按 ID 删除数据
func (qb *QueryBuilder) DeleteByID(id interface{}) (sql.Result, error) {
	return qb.Delete("id = ?", id)
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
	return qb.SoftDelete("id = ?", id)
}

// ==================== SELECT 操作 ====================

// Select 查询数据
func (qb *QueryBuilder) Select(columns string, whereClause string, whereArgs ...interface{}) (*sql.Rows, error) {
	if columns == "" {
		columns = "*"
	}

	sql := fmt.Sprintf("SELECT %s FROM %s", columns, qb.schema.TableName())

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
		"SELECT * FROM %s WHERE id = ? LIMIT 1",
		qb.schema.TableName(),
	)
	return qb.repo.QueryRow(qb.context, sql, id), nil
}

// SelectOne 查询单条数据
func (qb *QueryBuilder) SelectOne(whereClause string, whereArgs ...interface{}) (*sql.Row, error) {
	sql := fmt.Sprintf(
		"SELECT * FROM %s WHERE %s LIMIT 1",
		qb.schema.TableName(),
		whereClause,
	)
	return qb.repo.QueryRow(qb.context, sql, whereArgs...), nil
}

// SelectCount 查询数据总数
func (qb *QueryBuilder) SelectCount(whereClause string, whereArgs ...interface{}) (int64, error) {
	sql := fmt.Sprintf("SELECT COUNT(*) FROM %s", qb.schema.TableName())
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
		schema:   qb.schema,
		repo:     &Repository{adapter: &txAdapter{tx: tx}},
		context:  qb.context,
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
	tx Tx
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

// ==================== 链式操作辅助类 ====================

// QueryChain 链式查询构建器
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
	sql := fmt.Sprintf("SELECT * FROM %s", qc.builder.schema.TableName())
	
	if qc.whereSQL != "" {
		sql += " WHERE " + qc.whereSQL
	}
	
	if qc.orderBy != "" {
		sql += " ORDER BY " + qc.orderBy
	}
	
	sql += " LIMIT 1"
	
	return qc.builder.repo.QueryRow(qc.builder.context, sql, qc.whereArgs...), nil
}

// All 查询所有
func (qc *QueryChain) All() (*sql.Rows, error) {
	sql := fmt.Sprintf("SELECT * FROM %s", qc.builder.schema.TableName())
	
	if qc.whereSQL != "" {
		sql += " WHERE " + qc.whereSQL
	}
	
	if qc.orderBy != "" {
		sql += " ORDER BY " + qc.orderBy
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
	sql := fmt.Sprintf("SELECT COUNT(*) FROM %s", qc.builder.schema.TableName())
	
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
