package db

import (
	"context"
	"database/sql"
	"fmt"
)

// ChangesetExecutor 面向业务层的写操作封装。
// 它仅暴露基于 Changeset 的常见写路径，避免业务层默认直接操作 Tx。
type ChangesetExecutor struct {
	qb *QueryBuilder
}

func newChangesetExecutor(schema Schema, repo *Repository, ctx context.Context) *ChangesetExecutor {
	return &ChangesetExecutor{
		qb: &QueryBuilder{
			schema:  schema,
			repo:    repo,
			context: ctx,
		},
	}
}

// Insert 插入 Changeset。
func (e *ChangesetExecutor) Insert(cs *Changeset) (sql.Result, error) {
	if e == nil || e.qb == nil {
		return nil, fmt.Errorf("changeset executor is not initialized")
	}
	return e.qb.Insert(cs)
}

// Update 按条件更新 Changeset。
func (e *ChangesetExecutor) Update(cs *Changeset, whereClause string, whereArgs ...interface{}) (sql.Result, error) {
	if e == nil || e.qb == nil {
		return nil, fmt.Errorf("changeset executor is not initialized")
	}
	return e.qb.Update(cs, whereClause, whereArgs...)
}

// UpdateByID 按 ID 更新 Changeset。
func (e *ChangesetExecutor) UpdateByID(id interface{}, cs *Changeset) (sql.Result, error) {
	if e == nil || e.qb == nil {
		return nil, fmt.Errorf("changeset executor is not initialized")
	}
	return e.qb.UpdateByID(id, cs)
}

// Delete 按条件删除记录。
func (e *ChangesetExecutor) Delete(whereClause string, whereArgs ...interface{}) (sql.Result, error) {
	if e == nil || e.qb == nil {
		return nil, fmt.Errorf("changeset executor is not initialized")
	}
	return e.qb.Delete(whereClause, whereArgs...)
}

// DeleteByID 按 ID 删除记录。
func (e *ChangesetExecutor) DeleteByID(id interface{}) (sql.Result, error) {
	if e == nil || e.qb == nil {
		return nil, fmt.Errorf("changeset executor is not initialized")
	}
	return e.qb.DeleteByID(id)
}

// SoftDelete 软删除记录。
func (e *ChangesetExecutor) SoftDelete(whereClause string, whereArgs ...interface{}) (sql.Result, error) {
	if e == nil || e.qb == nil {
		return nil, fmt.Errorf("changeset executor is not initialized")
	}
	return e.qb.SoftDelete(whereClause, whereArgs...)
}

// SoftDeleteByID 按 ID 软删除记录。
func (e *ChangesetExecutor) SoftDeleteByID(id interface{}) (sql.Result, error) {
	if e == nil || e.qb == nil {
		return nil, fmt.Errorf("changeset executor is not initialized")
	}
	return e.qb.SoftDeleteByID(id)
}

// NewChangesetExecutor 创建面向业务层的 Changeset 执行器。
func (r *Repository) NewChangesetExecutor(ctx context.Context, schema Schema) (*ChangesetExecutor, error) {
	if r == nil {
		return nil, fmt.Errorf("repository is nil")
	}
	if schema == nil {
		return nil, fmt.Errorf("schema cannot be nil")
	}
	if r.GetAdapter() == nil {
		return nil, fmt.Errorf("adapter is not initialized")
	}
	return newChangesetExecutor(schema, r, ctx), nil
}

// WithChangeset 在单个事务内执行一组基于 Changeset 的写操作。
// 这是业务层推荐的多步写入入口。
func (r *Repository) WithChangeset(ctx context.Context, schema Schema, fn func(*ChangesetExecutor) error) error {
	if r == nil {
		return fmt.Errorf("repository is nil")
	}
	if schema == nil {
		return fmt.Errorf("schema cannot be nil")
	}
	if fn == nil {
		return fmt.Errorf("callback cannot be nil")
	}

	tx, err := r.Begin(ctx)
	if err != nil {
		return err
	}

	provider := r.GetAdapter().GetQueryBuilderProvider()
	txRepo := &Repository{adapter: &txAdapter{tx: tx, provider: provider}}
	executor := newChangesetExecutor(schema, txRepo, ctx)

	if err := fn(executor); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}

	return nil
}