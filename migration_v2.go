package db

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// Migration 接口 - 每个迁移文件都需要实现这个接口
type MigrationInterface interface {
	// Up 执行迁移
	Up(ctx context.Context, repo *Repository) error
	
	// Down 回滚迁移
	Down(ctx context.Context, repo *Repository) error
	
	// Version 返回迁移版本号（通常是时间戳）
	Version() string
	
	// Description 返回迁移描述
	Description() string
}

// BaseMigration 基础迁移结构，提供通用字段
type BaseMigration struct {
	version     string
	description string
}

// NewBaseMigration 创建基础迁移
func NewBaseMigration(version, description string) *BaseMigration {
	return &BaseMigration{
		version:     version,
		description: description,
	}
}

// Version 返回版本号
func (m *BaseMigration) Version() string {
	return m.version
}

// Description 返回描述
func (m *BaseMigration) Description() string {
	return m.description
}

// SchemaMigration 基于 Schema 的迁移
type SchemaMigration struct {
	*BaseMigration
	createSchemas []Schema
	dropSchemas   []Schema
}

// NewSchemaMigration 创建基于 Schema 的迁移
func NewSchemaMigration(version, description string) *SchemaMigration {
	return &SchemaMigration{
		BaseMigration: NewBaseMigration(version, description),
		createSchemas: make([]Schema, 0),
		dropSchemas:   make([]Schema, 0),
	}
}

// CreateTable 添加要创建的表
func (m *SchemaMigration) CreateTable(schema Schema) *SchemaMigration {
	m.createSchemas = append(m.createSchemas, schema)
	return m
}

// DropTable 添加要删除的表
func (m *SchemaMigration) DropTable(schema Schema) *SchemaMigration {
	m.dropSchemas = append(m.dropSchemas, schema)
	return m
}

// Up 执行迁移
func (m *SchemaMigration) Up(ctx context.Context, repo *Repository) error {
	for _, schema := range m.createSchemas {
		tableName := schema.TableName()
		createSQL := buildCreateTableSQL(repo, schema)
		if _, err := repo.Exec(ctx, createSQL); err != nil {
			return fmt.Errorf("failed to create table %s: %w", tableName, err)
		}
	}
	return nil
}

// Down 回滚迁移
func (m *SchemaMigration) Down(ctx context.Context, repo *Repository) error {
	// 先删除 Up 中创建的表
	for i := len(m.createSchemas) - 1; i >= 0; i-- {
		schema := m.createSchemas[i]
		dropSQL := buildDropTableSQL(repo, schema.TableName())
		if _, err := repo.Exec(ctx, dropSQL); err != nil {
			return fmt.Errorf("failed to drop table %s: %w", schema.TableName(), err)
		}
	}
	
	// 然后恢复 Up 中删除的表
	for _, schema := range m.dropSchemas {
		tableName := schema.TableName()
		createSQL := buildCreateTableSQL(repo, schema)
		if _, err := repo.Exec(ctx, createSQL); err != nil {
			return fmt.Errorf("failed to recreate table %s: %w", tableName, err)
		}
	}
	return nil
}

func buildCreateTableSQL(repo *Repository, schema Schema) string {
	columns := make([]string, 0, len(schema.Fields()))
	for _, field := range schema.Fields() {
		columns = append(columns, buildColumnDefinition(repo.GetAdapter(), field))
	}

	columnsSQL := strings.Join(columns, ", ")
	tableName := schema.TableName()

	switch repo.GetAdapter().(type) {
	case *SQLServerAdapter:
		return fmt.Sprintf("IF OBJECT_ID('%s', 'U') IS NULL CREATE TABLE %s (%s)", tableName, tableName, columnsSQL)
	default:
		return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", tableName, columnsSQL)
	}
}

func buildDropTableSQL(repo *Repository, tableName string) string {
	switch repo.GetAdapter().(type) {
	case *SQLServerAdapter:
		return fmt.Sprintf("IF OBJECT_ID('%s', 'U') IS NOT NULL DROP TABLE %s", tableName, tableName)
	default:
		return fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	}
}

func buildColumnDefinition(adapter Adapter, field *Field) string {
	switch adapter.(type) {
	case *PostgreSQLAdapter:
		return buildPostgresColumn(field)
	case *MySQLAdapter:
		return buildMySQLColumn(field)
	case *SQLiteAdapter:
		return buildSQLiteColumn(field)
	case *SQLServerAdapter:
		return buildSQLServerColumn(field)
	default:
		return buildGenericColumn(field)
	}
}

func buildPostgresColumn(field *Field) string {
	if field.Primary && field.Autoinc {
		return fmt.Sprintf("%s SERIAL PRIMARY KEY", field.Name)
	}
	col := fmt.Sprintf("%s %s", field.Name, mapPostgresType(field.Type))
	return applyColumnConstraints(col, field)
}

func buildMySQLColumn(field *Field) string {
	if field.Primary && field.Autoinc {
		return fmt.Sprintf("%s INT AUTO_INCREMENT PRIMARY KEY", field.Name)
	}
	col := fmt.Sprintf("%s %s", field.Name, mapMySQLType(field.Type))
	return applyColumnConstraints(col, field)
}

func buildSQLiteColumn(field *Field) string {
	if field.Primary && field.Autoinc {
		return fmt.Sprintf("%s INTEGER PRIMARY KEY AUTOINCREMENT", field.Name)
	}
	col := fmt.Sprintf("%s %s", field.Name, mapSQLiteType(field.Type))
	return applyColumnConstraints(col, field)
}

func buildSQLServerColumn(field *Field) string {
	if field.Primary && field.Autoinc {
		return fmt.Sprintf("%s INT IDENTITY(1,1) PRIMARY KEY", field.Name)
	}
	col := fmt.Sprintf("%s %s", field.Name, mapSQLServerType(field.Type))
	return applyColumnConstraints(col, field)
}

func buildGenericColumn(field *Field) string {
	col := fmt.Sprintf("%s %s", field.Name, "TEXT")
	return applyColumnConstraints(col, field)
}

func applyColumnConstraints(column string, field *Field) string {
	if !field.Null {
		column += " NOT NULL"
	}
	if field.Unique {
		column += " UNIQUE"
	}
	return column
}

func mapPostgresType(fieldType FieldType) string {
	switch fieldType {
	case TypeString:
		return "VARCHAR(255)"
	case TypeInteger:
		return "INTEGER"
	case TypeFloat:
		return "DOUBLE PRECISION"
	case TypeBoolean:
		return "BOOLEAN"
	case TypeTime:
		return "TIMESTAMP"
	case TypeBinary:
		return "BYTEA"
	case TypeDecimal:
		return "DECIMAL(18,2)"
	case TypeJSON:
		return "JSONB"
	case TypeArray:
		return "TEXT"
	default:
		return "TEXT"
	}
}

func mapMySQLType(fieldType FieldType) string {
	switch fieldType {
	case TypeString:
		return "VARCHAR(255)"
	case TypeInteger:
		return "INT"
	case TypeFloat:
		return "FLOAT"
	case TypeBoolean:
		return "TINYINT(1)"
	case TypeTime:
		return "DATETIME"
	case TypeBinary:
		return "LONGBLOB"
	case TypeDecimal:
		return "DECIMAL(18,2)"
	case TypeJSON:
		return "JSON"
	case TypeArray:
		return "TEXT"
	default:
		return "TEXT"
	}
}

func mapSQLiteType(fieldType FieldType) string {
	switch fieldType {
	case TypeString:
		return "TEXT"
	case TypeInteger:
		return "INTEGER"
	case TypeFloat:
		return "REAL"
	case TypeBoolean:
		return "INTEGER"
	case TypeTime:
		return "DATETIME"
	case TypeBinary:
		return "BLOB"
	case TypeDecimal:
		return "NUMERIC"
	case TypeJSON:
		return "TEXT"
	case TypeArray:
		return "TEXT"
	default:
		return "TEXT"
	}
}

func mapSQLServerType(fieldType FieldType) string {
	switch fieldType {
	case TypeString:
		return "NVARCHAR(255)"
	case TypeInteger:
		return "INT"
	case TypeFloat:
		return "FLOAT"
	case TypeBoolean:
		return "BIT"
	case TypeTime:
		return "DATETIME2"
	case TypeBinary:
		return "VARBINARY(MAX)"
	case TypeDecimal:
		return "DECIMAL(18,2)"
	case TypeJSON:
		return "NVARCHAR(MAX)"
	case TypeArray:
		return "NVARCHAR(MAX)"
	default:
		return "NVARCHAR(MAX)"
	}
}

// RawSQLMigration 原始 SQL 迁移
type RawSQLMigration struct {
	*BaseMigration
	upSQL    []string
	downSQL  []string
	adapter  string // 可选：指定特定的 adapter
}

// NewRawSQLMigration 创建原始 SQL 迁移
func NewRawSQLMigration(version, description string) *RawSQLMigration {
	return &RawSQLMigration{
		BaseMigration: NewBaseMigration(version, description),
		upSQL:         make([]string, 0),
		downSQL:       make([]string, 0),
	}
}

// AddUpSQL 添加 Up SQL
func (m *RawSQLMigration) AddUpSQL(sql string) *RawSQLMigration {
	m.upSQL = append(m.upSQL, sql)
	return m
}

// AddDownSQL 添加 Down SQL
func (m *RawSQLMigration) AddDownSQL(sql string) *RawSQLMigration {
	m.downSQL = append(m.downSQL, sql)
	return m
}

// ForAdapter 指定 adapter
func (m *RawSQLMigration) ForAdapter(adapter string) *RawSQLMigration {
	m.adapter = adapter
	return m
}

// Up 执行迁移
func (m *RawSQLMigration) Up(ctx context.Context, repo *Repository) error {
	for _, sql := range m.upSQL {
		if _, err := repo.Exec(ctx, sql); err != nil {
			return fmt.Errorf("failed to execute SQL: %s, error: %w", sql, err)
		}
	}
	return nil
}

// Down 回滚迁移
func (m *RawSQLMigration) Down(ctx context.Context, repo *Repository) error {
	for _, sql := range m.downSQL {
		if _, err := repo.Exec(ctx, sql); err != nil {
			return fmt.Errorf("failed to execute SQL: %s, error: %w", sql, err)
		}
	}
	return nil
}

// MigrationRunner 迁移运行器
type MigrationRunner struct {
	repo       *Repository
	migrations []MigrationInterface
}

// NewMigrationRunner 创建迁移运行器
func NewMigrationRunner(repo *Repository) *MigrationRunner {
	return &MigrationRunner{
		repo:       repo,
		migrations: make([]MigrationInterface, 0),
	}
}

// Register 注册迁移
func (r *MigrationRunner) Register(migration MigrationInterface) {
	r.migrations = append(r.migrations, migration)
}

// Up 执行所有待执行的迁移
func (r *MigrationRunner) Up(ctx context.Context) error {
	// 确保迁移日志表存在
	if err := r.ensureMigrationTable(ctx); err != nil {
		return err
	}
	
	// 获取已执行的迁移
	executed, err := r.getExecutedMigrations(ctx)
	if err != nil {
		return err
	}
	
	// 执行未执行的迁移
	for _, migration := range r.migrations {
		version := migration.Version()
		if _, exists := executed[version]; !exists {
			fmt.Printf("Running migration %s: %s\n", version, migration.Description())
			
			if err := migration.Up(ctx, r.repo); err != nil {
				return fmt.Errorf("migration %s failed: %w", version, err)
			}
			
			// 记录迁移
			if err := r.recordMigration(ctx, version); err != nil {
				return fmt.Errorf("failed to record migration %s: %w", version, err)
			}
			
			fmt.Printf("✓ Migration %s completed\n", version)
		}
	}
	
	return nil
}

// Down 回滚最后一个迁移
func (r *MigrationRunner) Down(ctx context.Context) error {
	// 获取最后执行的迁移
	lastVersion, err := r.getLastExecutedVersion(ctx)
	if err != nil {
		return err
	}
	
	if lastVersion == "" {
		return fmt.Errorf("no migrations to rollback")
	}
	
	// 找到对应的迁移
	var targetMigration MigrationInterface
	for _, migration := range r.migrations {
		if migration.Version() == lastVersion {
			targetMigration = migration
			break
		}
	}
	
	if targetMigration == nil {
		return fmt.Errorf("migration %s not found in registered migrations", lastVersion)
	}
	
	fmt.Printf("Rolling back migration %s: %s\n", lastVersion, targetMigration.Description())
	
	// 执行回滚
	if err := targetMigration.Down(ctx, r.repo); err != nil {
		return fmt.Errorf("rollback failed: %w", err)
	}
	
	// 删除迁移记录
	if err := r.removeMigrationRecord(ctx, lastVersion); err != nil {
		return fmt.Errorf("failed to remove migration record: %w", err)
	}
	
	fmt.Printf("✓ Migration %s rolled back\n", lastVersion)
	
	return nil
}

// Status 显示迁移状态
func (r *MigrationRunner) Status(ctx context.Context) ([]MigrationStatus, error) {
	if err := r.ensureMigrationTable(ctx); err != nil {
		return nil, err
	}
	
	executed, err := r.getExecutedMigrations(ctx)
	if err != nil {
		return nil, err
	}
	
	statuses := make([]MigrationStatus, 0, len(r.migrations))
	for _, migration := range r.migrations {
		version := migration.Version()
		status := MigrationStatus{
			Version:     version,
			Description: migration.Description(),
			Applied:     false,
		}
		
		if appliedAt, exists := executed[version]; exists {
			status.Applied = true
			status.AppliedAt = appliedAt
		}
		
		statuses = append(statuses, status)
	}
	
	return statuses, nil
}

// MigrationStatus 迁移状态
type MigrationStatus struct {
	Version     string
	Description string
	Applied     bool
	AppliedAt   time.Time
}

// ensureMigrationTable 确保迁移表存在
func (r *MigrationRunner) ensureMigrationTable(ctx context.Context) error {
	sql := `
CREATE TABLE IF NOT EXISTS schema_migrations (
    version VARCHAR(255) PRIMARY KEY,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)`
	_, err := r.repo.Exec(ctx, sql)
	return err
}

// getExecutedMigrations 获取已执行的迁移
func (r *MigrationRunner) getExecutedMigrations(ctx context.Context) (map[string]time.Time, error) {
	sql := "SELECT version, applied_at FROM schema_migrations ORDER BY version"
	
	rows, err := r.repo.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	
	executed := make(map[string]time.Time)
	for rows.Next() {
		var version string
		var appliedAt time.Time
		if err := rows.Scan(&version, &appliedAt); err != nil {
			return nil, err
		}
		executed[version] = appliedAt
	}
	
	return executed, rows.Err()
}

// getLastExecutedVersion 获取最后执行的迁移版本
func (r *MigrationRunner) getLastExecutedVersion(ctx context.Context) (string, error) {
	sql := "SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1"
	
	rows, err := r.repo.Query(ctx, sql)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	
	if rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			return "", err
		}
		return version, nil
	}
	
	return "", nil
}

// recordMigration 记录迁移
func (r *MigrationRunner) recordMigration(ctx context.Context, version string) error {
	sql := "INSERT INTO schema_migrations (version) VALUES (?)"
	_, err := r.repo.Exec(ctx, sql, version)
	return err
}

// removeMigrationRecord 删除迁移记录
func (r *MigrationRunner) removeMigrationRecord(ctx context.Context, version string) error {
	sql := "DELETE FROM schema_migrations WHERE version = ?"
	_, err := r.repo.Exec(ctx, sql, version)
	return err
}
