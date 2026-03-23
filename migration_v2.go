package db

import (
	"context"
	"fmt"
	"strings"
	"time"
)

type constraintSchema interface {
	Constraints() []TableConstraint
}

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
	// Phase 1: 创建所有表
	for _, schema := range m.createSchemas {
		if err := executeSchemaCreate(ctx, repo, schema); err != nil {
			return fmt.Errorf("failed to create table %s: %w", schema.TableName(), err)
		}
	}

	if !supportsSQLDDL(repo) {
		return nil
	}

	// Phase 2: 创建视图（FK ViewHint 声明的热点查询视图）
	for _, schema := range m.createSchemas {
		cs, ok := schema.(constraintSchema)
		if !ok {
			continue
		}
		for _, c := range cs.Constraints() {
			if c.Kind != ConstraintForeignKey || c.ViewHint == nil {
				continue
			}
			viewSQL, err := buildViewFromFKHintSQL(repo, schema.TableName(), c)
			if err != nil {
				return fmt.Errorf("failed to build view for FK %s: %w", c.Name, err)
			}
			if _, err := repo.Exec(ctx, viewSQL); err != nil {
				return fmt.Errorf("failed to create view for FK %s: %w", c.Name, err)
			}
		}
	}
	return nil
}

// Down 回滚迁移
func (m *SchemaMigration) Down(ctx context.Context, repo *Repository) error {
	if supportsSQLDDL(repo) {
		// Phase 1: 逆序删除视图（视图引用表，必须先删）
		for i := len(m.createSchemas) - 1; i >= 0; i-- {
			schema := m.createSchemas[i]
			cs, ok := schema.(constraintSchema)
			if !ok {
				continue
			}
			for _, c := range cs.Constraints() {
				if c.Kind != ConstraintForeignKey || c.ViewHint == nil {
					continue
				}
				hint := c.ViewHint
				viewName := hint.ViewName
				if viewName == "" {
					viewName = schema.TableName() + "_" + c.RefTable + "_view"
				}
				dropViewSQL := buildDropViewSQL(repo, viewName, hint.Materialized)
				if _, err := repo.Exec(ctx, dropViewSQL); err != nil {
					return fmt.Errorf("failed to drop view %s: %w", viewName, err)
				}
			}
		}
	}
	// Phase 2: 逆序删除表
	for i := len(m.createSchemas) - 1; i >= 0; i-- {
		schema := m.createSchemas[i]
		if err := executeSchemaDrop(ctx, repo, schema); err != nil {
			return fmt.Errorf("failed to drop table %s: %w", schema.TableName(), err)
		}
	}
	// Phase 3: 恢复 Up 中删除的表
	for _, schema := range m.dropSchemas {
		tableName := schema.TableName()
		if err := executeSchemaCreate(ctx, repo, schema); err != nil {
			return fmt.Errorf("failed to recreate table %s: %w", tableName, err)
		}
	}
	return nil
}

func buildCreateTableSQL(repo *Repository, schema Schema) string {
	adapter := repo.GetAdapter()
	dialect := resolveMigrationDialect(repo)
	quotedTableName := dialect.QuoteIdentifier(schema.TableName())

	primaryFields, uniqueConstraints, fkConstraints := collectTableConstraints(adapter, schema)
	effectiveInlinePrimary := ""
	if len(primaryFields) == 1 {
		effectiveInlinePrimary = primaryFields[0]
	}

	columns := make([]string, 0, len(schema.Fields()))
	for _, field := range schema.Fields() {
		columns = append(columns, buildColumnDefinition(adapter, dialect, field, field.Name == effectiveInlinePrimary))
	}

	if len(primaryFields) > 1 {
		columns = append(columns, fmt.Sprintf("PRIMARY KEY (%s)", joinQuotedIdentifiers(dialect, primaryFields)))
	}

	for _, unique := range uniqueConstraints {
		uniqueSQL := fmt.Sprintf("UNIQUE (%s)", joinQuotedIdentifiers(dialect, unique.Fields))
		if unique.Name != "" {
			uniqueSQL = fmt.Sprintf("CONSTRAINT %s %s", dialect.QuoteIdentifier(unique.Name), uniqueSQL)
		}
		columns = append(columns, uniqueSQL)
	}

	for _, fk := range fkConstraints {
		localCols := joinQuotedIdentifiers(dialect, fk.Fields)
		refTable := dialect.QuoteIdentifier(fk.RefTable)
		refCols := joinQuotedIdentifiers(dialect, fk.RefFields)
		fkSQL := fmt.Sprintf("FOREIGN KEY (%s) REFERENCES %s (%s)", localCols, refTable, refCols)
		if fk.OnDelete != "" {
			fkSQL += " ON DELETE " + fk.OnDelete
		}
		if fk.OnUpdate != "" {
			fkSQL += " ON UPDATE " + fk.OnUpdate
		}
		if fk.Name != "" {
			fkSQL = fmt.Sprintf("CONSTRAINT %s %s", dialect.QuoteIdentifier(fk.Name), fkSQL)
		}
		columns = append(columns, fkSQL)
	}

	columnsSQL := strings.Join(columns, ", ")
	tableName := schema.TableName()

	switch adapter.(type) {
	case *SQLServerAdapter:
		return fmt.Sprintf("IF OBJECT_ID('%s', 'U') IS NULL CREATE TABLE %s (%s)", tableName, quotedTableName, columnsSQL)
	default:
		return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", quotedTableName, columnsSQL)
	}
}

func buildDropTableSQL(repo *Repository, tableName string) string {
	quotedTableName := resolveMigrationDialect(repo).QuoteIdentifier(tableName)

	switch repo.GetAdapter().(type) {
	case *SQLServerAdapter:
		return fmt.Sprintf("IF OBJECT_ID('%s', 'U') IS NOT NULL DROP TABLE %s", tableName, quotedTableName)
	default:
		return fmt.Sprintf("DROP TABLE IF EXISTS %s", quotedTableName)
	}
}

func resolveMigrationDialect(repo *Repository) SQLDialect {
	if repo == nil || repo.GetAdapter() == nil {
		return NewMySQLDialect()
	}

	provider := repo.GetAdapter().GetQueryBuilderProvider()
	if p, ok := provider.(*DefaultSQLQueryConstructorProvider); ok && p.dialect != nil {
		return p.dialect
	}

	return NewMySQLDialect()
}

func joinQuotedIdentifiers(dialect SQLDialect, fields []string) string {
	quoted := make([]string, 0, len(fields))
	for _, field := range fields {
		quoted = append(quoted, dialect.QuoteIdentifier(field))
	}
	return strings.Join(quoted, ", ")
}

func collectTableConstraints(adapter Adapter, schema Schema) ([]string, []TableConstraint, []TableConstraint) {
	primaryFields := make([]string, 0)
	for _, field := range schema.Fields() {
		if field.Primary {
			primaryFields = append(primaryFields, field.Name)
		}
	}

	uniqueConstraints := make([]TableConstraint, 0)
	fkConstraints := make([]TableConstraint, 0)
	if cs, ok := schema.(constraintSchema); ok {
		for _, c := range cs.Constraints() {
			switch c.Kind {
			case ConstraintPrimaryKey:
				if len(c.Fields) > 0 {
					primaryFields = append([]string(nil), c.Fields...)
				}
			case ConstraintUnique:
				if len(c.Fields) > 0 {
					uniqueConstraints = append(uniqueConstraints, c)
				}
			case ConstraintForeignKey:
				if len(c.Fields) > 0 && c.RefTable != "" {
					fkConstraints = append(fkConstraints, c)
				}
			}
		}
	}

	primaryFields = normalizeConstraintFields(primaryFields)

	supportsCompositeKeys := true
	supportsCompositeIndexes := true
	supportsForeignKeys := true
	supportsCompositeForeignKeys := true
	if adapter != nil {
		if features := adapter.GetDatabaseFeatures(); features != nil {
			supportsCompositeKeys = features.SupportsCompositeKeys
			supportsCompositeIndexes = features.SupportsCompositeIndexes
			supportsForeignKeys = features.SupportsForeignKeys
			supportsCompositeForeignKeys = features.SupportsCompositeForeignKeys
		}
	}

	if len(primaryFields) > 1 && !supportsCompositeKeys {
		uniqueConstraints = append(uniqueConstraints, TableConstraint{
			Name:   "uk_fallback_composite_pk",
			Kind:   ConstraintUnique,
			Fields: append([]string(nil), primaryFields...),
		})
		primaryFields = primaryFields[:1]
	}

	filteredUnique := make([]TableConstraint, 0, len(uniqueConstraints))
	for _, c := range uniqueConstraints {
		fields := normalizeConstraintFields(c.Fields)
		if len(fields) == 0 {
			continue
		}
		if len(fields) > 1 && !supportsCompositeIndexes {
			continue
		}
		c.Fields = fields
		filteredUnique = append(filteredUnique, c)
	}

	// 外键约束：不支持外键时全部跳过；支持但不支持复合外键时降级为单列 FK
	filteredFK := make([]TableConstraint, 0, len(fkConstraints))
	if supportsForeignKeys {
		for _, fk := range fkConstraints {
			localFields := normalizeConstraintFields(fk.Fields)
			refFields := normalizeConstraintFields(fk.RefFields)
			if len(localFields) == 0 || fk.RefTable == "" {
				continue
			}
			if len(localFields) > 1 && !supportsCompositeForeignKeys {
				// 降级：仅保留第一列的单列 FK
				localFields = localFields[:1]
				if len(refFields) > 1 {
					refFields = refFields[:1]
				}
			}
			fk.Fields = localFields
			fk.RefFields = refFields
			filteredFK = append(filteredFK, fk)
		}
	}

	return primaryFields, filteredUnique, filteredFK
}

func buildColumnDefinition(adapter Adapter, dialect SQLDialect, field *Field, inlinePrimary bool) string {
	effective := *field
	effective.Primary = inlinePrimary
	if !inlinePrimary {
		effective.Autoinc = false
	}

	switch adapter.(type) {
	case *PostgreSQLAdapter:
		return buildPostgresColumn(&effective, adapter)
	case *MySQLAdapter:
		return buildMySQLColumn(&effective)
	case *SQLiteAdapter:
		return buildSQLiteColumn(&effective)
	case *SQLServerAdapter:
		return buildSQLServerColumn(&effective)
	default:
		return buildGenericColumn(&effective, dialect)
	}
}

func buildPostgresColumn(field *Field, adapter Adapter) string {
	name := quoteColumnIdentifier("postgres", field.Name)
	if field.Primary && field.Autoinc {
		return fmt.Sprintf("%s SERIAL PRIMARY KEY", name)
	}
	col := fmt.Sprintf("%s %s", name, mapPostgresType(field.Type, adapter))
	return applyColumnConstraints(col, field, "postgres")
}

func buildMySQLColumn(field *Field) string {
	name := quoteColumnIdentifier("mysql", field.Name)
	if field.Primary && field.Autoinc {
		return fmt.Sprintf("%s INT AUTO_INCREMENT PRIMARY KEY", name)
	}
	col := fmt.Sprintf("%s %s", name, mapMySQLType(field.Type))
	return applyColumnConstraints(col, field, "mysql")
}

func buildSQLiteColumn(field *Field) string {
	name := quoteColumnIdentifier("sqlite", field.Name)
	if field.Primary && field.Autoinc {
		return fmt.Sprintf("%s INTEGER PRIMARY KEY AUTOINCREMENT", name)
	}
	col := fmt.Sprintf("%s %s", name, mapSQLiteType(field.Type))
	return applyColumnConstraints(col, field, "sqlite")
}

func buildSQLServerColumn(field *Field) string {
	name := quoteColumnIdentifier("sqlserver", field.Name)
	if field.Primary && field.Autoinc {
		return fmt.Sprintf("%s INT IDENTITY(1,1) PRIMARY KEY", name)
	}
	col := fmt.Sprintf("%s %s", name, mapSQLServerType(field.Type))
	return applyColumnConstraints(col, field, "sqlserver")
}

func buildGenericColumn(field *Field, dialect SQLDialect) string {
	name := field.Name
	dialectName := "generic"
	if dialect != nil {
		name = dialect.QuoteIdentifier(field.Name)
		dialectName = strings.ToLower(strings.TrimSpace(dialect.Name()))
	}
	col := fmt.Sprintf("%s %s", name, "TEXT")
	return applyColumnConstraints(col, field, dialectName)
}

func quoteColumnIdentifier(dialectName, name string) string {
	switch dialectName {
	case "postgres":
		return quoteIdentifierWithDelimiter(name, `"`, `"`)
	case "mysql":
		return quoteIdentifierWithDelimiter(name, "`", "`")
	case "sqlite":
		return quoteIdentifierWithDelimiter(name, "`", "`")
	case "sqlserver":
		return quoteIdentifierWithDelimiter(name, "[", "]")
	default:
		return name
	}
}

func applyColumnConstraints(column string, field *Field, dialectName string) string {
	if !field.Null {
		column += " NOT NULL"
	}
	if field.Default != nil {
		column += " DEFAULT " + formatDefaultValueForDialect(field.Default, dialectName, field.Type)
	}
	if field.Primary {
		column += " PRIMARY KEY"
	}
	if field.Unique {
		column += " UNIQUE"
	}
	return column
}

func formatDefaultValueForDialect(value interface{}, dialectName string, fieldType FieldType) string {
	switch v := value.(type) {
	case string:
		return formatStringDefaultValue(v, fieldType)
	case bool:
		if strings.EqualFold(dialectName, "sqlserver") {
			if v {
				return "1"
			}
			return "0"
		}
		if v {
			return "TRUE"
		}
		return "FALSE"
	default:
		return fmt.Sprint(v)
	}
}

func formatStringDefaultValue(raw string, fieldType FieldType) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "''"
	}
	if isQuotedStringLiteral(trimmed) || isLikelyRawSQLDefaultExpression(trimmed, fieldType) {
		return trimmed
	}
	return "'" + strings.ReplaceAll(raw, "'", "''") + "'"
}

func isQuotedStringLiteral(value string) bool {
	if len(value) < 2 {
		return false
	}
	if (value[0] == '\'' && value[len(value)-1] == '\'') || (value[0] == '"' && value[len(value)-1] == '"') {
		return true
	}
	if len(value) >= 3 && (value[0] == 'N' || value[0] == 'n') && value[1] == '\'' && value[len(value)-1] == '\'' {
		return true
	}
	return false
}

func isLikelyRawSQLDefaultExpression(value string, fieldType FieldType) bool {
	if fieldType == TypeString {
		return false
	}

	upper := strings.ToUpper(strings.TrimSpace(value))
	switch upper {
	case "NULL", "TRUE", "FALSE", "CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME", "LOCALTIME", "LOCALTIMESTAMP":
		return true
	}
	if strings.Contains(value, "::") {
		return true
	}
	if strings.Contains(value, "(") && strings.HasSuffix(strings.TrimSpace(value), ")") {
		return true
	}
	if strings.HasPrefix(upper, "INTERVAL ") {
		return true
	}
	return false
}

func mapPostgresType(fieldType FieldType, adapter Adapter) string {
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
		if pg, ok := adapter.(*PostgreSQLAdapter); ok {
			return pg.PostgresJSONType()
		}
		return "JSONB"
	case TypeArray:
		return "TEXT"
	case TypeLocation:
		return "POINT"
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
	case TypeLocation:
		return "POINT"
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
	case TypeLocation:
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
	case TypeLocation:
		return "GEOGRAPHY"
	default:
		return "NVARCHAR(MAX)"
	}
}

// RawSQLMigration 原始 SQL 迁移
type RawSQLMigration struct {
	*BaseMigration
	upSQL   []string
	downSQL []string
	adapter string // 必填：指定目标 adapter
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
	m.adapter = normalizeMigrationAdapterName(adapter)
	return m
}

// Up 执行迁移
func (m *RawSQLMigration) Up(ctx context.Context, repo *Repository) error {
	if err := m.validateAdapterBinding(repo); err != nil {
		return err
	}

	for _, sql := range m.upSQL {
		if _, err := repo.Exec(ctx, sql); err != nil {
			return fmt.Errorf("failed to execute SQL: %s, error: %w", sql, err)
		}
	}
	return nil
}

// Down 回滚迁移
func (m *RawSQLMigration) Down(ctx context.Context, repo *Repository) error {
	if err := m.validateAdapterBinding(repo); err != nil {
		return err
	}

	for _, sql := range m.downSQL {
		if _, err := repo.Exec(ctx, sql); err != nil {
			return fmt.Errorf("failed to execute SQL: %s, error: %w", sql, err)
		}
	}
	return nil
}

func (m *RawSQLMigration) validateAdapterBinding(repo *Repository) error {
	if repo == nil || repo.GetAdapter() == nil {
		return fmt.Errorf("raw sql migration requires initialized repository")
	}
	if strings.TrimSpace(m.adapter) == "" {
		return fmt.Errorf("raw sql migration %s must call ForAdapter(adapter) before execution", m.Version())
	}

	current := currentMigrationAdapterName(repo)
	if current == "" {
		return fmt.Errorf("failed to resolve current repository adapter for raw sql migration %s", m.Version())
	}
	if current != m.adapter {
		return fmt.Errorf("raw sql migration %s targets adapter %q but current repository adapter is %q", m.Version(), m.adapter, current)
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
	return ensureFrameworkTableUsingSchema(ctx, r.repo, buildSchemaMigrationsSchemaV2())
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
	return executeMigrationOperation(ctx, r.repo, MigrationOperation{
		Kind:      MigrationOpRecordApplied,
		Version:   version,
		AppliedAt: time.Now(),
	})
}

// removeMigrationRecord 删除迁移记录
func (r *MigrationRunner) removeMigrationRecord(ctx context.Context, version string) error {
	return executeMigrationOperation(ctx, r.repo, MigrationOperation{
		Kind:    MigrationOpRemoveApplied,
		Version: version,
	})
}

func migrationLogPlaceholder(repo *Repository, index int) string {
	if index <= 0 {
		index = 1
	}
	dialect := resolveMigrationDialect(repo)
	if dialect == nil {
		return "?"
	}
	placeholder := strings.TrimSpace(dialect.GetPlaceholder(index))
	if placeholder == "" {
		return "?"
	}
	return placeholder
}

// ==================== FK ViewHint 视图 DDL 辅助函数 ====================

// deriveViewAlias 从表名推导简短别名，用于视图 SELECT 子句中区分两张表的列。
// 例如：users → u，order_items → oi，user_profiles → up。
func deriveViewAlias(tableName string) string {
	// 取 "." 后的最后一段（去除 schema 前缀）
	parts := strings.Split(tableName, ".")
	base := strings.Trim(parts[len(parts)-1], `"[]`+"`")
	if base == "" {
		return "t"
	}
	// 按 '_' 分词，取每个词的首字母
	words := strings.Split(base, "_")
	var result strings.Builder
	for _, w := range words {
		if len(w) > 0 {
			result.WriteByte(w[0])
		}
	}
	if result.Len() == 0 {
		return "t"
	}
	return result.String()
}

// buildViewFromFKHintSQL 根据外键约束上的 ViewHint 生成 CREATE VIEW SQL。
func buildViewFromFKHintSQL(repo *Repository, localTable string, fk TableConstraint) (string, error) {
	hint := fk.ViewHint
	viewName := hint.ViewName
	if viewName == "" {
		viewName = localTable + "_" + fk.RefTable + "_view"
	}
	if len(fk.Fields) == 0 || len(fk.RefFields) == 0 {
		return "", fmt.Errorf("FK %s: Fields or RefFields must not be empty", fk.Name)
	}

	dialect := resolveMigrationDialect(repo)
	adapter := repo.GetAdapter()

	localAlias := deriveViewAlias(localTable)
	refAlias := deriveViewAlias(fk.RefTable)
	if localAlias == refAlias {
		refAlias = refAlias + "2"
	}

	// SELECT 列
	var selectCols string
	if len(hint.Columns) > 0 {
		selectCols = strings.Join(hint.Columns, ", ")
	} else {
		selectCols = localAlias + ".*, " + refAlias + ".*"
	}

	// JOIN 类型
	joinType := "INNER"
	if hint.JoinType != "" {
		joinType = strings.ToUpper(strings.TrimSpace(hint.JoinType))
	}

	// ON 条件
	onParts := make([]string, 0, len(fk.Fields))
	for i, localField := range fk.Fields {
		refField := localField
		if i < len(fk.RefFields) {
			refField = fk.RefFields[i]
		}
		onParts = append(onParts, localAlias+"."+localField+" = "+refAlias+"."+refField)
	}
	onClause := strings.Join(onParts, " AND ")

	qLocalTable := dialect.QuoteIdentifier(localTable)
	qRefTable := dialect.QuoteIdentifier(fk.RefTable)
	qViewName := dialect.QuoteIdentifier(viewName)

	selectSQL := fmt.Sprintf("SELECT %s FROM %s %s %s JOIN %s %s ON %s",
		selectCols,
		qLocalTable, localAlias,
		joinType,
		qRefTable, refAlias,
		onClause,
	)

	switch adapter.(type) {
	case *PostgreSQLAdapter:
		if hint.Materialized {
			return fmt.Sprintf("CREATE MATERIALIZED VIEW IF NOT EXISTS %s AS %s", qViewName, selectSQL), nil
		}
		return fmt.Sprintf("CREATE OR REPLACE VIEW %s AS %s", qViewName, selectSQL), nil
	case *SQLServerAdapter:
		// SQL Server 不支持 CREATE OR REPLACE，使用 CREATE OR ALTER
		// Indexed View 限制过多，此处统一创建普通视图
		return fmt.Sprintf("CREATE OR ALTER VIEW %s AS %s", qViewName, selectSQL), nil
	case *MySQLAdapter:
		return fmt.Sprintf("CREATE OR REPLACE VIEW %s AS %s", qViewName, selectSQL), nil
	default:
		// SQLite 及其他
		return fmt.Sprintf("CREATE VIEW IF NOT EXISTS %s AS %s", qViewName, selectSQL), nil
	}
}

// buildDropViewSQL 生成删除视图的 SQL。
func buildDropViewSQL(repo *Repository, viewName string, materialized bool) string {
	adapter := repo.GetAdapter()
	dialect := resolveMigrationDialect(repo)
	qViewName := dialect.QuoteIdentifier(viewName)

	switch adapter.(type) {
	case *PostgreSQLAdapter:
		if materialized {
			return fmt.Sprintf("DROP MATERIALIZED VIEW IF EXISTS %s", qViewName)
		}
		return fmt.Sprintf("DROP VIEW IF EXISTS %s", qViewName)
	case *SQLServerAdapter:
		// SQL Server 2014 不支持 DROP VIEW IF EXISTS，兼容处理
		return fmt.Sprintf("IF OBJECT_ID('%s', 'V') IS NOT NULL DROP VIEW %s", viewName, qViewName)
	default:
		return fmt.Sprintf("DROP VIEW IF EXISTS %s", qViewName)
	}
}
