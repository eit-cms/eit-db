package db

import (
	"context"
	"fmt"
)

// buildSchemaMigrationsSchemaV1 定义 migration.go 使用的日志工具表。
func buildSchemaMigrationsSchemaV1() Schema {
	schema := NewBaseSchema("schema_migrations")
	schema.AddField(&Field{Name: "id", Type: TypeInteger, Primary: true, Autoinc: true, Null: false})
	schema.AddField(
		NewField("version", TypeString).
			Null(false).
			Unique().
			Build(),
	)
	schema.AddField(NewField("description", TypeString).Build())
	schema.AddField(NewField("executed_at", TypeTime).Null(false).Build())
	return schema
}

// buildSchemaMigrationsSchemaV2 定义 migration_v2.go 使用的日志工具表。
func buildSchemaMigrationsSchemaV2() Schema {
	schema := NewBaseSchema("schema_migrations")
	schema.AddField(
		NewField("version", TypeString).
			PrimaryKey().
			Null(false).
			Build(),
	)
	schema.AddField(NewField("applied_at", TypeTime).Null(false).Build())
	return schema
}

// ensureFrameworkTableUsingSchema 通过 Schema Builder + 方言建表器创建框架工具表。
func ensureFrameworkTableUsingSchema(ctx context.Context, repo *Repository, schema Schema) error {
	if repo == nil {
		return fmt.Errorf("repository is nil")
	}
	if schema == nil {
		return fmt.Errorf("schema is nil")
	}

	createSQL := buildCreateTableSQL(repo, schema)
	_, err := repo.Exec(ctx, createSQL)
	return err
}
