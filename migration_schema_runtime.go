package db

import (
	"context"
	"fmt"
	"strings"
	"unicode"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func supportsSQLDDL(repo *Repository) bool {
	if repo == nil || repo.GetAdapter() == nil {
		return false
	}
	switch repo.GetAdapter().(type) {
	case *MongoAdapter, *Neo4jAdapter:
		return false
	default:
		return true
	}
}

func executeSchemaCreate(ctx context.Context, repo *Repository, schema Schema) error {
	if repo == nil || repo.GetAdapter() == nil {
		return fmt.Errorf("schema create requires initialized repository")
	}

	switch adapter := repo.GetAdapter().(type) {
	case *MongoAdapter:
		return createMongoCollectionFromSchema(ctx, adapter, schema)
	case *Neo4jAdapter:
		return createNeo4jSchemaFromSchema(ctx, adapter, schema)
	default:
		createSQL := buildCreateTableSQL(repo, schema)
		_, err := repo.Exec(ctx, createSQL)
		return err
	}
}

func executeSchemaDrop(ctx context.Context, repo *Repository, schema Schema) error {
	if repo == nil || repo.GetAdapter() == nil {
		return fmt.Errorf("schema drop requires initialized repository")
	}

	switch adapter := repo.GetAdapter().(type) {
	case *MongoAdapter:
		return dropMongoCollectionFromSchema(ctx, adapter, schema)
	case *Neo4jAdapter:
		return dropNeo4jSchemaFromSchema(ctx, adapter, schema)
	default:
		dropSQL := buildDropTableSQL(repo, schema.TableName())
		_, err := repo.Exec(ctx, dropSQL)
		return err
	}
}

func createMongoCollectionFromSchema(ctx context.Context, adapter *MongoAdapter, schema Schema) error {
	if adapter == nil || adapter.client == nil {
		return fmt.Errorf("mongodb adapter not connected")
	}
	if strings.TrimSpace(adapter.database) == "" {
		return fmt.Errorf("mongodb database name is empty")
	}

	collectionName := strings.TrimSpace(schema.TableName())
	if collectionName == "" {
		return fmt.Errorf("schema table name is empty")
	}

	db := adapter.client.Database(adapter.database)
	existing, err := db.ListCollectionNames(ctx, bson.M{"name": collectionName})
	if err != nil {
		return err
	}
	if len(existing) == 0 {
		if err := db.CreateCollection(ctx, collectionName); err != nil {
			return err
		}
	}

	for _, field := range schema.Fields() {
		if !field.Primary && !field.Unique {
			continue
		}
		if strings.TrimSpace(field.Name) == "" {
			continue
		}

		name := fmt.Sprintf("uk_%s_%s", sanitizeMigrationIdentifier(collectionName), sanitizeMigrationIdentifier(field.Name))
		_, err := db.Collection(collectionName).Indexes().CreateOne(ctx, mongo.IndexModel{
			Keys: bson.D{{Key: field.Name, Value: 1}},
			Options: options.Index().
				SetName(name).
				SetUnique(true),
		})
		if err != nil {
			return fmt.Errorf("failed to create mongodb unique index %s: %w", name, err)
		}
	}

	return nil
}

func dropMongoCollectionFromSchema(ctx context.Context, adapter *MongoAdapter, schema Schema) error {
	if adapter == nil || adapter.client == nil {
		return fmt.Errorf("mongodb adapter not connected")
	}
	if strings.TrimSpace(adapter.database) == "" {
		return fmt.Errorf("mongodb database name is empty")
	}

	collectionName := strings.TrimSpace(schema.TableName())
	if collectionName == "" {
		return fmt.Errorf("schema table name is empty")
	}

	err := adapter.client.Database(adapter.database).Collection(collectionName).Drop(ctx)
	if err != nil && !strings.Contains(strings.ToLower(err.Error()), "ns not found") {
		return err
	}
	return nil
}

func createNeo4jSchemaFromSchema(ctx context.Context, adapter *Neo4jAdapter, schema Schema) error {
	if adapter == nil || adapter.driver == nil {
		return fmt.Errorf("neo4j adapter not connected")
	}

	label := toNeo4jLabel(schema.TableName())
	if label == "" {
		return fmt.Errorf("schema table name is empty")
	}

	for _, field := range schema.Fields() {
		if !field.Primary && !field.Unique {
			continue
		}
		if strings.TrimSpace(field.Name) == "" {
			continue
		}

		constraintName := fmt.Sprintf("uk_%s_%s", sanitizeMigrationIdentifier(label), sanitizeMigrationIdentifier(field.Name))
		cypher := fmt.Sprintf(
			"CREATE CONSTRAINT `%s` IF NOT EXISTS FOR (n:`%s`) REQUIRE n.`%s` IS UNIQUE",
			escapeNeo4jIdentifier(constraintName),
			escapeNeo4jIdentifier(label),
			escapeNeo4jIdentifier(field.Name),
		)
		if _, err := adapter.ExecCypher(ctx, cypher, nil); err != nil {
			return fmt.Errorf("failed to create neo4j constraint %s: %w", constraintName, err)
		}
	}

	return nil
}

func dropNeo4jSchemaFromSchema(ctx context.Context, adapter *Neo4jAdapter, schema Schema) error {
	if adapter == nil || adapter.driver == nil {
		return fmt.Errorf("neo4j adapter not connected")
	}

	label := toNeo4jLabel(schema.TableName())
	if label == "" {
		return nil
	}

	for _, field := range schema.Fields() {
		if !field.Primary && !field.Unique {
			continue
		}
		if strings.TrimSpace(field.Name) == "" {
			continue
		}

		constraintName := fmt.Sprintf("uk_%s_%s", sanitizeMigrationIdentifier(label), sanitizeMigrationIdentifier(field.Name))
		cypher := fmt.Sprintf("DROP CONSTRAINT `%s` IF EXISTS", escapeNeo4jIdentifier(constraintName))
		if _, err := adapter.ExecCypher(ctx, cypher, nil); err != nil {
			return fmt.Errorf("failed to drop neo4j constraint %s: %w", constraintName, err)
		}
	}

	return nil
}

func toNeo4jLabel(tableName string) string {
	trimmed := strings.TrimSpace(tableName)
	if trimmed == "" {
		return ""
	}

	parts := strings.Split(trimmed, ".")
	base := strings.TrimSpace(parts[len(parts)-1])
	base = strings.Trim(base, "`\"[]")
	label := sanitizeMigrationIdentifier(base)
	if label == "" {
		return ""
	}
	if unicode.IsDigit([]rune(label)[0]) {
		return "L_" + label
	}
	return label
}

func sanitizeMigrationIdentifier(input string) string {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return ""
	}

	var b strings.Builder
	for _, r := range trimmed {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			b.WriteRune(r)
			continue
		}
		b.WriteByte('_')
	}
	return b.String()
}

func escapeNeo4jIdentifier(input string) string {
	return strings.ReplaceAll(input, "`", "``")
}
