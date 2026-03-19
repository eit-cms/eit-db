package db

import "strings"

func normalizeMigrationAdapterName(name string) string {
	normalized := strings.ToLower(strings.TrimSpace(name))
	switch normalized {
	case "postgresql":
		return "postgres"
	case "mongo":
		return "mongodb"
	default:
		return normalized
	}
}

func currentMigrationAdapterName(repo *Repository) string {
	if repo == nil || repo.GetAdapter() == nil {
		return ""
	}

	switch repo.GetAdapter().(type) {
	case *PostgreSQLAdapter:
		return "postgres"
	case *MySQLAdapter:
		return "mysql"
	case *SQLiteAdapter:
		return "sqlite"
	case *SQLServerAdapter:
		return "sqlserver"
	case *MongoAdapter:
		return "mongodb"
	case *Neo4jAdapter:
		return "neo4j"
	default:
		dialect := resolveMigrationDialect(repo)
		if dialect == nil {
			return ""
		}
		return normalizeMigrationAdapterName(dialect.Name())
	}
}
