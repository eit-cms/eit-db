package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

func generateCmd() *cobra.Command {
	var migrationDir string
	var migrationType string

	cmd := &cobra.Command{
		Use:   "generate [name]",
		Short: "Generate a new migration file",
		Long:  `Creates a new migration file with the given name.`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			return generateMigration(migrationDir, name, migrationType)
		},
	}

	cmd.Flags().StringVarP(&migrationDir, "dir", "d", "migrations", "Directory to store migrations")
	cmd.Flags().StringVarP(&migrationType, "type", "t", "schema", "Migration type: schema or sql")

	return cmd
}

func generateMigration(migrationDir, name, migrationType string) error {
	// 检查 migrations 目录是否存在
	if _, err := os.Stat(migrationDir); os.IsNotExist(err) {
		return fmt.Errorf("migrations directory not found. Run 'eit-db-cli init' first")
	}

	// 生成版本号（时间戳）
	version := time.Now().Format("20060102150405")

	// 清理名称
	name = sanitizeName(name)

	// 生成文件名
	fileName := fmt.Sprintf("%s_%s.go", version, name)
	filePath := filepath.Join(migrationDir, fileName)

	// 生成迁移内容
	var content string
	if migrationType == "sql" {
		content = generateRawSQLMigration(version, name)
	} else {
		content = generateSchemaMigration(version, name)
	}

	// 写入文件
	if err := os.WriteFile(filePath, []byte(content), 0644); err != nil {
		return fmt.Errorf("failed to create migration file: %w", err)
	}

	// 更新 main.go 注册迁移
	if err := updateMainGo(migrationDir, version, name); err != nil {
		return fmt.Errorf("failed to update main.go: %w", err)
	}

	fmt.Printf("✓ Created migration: %s\n", fileName)
	fmt.Printf("\nEdit the migration file and then run:\n")
	fmt.Printf("  cd %s && go run main.go up\n", migrationDir)

	return nil
}

func generateSchemaMigration(version, name string) string {
	functionName := toCamelCase(name)

	return fmt.Sprintf(`package main

import (
	"context"
	
	db "github.com/eit-cms/eit-db"
)

// NewMigration_%s_%s creates the migration
func NewMigration_%s_%s() db.MigrationInterface {
	migration := db.NewSchemaMigration("%s", "%s")

	// Define your schema here
	// Example:
	// userSchema := db.NewBaseSchema("users")
	// userSchema.AddField(&db.Field{
	//     Name:    "id",
	//     Type:    db.TypeInteger,
	//     Primary: true,
	//     Autoinc: true,
	// })
	// userSchema.AddField(&db.Field{
	//     Name: "name",
	//     Type: db.TypeString,
	//     Null: false,
	// })
	// userSchema.AddField(&db.Field{
	//     Name: "email",
	//     Type: db.TypeString,
	//     Null: false,
	//     Unique: true,
	// })
	// userSchema.AddField(&db.Field{
	//     Name:    "created_at",
	//     Type:    db.TypeTime,
	//     Default: "CURRENT_TIMESTAMP",
	// })
	//
	// migration.CreateTable(userSchema)

	return migration
}
`, version, functionName, version, functionName, version, name)
}

func generateRawSQLMigration(version, name string) string {
	functionName := toCamelCase(name)

	return fmt.Sprintf(`package main

import (
	db "github.com/eit-cms/eit-db"
)

// NewMigration_%s_%s creates the migration
func NewMigration_%s_%s() db.MigrationInterface {
	migration := db.NewRawSQLMigration("%s", "%s")

	// Add your SQL statements here
	// Example:
	// migration.AddUpSQL(`+"`"+`
	//     CREATE TABLE users (
	//         id SERIAL PRIMARY KEY,
	//         name VARCHAR(255) NOT NULL,
	//         email VARCHAR(255) NOT NULL UNIQUE,
	//         created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	//     )
	// `+"`"+`)
	//
	// migration.AddDownSQL(`+"`"+`DROP TABLE users`+"`"+`)
	//
	// REQUIRED: bind this raw SQL migration to exactly one adapter.
	// migration.ForAdapter("postgres")
	// Supported values: "postgres", "mysql", "sqlite", "sqlserver".

	return migration
}
`, version, functionName, version, functionName, version, name)
}

func updateMainGo(migrationDir, version, name string) error {
	mainFile := filepath.Join(migrationDir, "main.go")

	// 读取现有内容
	content, err := os.ReadFile(mainFile)
	if err != nil {
		return err
	}

	lines := strings.Split(string(content), "\n")

	// 找到 registerMigrations 函数
	functionName := toCamelCase(name)
	registrationLine := fmt.Sprintf("\trunner.Register(NewMigration_%s_%s())", version, functionName)

	// 查找注册位置
	insertIndex := -1
	for i, line := range lines {
		if strings.Contains(line, "func registerMigrations") {
			// 找到函数开始，继续查找函数体
			for j := i + 1; j < len(lines); j++ {
				if strings.Contains(lines[j], "// Example:") || strings.TrimSpace(lines[j]) == "}" {
					insertIndex = j
					break
				}
			}
			break
		}
	}

	if insertIndex == -1 {
		return fmt.Errorf("could not find insertion point in main.go")
	}

	// 插入注册代码
	newLines := make([]string, 0, len(lines)+1)
	newLines = append(newLines, lines[:insertIndex]...)
	newLines = append(newLines, registrationLine)
	newLines = append(newLines, lines[insertIndex:]...)

	// 写回文件
	newContent := strings.Join(newLines, "\n")
	return os.WriteFile(mainFile, []byte(newContent), 0644)
}

func sanitizeName(name string) string {
	// 转换为小写并替换空格和特殊字符为下划线
	name = strings.ToLower(name)
	reg := regexp.MustCompile(`[^a-z0-9]+`)
	name = reg.ReplaceAllString(name, "_")
	name = strings.Trim(name, "_")
	return name
}

func toCamelCase(s string) string {
	parts := strings.Split(s, "_")
	for i, part := range parts {
		if len(part) > 0 {
			parts[i] = strings.ToUpper(part[:1]) + part[1:]
		}
	}
	return strings.Join(parts, "")
}
