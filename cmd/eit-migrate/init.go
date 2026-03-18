package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

func initCmd() *cobra.Command {
	var migrationDir string

	cmd := &cobra.Command{
		Use:   "init",
		Short: "Initialize a new migration project",
		Long:  `Creates the necessary directory structure and configuration files for migrations.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return initMigrationProject(migrationDir)
		},
	}

	cmd.Flags().StringVarP(&migrationDir, "dir", "d", "migrations", "Directory to store migrations")

	return cmd
}

func initMigrationProject(migrationDir string) error {
	// 创建 migrations 目录
	if err := os.MkdirAll(migrationDir, 0755); err != nil {
		return fmt.Errorf("failed to create migrations directory: %w", err)
	}

	// 创建 main.go 入口文件
	mainFile := filepath.Join(migrationDir, "main.go")
	mainContent := `package main

import (
	"context"
	"fmt"
	"log"
	"os"

	db "github.com/eit-cms/eit-db"
)

func main() {
	// 从环境变量或命令行参数读取配置
	config := &db.Config{
		Adapter:  getEnv("DB_ADAPTER", "postgres"),
		Host:     getEnv("DB_HOST", "localhost"),
		Port:     getEnvInt("DB_PORT", 5432),
		Database: getEnv("DB_NAME", "mydb"),
		Username: getEnv("DB_USER", "postgres"),
		Password: getEnv("DB_PASSWORD", ""),
	}

	// 创建 Repository
	repo, err := db.NewRepository(config)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer repo.Close()

	// 创建 MigrationRunner
	runner := db.NewMigrationRunner(repo)

	// 注册所有迁移
	registerMigrations(runner)

	// 执行命令
	ctx := context.Background()
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run main.go [up|down|status]")
		os.Exit(1)
	}

	command := os.Args[1]
	switch command {
	case "up":
		if err := runner.Up(ctx); err != nil {
			log.Fatalf("Migration failed: %v", err)
		}
		fmt.Println("All migrations completed successfully!")

	case "down":
		if err := runner.Down(ctx); err != nil {
			log.Fatalf("Rollback failed: %v", err)
		}
		fmt.Println("Rollback completed successfully!")

	case "status":
		statuses, err := runner.Status(ctx)
		if err != nil {
			log.Fatalf("Failed to get status: %v", err)
		}
		fmt.Println("\nMigration Status:")
		fmt.Println("================")
		for _, status := range statuses {
			applied := "[ ]"
			appliedAt := ""
			if status.Applied {
				applied = "[✓]"
				appliedAt = fmt.Sprintf(" (applied at %s)", status.AppliedAt.Format("2006-01-02 15:04:05"))
			}
			fmt.Printf("%s %s - %s%s\n", applied, status.Version, status.Description, appliedAt)
		}

	default:
		fmt.Printf("Unknown command: %s\n", command)
		fmt.Println("Available commands: up, down, status")
		os.Exit(1)
	}
}

// registerMigrations 注册所有迁移
// 这个函数会在生成新迁移时自动更新
func registerMigrations(runner *db.MigrationRunner) {
	// Migrations will be registered here
	// Example:
	// runner.Register(NewMigration_20260203000000_create_users())
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		var result int
		fmt.Sscanf(value, "%d", &result)
		return result
	}
	return defaultValue
}
`

	if err := os.WriteFile(mainFile, []byte(mainContent), 0644); err != nil {
		return fmt.Errorf("failed to create main.go: %w", err)
	}

	// 创建 .env.example 文件
	envFile := filepath.Join(migrationDir, ".env.example")
	envContent := `# Database Configuration
DB_ADAPTER=postgres
DB_HOST=localhost
DB_PORT=5432
DB_NAME=mydb
DB_USER=postgres
DB_PASSWORD=
`

	if err := os.WriteFile(envFile, []byte(envContent), 0644); err != nil {
		return fmt.Errorf("failed to create .env.example: %w", err)
	}

	// 创建 README.md
	readmeFile := filepath.Join(migrationDir, "README.md")
	readmeContent := `# Database Migrations

This directory contains database migrations for your project.

## Setup

1. Copy .env.example to .env and configure your database settings:
   ` + "```" + `bash
   cp .env.example .env
   ` + "```" + `

2. Edit .env with your database credentials

## Usage

Run migrations:
` + "```" + `bash
cd migrations
source .env
go run main.go up
` + "```" + `

Rollback last migration:
` + "```" + `bash
go run main.go down
` + "```" + `

Check migration status:
` + "```" + `bash
go run main.go status
` + "```" + `

## Creating New Migrations

From the project root, run:
` + "```" + `bash
eit-db-cli generate create_users_table
` + "```" + `

This will create a new migration file with timestamp prefix.
`

	if err := os.WriteFile(readmeFile, []byte(readmeContent), 0644); err != nil {
		return fmt.Errorf("failed to create README.md: %w", err)
	}

	fmt.Printf("✓ Migration project initialized in '%s'\n", migrationDir)
	fmt.Println("\nNext steps:")
	fmt.Println("1. Edit migrations/.env with your database credentials")
	fmt.Println("2. Generate your first migration:")
	fmt.Printf("   eit-db-cli generate create_users_table\n")
	fmt.Println("3. Run migrations:")
	fmt.Printf("   cd %s && go run main.go up\n", migrationDir)

	return nil
}
