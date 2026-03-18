package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
)

type adapterScaffoldOptions struct {
	OutDir        string
	Name          string
	Kind          string
	Package       string
	WithTests     bool
	SplitFeatures bool
	Force         bool
	Interactive   bool
}

func adapterCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "adapter",
		Short: "Adapter provider utilities",
		Long:  "Generate scaffolding and helper assets for custom adapter providers.",
	}

	cmd.AddCommand(adapterGenerateCmd())
	return cmd
}

func adapterGenerateCmd() *cobra.Command {
	opts := &adapterScaffoldOptions{}

	cmd := &cobra.Command{
		Use:   "generate [name]",
		Short: "Generate a custom adapter scaffold",
		Long:  "Generates a compile-ready custom adapter scaffold for SQL or non-SQL backends.",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) > 0 {
				opts.Name = args[0]
			}
			if opts.Interactive {
				if err := fillOptionsInteractively(opts); err != nil {
					return err
				}
			}
			if strings.TrimSpace(opts.Name) == "" {
				return fmt.Errorf("adapter name is required (arg or --interactive)")
			}
			return generateAdapterScaffold(opts)
		},
	}

	cmd.Flags().StringVarP(&opts.OutDir, "dir", "d", "adapter-providers", "Output directory for adapter scaffold")
	cmd.Flags().StringVarP(&opts.Kind, "kind", "k", "mongo-like", "Template kind: sql | mongo-like | neo4j-like")
	cmd.Flags().StringVarP(&opts.Package, "package", "p", "main", "Go package name for generated file")
	cmd.Flags().BoolVar(&opts.WithTests, "with-tests", true, "Generate adapter test scaffold")
	cmd.Flags().BoolVar(&opts.SplitFeatures, "split-features", true, "Generate separate *_features.go and *_query_features.go files")
	cmd.Flags().BoolVarP(&opts.Interactive, "interactive", "i", false, "Interactive mode for guided scaffold generation")
	cmd.Flags().BoolVar(&opts.Force, "force", false, "Overwrite files if they already exist")

	return cmd
}

func fillOptionsInteractively(opts *adapterScaffoldOptions) error {
	reader := bufio.NewReader(os.Stdin)

	name, err := promptInput(reader, "Adapter name", opts.Name)
	if err != nil {
		return err
	}
	opts.Name = name

	kind, err := promptChoice(reader, "Template kind", []string{"sql", "mongo-like", "neo4j-like"}, opts.Kind)
	if err != nil {
		return err
	}
	opts.Kind = kind

	pkg, err := promptInput(reader, "Go package", opts.Package)
	if err != nil {
		return err
	}
	opts.Package = pkg

	outDir, err := promptInput(reader, "Output directory", opts.OutDir)
	if err != nil {
		return err
	}
	opts.OutDir = outDir

	withTests, err := promptYesNo(reader, "Generate test scaffold", opts.WithTests)
	if err != nil {
		return err
	}
	opts.WithTests = withTests

	splitFeatures, err := promptYesNo(reader, "Split features into separate files", opts.SplitFeatures)
	if err != nil {
		return err
	}
	opts.SplitFeatures = splitFeatures

	return nil
}

func promptInput(reader *bufio.Reader, label, defaultVal string) (string, error) {
	fmt.Printf("%s [%s]: ", label, defaultVal)
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimSpace(line)
	if line == "" {
		return defaultVal, nil
	}
	return line, nil
}

func promptChoice(reader *bufio.Reader, label string, choices []string, defaultVal string) (string, error) {
	fmt.Printf("%s %v [%s]: ", label, choices, defaultVal)
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimSpace(strings.ToLower(line))
	if line == "" {
		line = strings.ToLower(defaultVal)
	}
	for _, choice := range choices {
		if line == choice {
			return line, nil
		}
	}
	return "", fmt.Errorf("invalid choice %q", line)
}

func promptYesNo(reader *bufio.Reader, label string, defaultVal bool) (bool, error) {
	defaultText := "y"
	if !defaultVal {
		defaultText = "n"
	}
	fmt.Printf("%s (y/n) [%s]: ", label, defaultText)
	line, err := reader.ReadString('\n')
	if err != nil {
		return false, err
	}
	line = strings.TrimSpace(strings.ToLower(line))
	if line == "" {
		return defaultVal, nil
	}
	return line == "y" || line == "yes", nil
}

func generateAdapterScaffold(opts *adapterScaffoldOptions) error {
	normalizedName := sanitizeName(opts.Name)
	if normalizedName == "" {
		return fmt.Errorf("invalid adapter name")
	}

	kind := strings.ToLower(strings.TrimSpace(opts.Kind))
	supported := map[string]bool{"sql": true, "mongo-like": true, "neo4j-like": true}
	if !supported[kind] {
		return fmt.Errorf("unsupported kind %q, expected one of: sql, mongo-like, neo4j-like", kind)
	}

	pkg := strings.TrimSpace(opts.Package)
	if pkg == "" {
		pkg = "main"
	}

	if err := os.MkdirAll(opts.OutDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	structName := toCamelCase(normalizedName) + "Adapter"
	featureFunc := "baseDatabaseFeatures"
	queryFeatureFunc := "baseQueryFeatures"
	if opts.SplitFeatures {
		featureFunc = "newAdapterDatabaseFeatures"
		queryFeatureFunc = "newAdapterQueryFeatures"
	}

	adapterPath := filepath.Join(opts.OutDir, normalizedName+"_adapter.go")
	adapterContent, err := buildAdapterTemplate(pkg, normalizedName, structName, kind, featureFunc, queryFeatureFunc)
	if err != nil {
		return err
	}
	if err := writeMaybeOverwrite(adapterPath, adapterContent, opts.Force); err != nil {
		return err
	}
	fmt.Printf("✓ Generated adapter scaffold: %s\n", adapterPath)

	if opts.SplitFeatures {
		featuresPath := filepath.Join(opts.OutDir, normalizedName+"_features.go")
		featuresContent := buildFeaturesTemplate(pkg, kind)
		if err := writeMaybeOverwrite(featuresPath, featuresContent, opts.Force); err != nil {
			return err
		}
		fmt.Printf("✓ Generated database features scaffold: %s\n", featuresPath)

		queryFeaturesPath := filepath.Join(opts.OutDir, normalizedName+"_query_features.go")
		queryFeaturesContent := buildQueryFeaturesTemplate(pkg, kind)
		if err := writeMaybeOverwrite(queryFeaturesPath, queryFeaturesContent, opts.Force); err != nil {
			return err
		}
		fmt.Printf("✓ Generated query features scaffold: %s\n", queryFeaturesPath)
	}

	if opts.WithTests {
		testPath := filepath.Join(opts.OutDir, normalizedName+"_adapter_test.go")
		testContent := buildAdapterTestTemplate(pkg, structName)
		if err := writeMaybeOverwrite(testPath, testContent, opts.Force); err != nil {
			return err
		}
		fmt.Printf("✓ Generated adapter test scaffold: %s\n", testPath)
	}

	fmt.Println("\nReferences:")
	fmt.Println("- docs/ADAPTER_PROVIDER_GUIDE.md")
	fmt.Println("- mongo_adapter.go (mongo-like reference)")
	fmt.Println("- neo4j_adapter.go (neo4j-like reference)")

	return nil
}

func writeMaybeOverwrite(path, content string, force bool) error {
	if _, err := os.Stat(path); err == nil && !force {
		return fmt.Errorf("target file already exists: %s (use --force to overwrite)", path)
	}
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		return fmt.Errorf("failed to write %s: %w", path, err)
	}
	return nil
}

func buildAdapterTemplate(pkg, normalizedName, structName, kind, featureFunc, queryFeatureFunc string) (string, error) {
	registerName := normalizedName

	providerLine := "return nil // TODO: provide your QueryConstructorProvider"
	featureCtor := "return " + featureFunc + "()"
	queryFeatureCtor := "return " + queryFeatureFunc + "()"

	switch kind {
	case "sql":
		providerLine = "return db.NewDefaultSQLQueryConstructorProvider(db.NewMySQLDialect())"
	case "mongo-like":
		providerLine = "return db.NewMongoQueryConstructorProvider()"
	case "neo4j-like":
		providerLine = "return db.NewNeo4jQueryConstructorProvider()"
	default:
		return "", fmt.Errorf("unsupported template kind: %s", kind)
	}

	tpl := fmt.Sprintf(`package %s

import (
	"context"
	"database/sql"
	"fmt"

	db "github.com/eit-cms/eit-db"
)

// %s demonstrates a custom adapter scaffold generated by eit-db-cli.
// Reference non-SQL adapters:
// - mongo_adapter.go
// - neo4j_adapter.go
type %s struct {
	connected bool
}

func New%s(config *db.Config) (*%s, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &%s{}, nil
}

func (a *%s) Connect(ctx context.Context, config *db.Config) error {
	a.connected = true
	return nil
}

func (a *%s) Close() error {
	a.connected = false
	return nil
}

func (a *%s) Ping(ctx context.Context) error {
	if !a.connected {
		return fmt.Errorf("adapter not connected")
	}
	return nil
}

func (a *%s) Begin(ctx context.Context, opts ...interface{}) (db.Tx, error) {
	return nil, fmt.Errorf("TODO: implement transaction support or return not-supported error")
}

func (a *%s) Query(ctx context.Context, sql string, args ...interface{}) (*sql.Rows, error) {
	return nil, fmt.Errorf("TODO: implement query support")
}

func (a *%s) QueryRow(ctx context.Context, sql string, args ...interface{}) *sql.Row {
	return nil
}

func (a *%s) Exec(ctx context.Context, sql string, args ...interface{}) (sql.Result, error) {
	return nil, fmt.Errorf("TODO: implement exec support")
}

func (a *%s) GetRawConn() interface{} {
	return nil // TODO: return native backend connection object
}

func (a *%s) RegisterScheduledTask(ctx context.Context, task *db.ScheduledTaskConfig) error {
	return fmt.Errorf("scheduled task not supported")
}

func (a *%s) UnregisterScheduledTask(ctx context.Context, taskName string) error {
	return fmt.Errorf("scheduled task not supported")
}

func (a *%s) ListScheduledTasks(ctx context.Context) ([]*db.ScheduledTaskStatus, error) {
	return nil, fmt.Errorf("scheduled task not supported")
}

func (a *%s) GetQueryBuilderProvider() db.QueryConstructorProvider {
	%s
}

func (a *%s) GetDatabaseFeatures() *db.DatabaseFeatures {
	%s
}

func (a *%s) GetQueryFeatures() *db.QueryFeatures {
	%s
}

func init() {
	_ = db.RegisterAdapterConstructor("%s", New%s)
}
`, pkg, structName, structName,
		structName, structName, structName,
		structName,
		structName,
		structName,
		structName,
		structName,
		structName,
		structName,
		structName,
		structName,
		structName,
		structName,
		structName,
		providerLine,
		structName,
		featureCtor,
		structName,
		queryFeatureCtor,
		registerName, structName)

	return tpl, nil
}

func buildFeaturesTemplate(pkg, kind string) string {
	base := "db.NewMongoDatabaseFeatures()"
	switch kind {
	case "sql":
		base = "db.NewMySQLDatabaseFeatures()"
	case "neo4j-like":
		base = "db.NewNeo4jDatabaseFeatures()"
	}

	return fmt.Sprintf(`package %s

import db "github.com/eit-cms/eit-db"

func newAdapterDatabaseFeatures() *db.DatabaseFeatures {
	features := %s
	// TODO: tune features.Description / FeatureSupport / FallbackStrategies for your backend.
	return features
}
`, pkg, base)
}

func buildQueryFeaturesTemplate(pkg, kind string) string {
	base := "db.NewMongoQueryFeatures()"
	switch kind {
	case "sql":
		base = "db.NewMySQLQueryFeatures()"
	case "neo4j-like":
		base = "db.NewNeo4jQueryFeatures()"
	}

	return fmt.Sprintf(`package %s

import db "github.com/eit-cms/eit-db"

func newAdapterQueryFeatures() *db.QueryFeatures {
	features := %s
	// TODO: tune query capabilities and fallback strategies for your backend.
	return features
}
`, pkg, base)
}

func buildAdapterTestTemplate(pkg, structName string) string {
	return fmt.Sprintf(`package %s

import (
	"context"
	"testing"
)

func Test%s_PingRequiresConnect(t *testing.T) {
	a := &%s{}
	if err := a.Ping(context.Background()); err == nil {
		t.Fatal("expected ping to fail before connect")
	}
}
`, pkg, structName, structName)
}
