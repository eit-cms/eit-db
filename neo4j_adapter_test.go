package db

import (
	"context"
	"os"
	"strings"
	"testing"
)

func TestNeo4jConfigValidationRequiresURIAndUsername(t *testing.T) {
	cfg := &Config{
		Adapter: "neo4j",
		Neo4j: &Neo4jConnectionConfig{
			Database: "neo4j",
		},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected validation error for missing uri/username")
	}
}

func TestNeo4jAdapterFactory(t *testing.T) {
	cfg := &Config{
		Adapter: "neo4j",
		Neo4j: &Neo4jConnectionConfig{
			URI:      "neo4j://localhost:7687",
			Username: "neo4j",
			Password: "password",
			Database: "neo4j",
		},
	}

	repo, err := NewRepository(cfg)
	if err != nil {
		t.Fatalf("NewRepository failed: %v", err)
	}
	if repo == nil {
		t.Fatalf("expected repo, got nil")
	}

	adapter, ok := repo.GetAdapter().(*Neo4jAdapter)
	if !ok {
		t.Fatalf("expected Neo4jAdapter, got %T", repo.GetAdapter())
	}
	if adapter.GetDatabaseFeatures() == nil {
		t.Fatalf("expected database features")
	}
	if adapter.GetQueryFeatures() == nil {
		t.Fatalf("expected query features")
	}
}

func TestNeo4jAdapterPing(t *testing.T) {
	uri := os.Getenv("NEO4J_URI")
	user := os.Getenv("NEO4J_USER")
	password := os.Getenv("NEO4J_PASSWORD")
	if uri == "" || user == "" || password == "" {
		t.Skip("NEO4J_URI/NEO4J_USER/NEO4J_PASSWORD not set; skipping integration test")
	}

	cfg := &Config{
		Adapter: "neo4j",
		Neo4j: &Neo4jConnectionConfig{
			URI:      uri,
			Username: user,
			Password: password,
			Database: "neo4j",
		},
	}

	repo, err := NewRepository(cfg)
	if err != nil {
		t.Fatalf("NewRepository failed: %v", err)
	}
	defer repo.Close()

	if err := repo.Connect(context.Background()); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	if err := repo.Ping(context.Background()); err != nil {
		t.Fatalf("Ping failed: %v", err)
	}
}

func TestGetQueryFeaturesNeo4j(t *testing.T) {
	qf := GetQueryFeatures("neo4j")
	if !qf.SupportsRecursiveCTE {
		t.Fatalf("expected neo4j recursive optimization mapping to be true")
	}
	if !qf.SupportsFullTextSearch {
		t.Fatalf("expected neo4j full text support")
	}
}

func TestGetDatabaseFeaturesNeo4jRelationSemantics(t *testing.T) {
	f := NewNeo4jDatabaseFeatures()
	if !f.SupportsForeignKeys {
		t.Fatalf("expected neo4j to expose relation-native foreign key semantics")
	}
	if !f.SupportsCompositeForeignKeys {
		t.Fatalf("expected neo4j to expose relation-native composite foreign key semantics")
	}
	if f.GetFallbackStrategy("foreign_keys") != FallbackNone {
		t.Fatalf("expected no application-layer fallback for neo4j foreign keys")
	}
}

func TestNeo4jCustomFeatureRelationshipAssociationQuery(t *testing.T) {
	a := &Neo4jAdapter{}
	if !a.HasCustomFeatureImplementation("relationship_association_query") {
		t.Fatalf("expected relationship_association_query custom feature")
	}
	out, err := a.ExecuteCustomFeature(context.Background(), "relationship_association_query", map[string]interface{}{
		"from_label":   "User",
		"to_label":     "User",
		"relationship": "KNOWS",
		"direction":    "both",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	payload, ok := out.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map payload, got %T", out)
	}
	cypher, _ := payload["cypher"].(string)
	if !strings.Contains(cypher, "-[r:KNOWS]-") {
		t.Fatalf("expected undirected relationship pattern, got: %s", cypher)
	}
}

func TestNeo4jCustomFeatureRelationshipWithPayload(t *testing.T) {
	a := &Neo4jAdapter{}
	out, err := a.ExecuteCustomFeature(context.Background(), "relationship_with_payload", map[string]interface{}{
		"from_label":   "User",
		"to_label":     "User",
		"relationship": "KNOWS_REQUEST",
		"from_id":      "u1",
		"to_id":        "u2",
		"payload": map[string]interface{}{
			"message": "hi",
			"status":  "pending",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	payload, ok := out.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map payload, got %T", out)
	}
	cypher, _ := payload["cypher"].(string)
	if !strings.Contains(cypher, "SET r += $payload") {
		t.Fatalf("expected relationship payload merge semantics, got: %s", cypher)
	}
}

func TestNeo4jCustomFeatureBidirectionalRelationshipSemantics(t *testing.T) {
	a := &Neo4jAdapter{}
	out, err := a.ExecuteCustomFeature(context.Background(), "bidirectional_relationship_semantics", map[string]interface{}{
		"from_id": "u1",
		"to_id":   "u2",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	payload, ok := out.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map payload, got %T", out)
	}
	cypher, _ := payload["cypher"].(string)
	if !strings.Contains(cypher, "DELETE req") || !strings.Contains(cypher, "MERGE (b)-[f2:KNOWS]->(a)") {
		t.Fatalf("expected request-to-friend bidirectional transition semantics, got: %s", cypher)
	}
}

func TestNeo4jQueryBuilderProvider(t *testing.T) {
	a := &Neo4jAdapter{}
	p := a.GetQueryBuilderProvider()
	if p == nil {
		t.Fatalf("expected neo4j query builder provider")
	}

	cap := p.GetCapabilities()
	if cap == nil {
		t.Fatalf("expected capabilities")
	}
	if !cap.SupportsNativeQuery || cap.NativeQueryLang != "cypher" {
		t.Fatalf("expected native cypher support, got %+v", cap)
	}
}

func TestNeo4jQueryBuilderBuildBasicCypher(t *testing.T) {
	schema := NewBaseSchema("User")
	provider := NewNeo4jQueryConstructorProvider()

	q := provider.NewQueryConstructor(schema).
		FromAlias("u").
		Where(Eq("name", "alice")).
		Where(Gte("age", 18)).
		Select("u.id", "u.name").
		OrderBy("u.created_at", "DESC").
		Offset(10).
		Limit(20)

	cypher, args, err := q.Build(context.Background())
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	checks := []string{
		"MATCH (u:User)",
		"WHERE u.name = $p1 AND u.age >= $p2",
		"RETURN u.id, u.name",
		"ORDER BY u.created_at DESC",
		"SKIP 10 LIMIT 20",
	}
	for _, want := range checks {
		if !strings.Contains(cypher, want) {
			t.Fatalf("expected cypher to contain %q, got %s", want, cypher)
		}
	}

	if len(args) != 2 {
		t.Fatalf("expected 2 args, got %d (%v)", len(args), args)
	}
	if args[0] != "alice" || args[1] != 18 {
		t.Fatalf("unexpected args: %v", args)
	}
}

func TestNeo4jQueryBuilderJoinAndOptionalMatch(t *testing.T) {
	schema := NewBaseSchema("User")
	q := NewNeo4jQueryConstructor(schema).
		FromAlias("u").
		Join("Company", "WORKS_AT", "c").
		LeftJoin("Department", "BELONGS_TO", "d").
		Select("u", "c", "d")

	cypher, _, err := q.Build(context.Background())
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if !strings.Contains(cypher, "MATCH (u)-[r1:WORKS_AT]->(c:Company)") {
		t.Fatalf("expected relationship match in cypher, got: %s", cypher)
	}
	if !strings.Contains(cypher, "OPTIONAL MATCH (u)-[r2:BELONGS_TO]->(d:Department)") {
		t.Fatalf("expected optional relationship match in cypher, got: %s", cypher)
	}
}

func TestNeo4jQueryBuilderRejectsRightJoin(t *testing.T) {
	schema := NewBaseSchema("User")
	q := NewNeo4jQueryConstructor(schema).
		FromAlias("u").
		RightJoin("Company", "WORKS_AT", "c")

	_, _, err := q.Build(context.Background())
	if err == nil {
		t.Fatalf("expected error for RIGHT JOIN semantics")
	}
}

func TestNeo4jQueryBuilderJoinWithSQLOnClauseFallsBackRelation(t *testing.T) {
	schema := NewBaseSchema("User")
	q := NewNeo4jQueryConstructor(schema).
		FromAlias("u").
		Join("Company", "u.company_id = c.id", "c").
		Select("u", "c")

	cypher, _, err := q.Build(context.Background())
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if !strings.Contains(cypher, "MATCH (u)-[r1:RELATED_TO]->(c:Company)") {
		t.Fatalf("expected fallback relation type RELATED_TO, got: %s", cypher)
	}
}

func TestNeo4jAdapterQueryCypherRequiresConnection(t *testing.T) {
	a := &Neo4jAdapter{}
	_, err := a.QueryCypher(context.Background(), "MATCH (n) RETURN n LIMIT 1", nil)
	if err == nil {
		t.Fatalf("expected not connected error")
	}
}

func TestNeo4jAdapterExecCypherRequiresConnection(t *testing.T) {
	a := &Neo4jAdapter{}
	_, err := a.ExecCypher(context.Background(), "CREATE (n:User {name: $name})", map[string]interface{}{"name": "alice"})
	if err == nil {
		t.Fatalf("expected not connected error")
	}
}

func TestRepositoryCypherHelpersRequireNeo4jAdapter(t *testing.T) {
	repo := &Repository{adapter: &SQLiteAdapter{}}

	if _, err := repo.QueryCypher(context.Background(), "MATCH (n) RETURN n", nil); err == nil {
		t.Fatalf("expected non-neo4j adapter error for QueryCypher")
	}
	if _, err := repo.ExecCypher(context.Background(), "CREATE (n:X)", nil); err == nil {
		t.Fatalf("expected non-neo4j adapter error for ExecCypher")
	}
}
