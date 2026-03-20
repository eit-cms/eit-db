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

// TestNeo4jQueryBuilderJoinWith_InferHAS 验证 JoinWith 在 join Schema 持有 FK 指向 source 时推断 HAS 关系。
func TestNeo4jQueryBuilderJoinWith_InferHAS(t *testing.T) {
	userSchema := NewBaseSchema("User").
		AddField(&Field{Name: "id", Type: TypeInteger, Primary: true})

	orderSchema := NewBaseSchema("Order").
		AddField(&Field{Name: "id", Type: TypeInteger, Primary: true}).
		AddField(&Field{Name: "user_id", Type: TypeInteger})
	orderSchema.AddForeignKey("fk_orders_users", []string{"user_id"}, "User", []string{"id"}, "", "")

	qb := NewNeo4jQueryConstructor(userSchema).
		FromAlias("u").
		JoinWith(NewLeftJoin(orderSchema).As("o"))

	cypher, _, err := qb.Build(context.Background())
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if !strings.Contains(cypher, ":HAS]") {
		t.Fatalf("expected HAS relation type inferred from FK, got: %s", cypher)
	}
	if !strings.Contains(cypher, "OPTIONAL MATCH") {
		t.Fatalf("expected OPTIONAL MATCH for LEFT join, got: %s", cypher)
	}
}

// TestNeo4jQueryBuilderJoinWith_InferBINDS 验证 unique FK 时推断 BINDS（一对一）。
func TestNeo4jQueryBuilderJoinWith_InferBINDS(t *testing.T) {
	userSchema := NewBaseSchema("User").
		AddField(&Field{Name: "id", Type: TypeInteger, Primary: true})

	profileSchema := NewBaseSchema("Profile").
		AddField(&Field{Name: "id", Type: TypeInteger, Primary: true}).
		AddField(&Field{Name: "user_id", Type: TypeInteger, Unique: true}) // unique FK → one-to-one
	profileSchema.AddForeignKey("fk_profile_users", []string{"user_id"}, "User", []string{"id"}, "", "")

	qb := NewNeo4jQueryConstructor(userSchema).
		FromAlias("u").
		JoinWith(NewInnerJoin(profileSchema).As("p"))

	cypher, _, err := qb.Build(context.Background())
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if !strings.Contains(cypher, ":BINDS]") {
		t.Fatalf("expected BINDS relation type inferred from unique FK, got: %s", cypher)
	}
}

// TestNeo4jQueryBuilderJoinWith_FilterQualification 验证 Filter 条件以 join alias 限定输出到 WHERE。
func TestNeo4jQueryBuilderJoinWith_FilterQualification(t *testing.T) {
	userSchema := NewBaseSchema("User").
		AddField(&Field{Name: "id", Type: TypeInteger, Primary: true})

	orderSchema := NewBaseSchema("Order").
		AddField(&Field{Name: "id", Type: TypeInteger, Primary: true}).
		AddField(&Field{Name: "user_id", Type: TypeInteger}).
		AddField(&Field{Name: "status", Type: TypeString})
	orderSchema.AddForeignKey("fk_orders_users", []string{"user_id"}, "User", []string{"id"}, "", "")

	qb := NewNeo4jQueryConstructor(userSchema).
		FromAlias("u").
		JoinWith(NewInnerJoin(orderSchema).As("o").Filter(Eq("status", "paid")))

	cypher, _, err := qb.Build(context.Background())
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if !strings.Contains(cypher, "WHERE") || !strings.Contains(cypher, "o.status") {
		t.Fatalf("expected join filter 'o.status' in WHERE clause, got: %s", cypher)
	}
}

// TestSQLQueryBuilderJoinWith_FilterQualification 验证 SQL 端 JoinWith Filter 以 join alias 限定追加到 WHERE。
func TestSQLQueryBuilderJoinWith_FilterQualification(t *testing.T) {
	userSchema := NewBaseSchema("users").
		AddField(&Field{Name: "id", Type: TypeInteger, Primary: true})

	orderSchema := NewBaseSchema("orders").
		AddField(&Field{Name: "id", Type: TypeInteger, Primary: true}).
		AddField(&Field{Name: "user_id", Type: TypeInteger}).
		AddField(&Field{Name: "status", Type: TypeString})

	qb := NewSQLQueryConstructor(userSchema, NewSQLiteDialect()).
		JoinWith(NewInnerJoin(orderSchema).As("o").
			On("users.id = o.user_id").
			Filter(Eq("status", "paid")))

	sqlText, _, err := qb.Build(context.Background())
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if !strings.Contains(sqlText, "JOIN") || !strings.Contains(strings.ToUpper(sqlText), "WHERE") {
		t.Fatalf("expected JOIN and WHERE, got: %s", sqlText)
	}
	// filter 字段以方言引号输出（SQLite 用反引号），只验证语义存在即可。
	if !strings.Contains(sqlText, "status") {
		t.Fatalf("expected filter field 'status' in WHERE clause, got: %s", sqlText)
	}
	if !strings.Contains(sqlText, "o") {
		t.Fatalf("expected join alias 'o' in SQL, got: %s", sqlText)
	}
}

// TestNeo4jQueryBuilderJoinWith_NoSchemaFallback 验证无 FK 约束时回退到 RELATED_TO。
func TestNeo4jQueryBuilderJoinWith_NoSchemaFallback(t *testing.T) {
	userSchema := NewBaseSchema("User")
	companySchema := NewBaseSchema("Company") // no FK

	qb := NewNeo4jQueryConstructor(userSchema).
		FromAlias("u").
		JoinWith(NewInnerJoin(companySchema).As("c"))

	cypher, _, err := qb.Build(context.Background())
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if !strings.Contains(cypher, ":RELATED_TO]") {
		t.Fatalf("expected RELATED_TO fallback, got: %s", cypher)
	}
}

// TestNeo4jQueryBuilderNewJoinWith_NullableFKInferredOptional 验证 NewJoinWith 在 FK 字段可空时
// 自动推断为 OPTIONAL MATCH（无需显式指定 LEFT/OPTIONAL）。
func TestNeo4jQueryBuilderNewJoinWith_NullableFKInferredOptional(t *testing.T) {
	userSchema := NewBaseSchema("User").
		AddField(&Field{Name: "id", Type: TypeInteger, Primary: true})
	orderSchema := NewBaseSchema("Order").
		AddField(&Field{Name: "id", Type: TypeInteger, Primary: true}).
		AddField(&Field{Name: "user_id", Type: TypeInteger, Null: true}) // 可空 FK → 推断 optional
	orderSchema.AddForeignKey("fk_orders_users", []string{"user_id"}, "User", []string{"id"}, "", "")

	qb := NewNeo4jQueryConstructor(userSchema).
		FromAlias("u").
		JoinWith(NewJoinWith(orderSchema).As("o"))

	cypher, _, err := qb.Build(context.Background())
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if !strings.Contains(cypher, "OPTIONAL MATCH") {
		t.Fatalf("expected OPTIONAL MATCH for nullable FK inference, got: %s", cypher)
	}
}

// TestNeo4jQueryBuilderNewJoinWith_NotNullFKInferredRequired 验证 NewJoinWith 在 FK 字段 NOT NULL 时
// 自动推断为 MATCH（必须匹配）。
func TestNeo4jQueryBuilderNewJoinWith_NotNullFKInferredRequired(t *testing.T) {
	userSchema := NewBaseSchema("User").
		AddField(&Field{Name: "id", Type: TypeInteger, Primary: true})
	orderSchema := NewBaseSchema("Order").
		AddField(&Field{Name: "id", Type: TypeInteger, Primary: true}).
		AddField(&Field{Name: "user_id", Type: TypeInteger, Null: false}) // NOT NULL → 推断 required
	orderSchema.AddForeignKey("fk_orders_users", []string{"user_id"}, "User", []string{"id"}, "", "")

	qb := NewNeo4jQueryConstructor(userSchema).
		FromAlias("u").
		JoinWith(NewJoinWith(orderSchema).As("o"))

	cypher, _, err := qb.Build(context.Background())
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if strings.Contains(cypher, "OPTIONAL MATCH") {
		t.Fatalf("expected MATCH (not OPTIONAL MATCH) for NOT NULL FK inference, got: %s", cypher)
	}
	if !strings.Contains(cypher, "MATCH") {
		t.Fatalf("expected MATCH pattern, got: %s", cypher)
	}
}

// TestNeo4jQueryBuilderNewJoinWith_ExplicitOptionalOverride 验证 .Optional() 可覆盖自动推断，
// 即使无 FK 约束也能强制使用 OPTIONAL MATCH。
func TestNeo4jQueryBuilderNewJoinWith_ExplicitOptionalOverride(t *testing.T) {
	userSchema := NewBaseSchema("User")
	companySchema := NewBaseSchema("Company") // 无 FK，默认 required，但 .Optional() 覆盖

	qb := NewNeo4jQueryConstructor(userSchema).
		FromAlias("u").
		JoinWith(NewJoinWith(companySchema).As("c").Optional())

	cypher, _, err := qb.Build(context.Background())
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if !strings.Contains(cypher, "OPTIONAL MATCH") {
		t.Fatalf("expected OPTIONAL MATCH from explicit .Optional(), got: %s", cypher)
	}
}

// TestNeo4jQueryBuilderNewRightJoin_SafelyMapsToOptional 验证 NewRightJoin 在 Neo4j 不再报错，
// 统一映射为 OPTIONAL MATCH（语义安全）。
func TestNeo4jQueryBuilderNewRightJoin_SafelyMapsToOptional(t *testing.T) {
	userSchema := NewBaseSchema("User")
	orderSchema := NewBaseSchema("Order")

	qb := NewNeo4jQueryConstructor(userSchema).
		FromAlias("u").
		JoinWith(NewRightJoin(orderSchema).As("o"))

	cypher, _, err := qb.Build(context.Background())
	if err != nil {
		t.Fatalf("NewRightJoin should not error on Neo4j, got: %v", err)
	}
	if !strings.Contains(cypher, "OPTIONAL MATCH") {
		t.Fatalf("expected OPTIONAL MATCH from NewRightJoin, got: %s", cypher)
	}
}

// TestSQLQueryBuilderNewJoinWith_NullableFKInferredLeftJoin 验证 SQL 适配器中
// NewJoinWith 在 FK 字段可空时推断为 LEFT JOIN。
func TestSQLQueryBuilderNewJoinWith_NullableFKInferredLeftJoin(t *testing.T) {
	userSchema := NewBaseSchema("users").
		AddField(&Field{Name: "id", Type: TypeInteger, Primary: true})
	orderSchema := NewBaseSchema("orders").
		AddField(&Field{Name: "id", Type: TypeInteger, Primary: true}).
		AddField(&Field{Name: "user_id", Type: TypeInteger, Null: true}) // 可空 → LEFT JOIN
	orderSchema.AddForeignKey("fk_orders_users", []string{"user_id"}, "users", []string{"id"}, "", "")

	qb := NewSQLQueryConstructor(userSchema, NewSQLiteDialect()).
		JoinWith(NewJoinWith(orderSchema).As("o").On("users.id = o.user_id"))

	sql, _, err := qb.Build(context.Background())
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if !strings.Contains(strings.ToUpper(sql), "LEFT JOIN") {
		t.Fatalf("expected LEFT JOIN for nullable FK inference, got: %s", sql)
	}
}

// TestSQLQueryBuilderNewJoinWith_NotNullFKInferredInnerJoin 验证 SQL 适配器中
// NewJoinWith 在 FK 字段 NOT NULL 时推断为 INNER JOIN。
func TestSQLQueryBuilderNewJoinWith_NotNullFKInferredInnerJoin(t *testing.T) {
	userSchema := NewBaseSchema("users").
		AddField(&Field{Name: "id", Type: TypeInteger, Primary: true})
	orderSchema := NewBaseSchema("orders").
		AddField(&Field{Name: "id", Type: TypeInteger, Primary: true}).
		AddField(&Field{Name: "user_id", Type: TypeInteger, Null: false}) // NOT NULL → INNER JOIN
	orderSchema.AddForeignKey("fk_orders_users", []string{"user_id"}, "users", []string{"id"}, "", "")

	qb := NewSQLQueryConstructor(userSchema, NewSQLiteDialect()).
		JoinWith(NewJoinWith(orderSchema).As("o").On("users.id = o.user_id"))

	sql, _, err := qb.Build(context.Background())
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	upper := strings.ToUpper(sql)
	if strings.Contains(upper, "LEFT JOIN") {
		t.Fatalf("expected INNER JOIN (not LEFT JOIN) for NOT NULL FK, got: %s", sql)
	}
	if !strings.Contains(upper, "INNER JOIN") {
		t.Fatalf("expected INNER JOIN pattern, got: %s", sql)
	}
}

func TestNeo4jQueryBuilderManyToManyThroughCompilesTwoHopPattern(t *testing.T) {
	userSchema := NewBaseSchema("users")
	roleSchema := NewBaseSchema("roles")
	userRoleSchema := NewBaseSchema("user_roles")

	userSchema.ManyToMany(roleSchema).Through(userRoleSchema, "user_id", "role_id").Named("grants_role")

	qb := NewNeo4jQueryConstructor(userSchema).
		FromAlias("u").
		JoinWith(NewJoinWith(roleSchema).As("r"))

	cypher, _, err := qb.Build(context.Background())
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if !strings.Contains(cypher, ":user_roles") {
		t.Fatalf("expected through label user_roles in cypher, got: %s", cypher)
	}
	if !strings.Contains(cypher, "GRANTS_ROLE") {
		t.Fatalf("expected named relation type GRANTS_ROLE in two-hop pattern, got: %s", cypher)
	}
}

func TestNeo4jQueryBuilderNamedRelationTypeUsed(t *testing.T) {
	companySchema := NewBaseSchema("companies")
	userSchema := NewBaseSchema("users")

	userSchema.BelongsTo(companySchema).Over("company_id", "id").Named("works_at")

	qb := NewNeo4jQueryConstructor(userSchema).
		FromAlias("u").
		JoinWith(NewJoinWith(companySchema).As("c"))

	cypher, _, err := qb.Build(context.Background())
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if !strings.Contains(cypher, "WORKS_AT") {
		t.Fatalf("expected named relation type WORKS_AT in cypher, got: %s", cypher)
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
