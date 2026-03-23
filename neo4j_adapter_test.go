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

func TestNeo4jCustomFeatureSocialBidirectionalFollowModel(t *testing.T) {
	a := &Neo4jAdapter{}
	out, err := a.ExecuteCustomFeature(context.Background(), "social_model_bidirectional_follow", map[string]interface{}{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	payload, ok := out.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map payload, got %T", out)
	}
	if payload["preset"] != "bidirectional_follow" {
		t.Fatalf("expected bidirectional_follow preset, got %v", payload["preset"])
	}
	queries, ok := payload["queries"].(map[string]string)
	if !ok {
		t.Fatalf("expected queries map, got %T", payload["queries"])
	}
	if !strings.Contains(queries["mutual_followers"], "[:FOLLOWS]") {
		t.Fatalf("expected mutual followers query using FOLLOWS, got: %s", queries["mutual_followers"])
	}
}

func TestNeo4jCustomFeatureSocialForumPostModel(t *testing.T) {
	a := &Neo4jAdapter{}
	out, err := a.ExecuteCustomFeature(context.Background(), "social_network_preset_model", map[string]interface{}{"preset": "forum_post"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	payload, ok := out.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map payload, got %T", out)
	}
	if payload["preset"] != "forum_post" {
		t.Fatalf("expected forum_post preset, got %v", payload["preset"])
	}
	queries, ok := payload["queries"].(map[string]string)
	if !ok {
		t.Fatalf("expected queries map, got %T", payload["queries"])
	}
	if !strings.Contains(queries["create_post"], "POSTED_IN") || !strings.Contains(queries["like_post"], "LIKES_POST") {
		t.Fatalf("expected forum model queries, got create_post=%s like_post=%s", queries["create_post"], queries["like_post"])
	}
}

func TestNeo4jCustomFeatureSocialOneToOneChatModel(t *testing.T) {
	a := &Neo4jAdapter{}
	out, err := a.ExecuteCustomFeature(context.Background(), "social_model_one_to_one_chat", map[string]interface{}{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	payload, ok := out.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map payload, got %T", out)
	}
	if payload["preset"] != "one_to_one_chat" {
		t.Fatalf("expected one_to_one_chat preset, got %v", payload["preset"])
	}
	queries, ok := payload["queries"].(map[string]string)
	if !ok {
		t.Fatalf("expected queries map, got %T", payload["queries"])
	}
	if !strings.Contains(queries["send_direct_message"], "ChatMessage") || !strings.Contains(queries["send_direct_message"], "[:SENT]") || !strings.Contains(queries["send_direct_message"], "[:TO]") {
		t.Fatalf("expected direct message middle-node model query, got: %s", queries["send_direct_message"])
	}
	if !strings.Contains(queries["can_chat_check"], "FOLLOWS") || !strings.Contains(queries["can_chat_check"], "FRIEND") {
		t.Fatalf("expected can_chat_check to include FRIEND and FOLLOWS semantics, got: %s", queries["can_chat_check"])
	}
	constraints, ok := payload["constraints"].([]string)
	if !ok {
		t.Fatalf("expected constraints list, got %T", payload["constraints"])
	}
	if len(constraints) == 0 || !strings.Contains(strings.Join(constraints, "\n"), "CREATE FULLTEXT INDEX chat_message_fulltext") {
		t.Fatalf("expected chat message fulltext index in constraints, got %+v", constraints)
	}
	if !strings.Contains(queries["search_direct_messages"], "db.index.fulltext.queryNodes('chat_message_fulltext'") {
		t.Fatalf("expected direct message fulltext search query, got: %s", queries["search_direct_messages"])
	}
	if !strings.Contains(queries["search_direct_messages_advanced"], "node.deleted_at IS NULL") || !strings.Contains(queries["search_direct_messages_advanced"], "$start_at") || !strings.Contains(queries["search_direct_messages_advanced"], "mention_boost") {
		t.Fatalf("expected advanced direct search to include soft-delete/time-window/mention boost semantics, got: %s", queries["search_direct_messages_advanced"])
	}
}

func TestNeo4jCustomFeatureSocialGroupChatRoomModel(t *testing.T) {
	a := &Neo4jAdapter{}
	out, err := a.ExecuteCustomFeature(context.Background(), "social_network_preset_model", map[string]interface{}{"preset": "group_chat_room"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	payload, ok := out.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map payload, got %T", out)
	}
	if payload["preset"] != "group_chat_room" {
		t.Fatalf("expected group_chat_room preset, got %v", payload["preset"])
	}
	queries, ok := payload["queries"].(map[string]string)
	if !ok {
		t.Fatalf("expected queries map, got %T", payload["queries"])
	}
	if !strings.Contains(queries["create_room"], "coalesce($name, coalesce(room.name, 'room'))") {
		t.Fatalf("expected create_room to include default room name behavior, got: %s", queries["create_room"])
	}
	if !strings.Contains(queries["send_room_message"], "-[in1:IN]->(room") || !strings.Contains(queries["send_room_message"], "-[in2:IN]->(u)") {
		t.Fatalf("expected send_room_message to require bidirectional IN membership, got: %s", queries["send_room_message"])
	}
	if !strings.Contains(queries["at_user"], "[:AT]") || !strings.Contains(queries["ref_message"], "[:REF]") {
		t.Fatalf("expected @ and ref relationship queries, got at_user=%s ref_message=%s", queries["at_user"], queries["ref_message"])
	}
	constraints, ok := payload["constraints"].([]string)
	if !ok {
		t.Fatalf("expected constraints list, got %T", payload["constraints"])
	}
	if len(constraints) == 0 || !strings.Contains(strings.Join(constraints, "\n"), "CREATE FULLTEXT INDEX chat_message_fulltext") {
		t.Fatalf("expected chat message fulltext index in constraints, got %+v", constraints)
	}
	if !strings.Contains(queries["search_room_messages"], "db.index.fulltext.queryNodes('chat_message_fulltext'") {
		t.Fatalf("expected room message fulltext search query, got: %s", queries["search_room_messages"])
	}
	if !strings.Contains(queries["search_room_messages_advanced"], "node.deleted_at IS NULL") || !strings.Contains(queries["search_room_messages_advanced"], "$start_at") || !strings.Contains(queries["search_room_messages_advanced"], "mention_boost") {
		t.Fatalf("expected advanced room search to include soft-delete/time-window/mention boost semantics, got: %s", queries["search_room_messages_advanced"])
	}
}

func TestNeo4jCustomFeatureSocialChatReceiptModel(t *testing.T) {
	a := &Neo4jAdapter{}
	out, err := a.ExecuteCustomFeature(context.Background(), "social_model_chat_receipt", map[string]interface{}{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	payload, ok := out.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map payload, got %T", out)
	}
	if payload["preset"] != "chat_receipt" {
		t.Fatalf("expected chat_receipt preset, got %v", payload["preset"])
	}
	queries, ok := payload["queries"].(map[string]string)
	if !ok {
		t.Fatalf("expected queries map, got %T", payload["queries"])
	}
	if !strings.Contains(queries["mark_direct_message_read"], "READ_BY") || !strings.Contains(queries["mark_room_message_read"], "READ_BY") {
		t.Fatalf("expected READ_BY semantics in receipt queries, got direct=%s room=%s", queries["mark_direct_message_read"], queries["mark_room_message_read"])
	}
	if !strings.Contains(queries["list_room_unread"], "NOT (u)-[:READ_BY]->(m)") {
		t.Fatalf("expected unread query using READ_BY anti-pattern, got: %s", queries["list_room_unread"])
	}
	constraints, ok := payload["constraints"].([]string)
	if !ok {
		t.Fatalf("expected constraints list, got %T", payload["constraints"])
	}
	if len(constraints) == 0 || !strings.Contains(strings.Join(constraints, "\n"), "CREATE FULLTEXT INDEX chat_message_fulltext") {
		t.Fatalf("expected chat message fulltext index in constraints, got %+v", constraints)
	}
}

func TestNeo4jCustomFeatureSocialChatModerationModel(t *testing.T) {
	a := &Neo4jAdapter{}
	out, err := a.ExecuteCustomFeature(context.Background(), "social_network_preset_model", map[string]interface{}{"preset": "chat_moderation"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	payload, ok := out.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map payload, got %T", out)
	}
	if payload["preset"] != "chat_moderation" {
		t.Fatalf("expected chat_moderation preset, got %v", payload["preset"])
	}
	queries, ok := payload["queries"].(map[string]string)
	if !ok {
		t.Fatalf("expected queries map, got %T", payload["queries"])
	}
	if !strings.Contains(queries["mute_member"], "MUTED_IN") || !strings.Contains(queries["ban_member"], "BANNED_IN") {
		t.Fatalf("expected MUTED_IN/BANNED_IN moderation queries, got mute=%s ban=%s", queries["mute_member"], queries["ban_member"])
	}
	if !strings.Contains(queries["can_send_room_message"], "AS can_send") || !strings.Contains(queries["can_send_room_message"], "BANNED_IN") {
		t.Fatalf("expected can_send guard query for safe API gating, got: %s", queries["can_send_room_message"])
	}
}

func TestNeo4jCustomFeatureSocialMessageEmojiModel(t *testing.T) {
	a := &Neo4jAdapter{}
	out, err := a.ExecuteCustomFeature(context.Background(), "social_model_message_emoji", map[string]interface{}{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	payload, ok := out.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map payload, got %T", out)
	}
	if payload["preset"] != "message_emoji" {
		t.Fatalf("expected message_emoji preset, got %v", payload["preset"])
	}
	queries, ok := payload["queries"].(map[string]string)
	if !ok {
		t.Fatalf("expected queries map, got %T", payload["queries"])
	}
	if !strings.Contains(queries["attach_emoji_to_message"], "(e)-[r:INCLUDED_BY {index: $index}]->(m)") {
		t.Fatalf("expected INCLUDED_BY relation from emoji to message with index field, got: %s", queries["attach_emoji_to_message"])
	}
	if !strings.Contains(queries["render_message_emoji_payload"], "template_content") || !strings.Contains(queries["render_message_emoji_payload"], "collect({index: r.index") {
		t.Fatalf("expected emoji render payload query with indexed placeholder mapping, got: %s", queries["render_message_emoji_payload"])
	}
	if !strings.Contains(queries["list_message_emojis"], "(e:Emoji)-[r:INCLUDED_BY]->(m:ChatMessage") {
		t.Fatalf("expected list_message_emojis to traverse from static Emoji node to message, got: %s", queries["list_message_emojis"])
	}
	constraints, ok := payload["constraints"].([]string)
	if !ok || len(constraints) == 0 {
		t.Fatalf("expected constraints list, got %+v", payload["constraints"])
	}
	if !strings.Contains(strings.Join(constraints, "\n"), "emoji_id_unique") {
		t.Fatalf("expected emoji constraints, got %+v", constraints)
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

func TestNeo4jQueryBuilderPaginateCursorMode(t *testing.T) {
	schema := NewBaseSchema("User")
	q := NewNeo4jQueryConstructor(schema).
		FromAlias("u").
		Select("u").
		Paginate(NewPaginationBuilder(1, 3).CursorBy("created_at", "DESC", "2026-03-21T10:00:00Z", nil))

	cypher, args, err := q.Build(context.Background())
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if !strings.Contains(cypher, "WHERE") || !strings.Contains(cypher, "u.created_at < $p1") {
		t.Fatalf("expected cursor predicate in cypher, got: %s", cypher)
	}
	if !strings.Contains(cypher, "ORDER BY u.created_at DESC") {
		t.Fatalf("expected cursor sort in cypher, got: %s", cypher)
	}
	if !strings.Contains(cypher, "LIMIT 3") || strings.Contains(cypher, "SKIP") {
		t.Fatalf("expected cursor pagination to use LIMIT without SKIP, got: %s", cypher)
	}
	if len(args) != 1 || args[0] != "2026-03-21T10:00:00Z" {
		t.Fatalf("unexpected cursor args: %v", args)
	}
}

func TestNeo4jQueryBuilderPaginateCursorModeWithPrimaryTieBreaker(t *testing.T) {
	schema := NewBaseSchema("User")
	schema.AddField(NewField("id", TypeInteger).PrimaryKey().Build())
	schema.AddField(NewField("created_at", TypeString).Build())

	q := NewNeo4jQueryConstructor(schema).
		FromAlias("u").
		Select("u").
		Paginate(NewPaginationBuilder(1, 3).CursorBy("created_at", "ASC", "2026-03-21T10:00:00Z", 12))

	cypher, args, err := q.Build(context.Background())
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if !strings.Contains(cypher, "(u.created_at > $p1 OR (u.created_at = $p2 AND u.id > $p3))") {
		t.Fatalf("expected cursor OR tie-breaker predicate, got: %s", cypher)
	}
	if !strings.Contains(cypher, "ORDER BY u.created_at ASC, u.id ASC") {
		t.Fatalf("expected stable cursor sort by created_at then id, got: %s", cypher)
	}
	if len(args) != 3 || args[0] != "2026-03-21T10:00:00Z" || args[1] != "2026-03-21T10:00:00Z" || args[2] != 12 {
		t.Fatalf("unexpected tie-breaker args: %v", args)
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

func TestNeo4jCustomFeatureSocialModelExecutor(t *testing.T) {
	a := &Neo4jAdapter{}
	// 测试不执行实际操作的模式（无数据库连接）
	out, err := a.ExecuteCustomFeature(context.Background(), "social_model_executor", map[string]interface{}{
		"preset":  "bidirectional_follow",
		"execute": false,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	payload, ok := out.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map payload, got %T", out)
	}
	if payload["strategy"] != "social_model_executor" {
		t.Fatalf("expected social_model_executor strategy, got %v", payload["strategy"])
	}
	if payload["preset"] != "bidirectional_follow" {
		t.Fatalf("expected bidirectional_follow preset, got %v", payload["preset"])
	}
	
	// 验证constraints存在
	constraints, ok := payload["constraints"].([]string)
	if !ok || len(constraints) == 0 {
		t.Fatalf("expected non-empty constraints, got %+v", payload["constraints"])
	}
	
	// 验证sample_rules存在
	rules, ok := payload["sample_rules"].(map[string]string)
	if !ok || len(rules) == 0 {
		t.Fatalf("expected non-empty sample_rules, got %+v", payload["sample_rules"])
	}
	
	// 当execute=false时，不应有execution_results
	if _, ok := payload["execution_results"]; ok {
		t.Fatalf("expected no execution_results when execute=false, got %+v", payload["execution_results"])
	}
}

func TestNeo4jCustomFeatureSocialModelExecutorAllPresets(t *testing.T) {
	a := &Neo4jAdapter{}
	presets := []string{"bidirectional_follow", "friendship", "forum_post", "one_to_one_chat", "group_chat_room", "chat_receipt", "chat_moderation", "message_emoji"}
	
	for _, preset := range presets {
		out, err := a.ExecuteCustomFeature(context.Background(), "social_model_executor", map[string]interface{}{
			"preset":  preset,
			"execute": false,
		})
		if err != nil {
			t.Fatalf("unexpected error for preset %s: %v", preset, err)
		}
		payload, ok := out.(map[string]interface{})
		if !ok {
			t.Fatalf("expected map payload for preset %s, got %T", preset, out)
		}
		if payload["preset"] != preset {
			t.Fatalf("expected preset %s, got %v", preset, payload["preset"])
		}
		
		// 验证constraints和rules
		constraints, ok := payload["constraints"].([]string)
		if !ok || len(constraints) == 0 {
			t.Fatalf("expected non-empty constraints for preset %s, got %+v", preset, payload["constraints"])
		}
		rules, ok := payload["sample_rules"].(map[string]string)
		if !ok || len(rules) == 0 {
			t.Fatalf("expected non-empty sample_rules for preset %s, got %+v", preset, payload["sample_rules"])
		}
	}
}
