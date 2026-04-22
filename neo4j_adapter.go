package db

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// Neo4jAdapter Neo4j 适配器（最小可用版本：连接/健康检查/能力声明）。
type Neo4jAdapter struct {
	driver       neo4j.DriverWithContext
	uri          string
	username     string
	password     string
	database     string
	socialConfig *Neo4jSocialNetworkConfig
}

// CypherWriteSummary 表示 Neo4j 写入执行摘要。
type CypherWriteSummary struct {
	NodesCreated         int
	NodesDeleted         int
	RelationshipsCreated int
	RelationshipsDeleted int
	PropertiesSet        int
	LabelsAdded          int
	LabelsRemoved        int
}

// NewNeo4jAdapter 创建 Neo4jAdapter（不建立连接）。
func NewNeo4jAdapter(config *Config) (*Neo4jAdapter, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	resolved := config.ResolvedNeo4jConfig()
	return &Neo4jAdapter{
		uri:          resolved.URI,
		username:     resolved.Username,
		password:     resolved.Password,
		database:     resolved.Database,
		socialConfig: resolved.SocialNetwork,
	}, nil
}

// Connect 建立 Neo4j 连接。
func (a *Neo4jAdapter) Connect(ctx context.Context, config *Config) error {
	if a.driver != nil {
		return nil
	}

	uri := a.uri
	username := a.username
	password := a.password
	database := a.database

	if config != nil {
		if err := config.Validate(); err != nil {
			return err
		}
		resolved := config.ResolvedNeo4jConfig()
		uri = resolved.URI
		username = resolved.Username
		password = resolved.Password
		database = resolved.Database
		a.socialConfig = resolved.SocialNetwork
	}

	driver, err := neo4j.NewDriverWithContext(uri, neo4j.BasicAuth(username, password, ""))
	if err != nil {
		return fmt.Errorf("failed to connect to neo4j: %w", err)
	}

	verifyCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := driver.VerifyConnectivity(verifyCtx); err != nil {
		_ = driver.Close(ctx)
		return fmt.Errorf("neo4j connectivity check failed: %w", err)
	}

	a.driver = driver
	a.uri = uri
	a.username = username
	a.password = password
	a.database = database
	return nil
}

// Close 关闭 Neo4j 连接。
func (a *Neo4jAdapter) Close() error {
	if a.driver == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := a.driver.Close(ctx)
	a.driver = nil
	return err
}

// Ping 检查 Neo4j 连接可用性。
func (a *Neo4jAdapter) Ping(ctx context.Context) error {
	if a.driver == nil {
		return fmt.Errorf("neo4j driver not connected")
	}
	return a.driver.VerifyConnectivity(ctx)
}

// Begin Neo4j 不支持 SQL 事务接口。
func (a *Neo4jAdapter) Begin(ctx context.Context, opts ...interface{}) (Tx, error) {
	return nil, fmt.Errorf("neo4j: SQL transaction interface is not supported")
}

// Query Neo4j 不支持 SQL Query 接口。
func (a *Neo4jAdapter) Query(ctx context.Context, sql string, args ...interface{}) (*sql.Rows, error) {
	return nil, fmt.Errorf("neo4j: sql query not supported; use custom feature or native driver")
}

// QueryRow Neo4j 不支持 SQL QueryRow 接口。
func (a *Neo4jAdapter) QueryRow(ctx context.Context, sql string, args ...interface{}) *sql.Row {
	return nil
}

// Exec Neo4j 不支持 SQL Exec 接口。
func (a *Neo4jAdapter) Exec(ctx context.Context, sql string, args ...interface{}) (sql.Result, error) {
	return nil, fmt.Errorf("neo4j: sql exec not supported; use custom feature or native driver")
}

// GetRawConn 返回底层 Neo4j Driver。
func (a *Neo4jAdapter) GetRawConn() interface{} {
	return a.driver
}

// RegisterScheduledTask Neo4j 暂不支持定时任务接口。
func (a *Neo4jAdapter) RegisterScheduledTask(ctx context.Context, task *ScheduledTaskConfig) error {
	return NewScheduledTaskFallbackErrorWithReason("neo4j", ScheduledTaskFallbackReasonAdapterUnsupported, "native scheduled tasks not supported")
}

// UnregisterScheduledTask Neo4j 暂不支持定时任务接口。
func (a *Neo4jAdapter) UnregisterScheduledTask(ctx context.Context, taskName string) error {
	return NewScheduledTaskFallbackErrorWithReason("neo4j", ScheduledTaskFallbackReasonAdapterUnsupported, "native scheduled tasks not supported")
}

// ListScheduledTasks Neo4j 暂不支持定时任务接口。
func (a *Neo4jAdapter) ListScheduledTasks(ctx context.Context) ([]*ScheduledTaskStatus, error) {
	return nil, NewScheduledTaskFallbackErrorWithReason("neo4j", ScheduledTaskFallbackReasonAdapterUnsupported, "native scheduled tasks not supported")
}

// QueryCypher 执行读类型 Cypher，并返回记录列表。
func (a *Neo4jAdapter) QueryCypher(ctx context.Context, cypher string, params map[string]interface{}) ([]map[string]interface{}, error) {
	if a.driver == nil {
		return nil, fmt.Errorf("neo4j driver not connected")
	}
	if strings.TrimSpace(cypher) == "" {
		return nil, fmt.Errorf("cypher cannot be empty")
	}
	if params == nil {
		params = map[string]interface{}{}
	}

	session := a.driver.NewSession(ctx, neo4j.SessionConfig{
		DatabaseName: a.database,
		AccessMode:   neo4j.AccessModeRead,
	})
	defer func() { _ = session.Close(ctx) }()

	out, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
		result, runErr := tx.Run(ctx, cypher, params)
		if runErr != nil {
			return nil, runErr
		}
		rows := make([]map[string]interface{}, 0)
		for result.Next(ctx) {
			record := result.Record()
			rows = append(rows, record.AsMap())
		}
		if consumeErr := result.Err(); consumeErr != nil {
			return nil, consumeErr
		}
		return rows, nil
	})
	if err != nil {
		return nil, err
	}

	rows, ok := out.([]map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("neo4j query returned unexpected result type: %T", out)
	}
	return rows, nil
}

// ExecCypher 执行写类型 Cypher，并返回写入摘要。
func (a *Neo4jAdapter) ExecCypher(ctx context.Context, cypher string, params map[string]interface{}) (*CypherWriteSummary, error) {
	if a.driver == nil {
		return nil, fmt.Errorf("neo4j driver not connected")
	}
	if strings.TrimSpace(cypher) == "" {
		return nil, fmt.Errorf("cypher cannot be empty")
	}
	if params == nil {
		params = map[string]interface{}{}
	}

	session := a.driver.NewSession(ctx, neo4j.SessionConfig{
		DatabaseName: a.database,
		AccessMode:   neo4j.AccessModeWrite,
	})
	defer func() { _ = session.Close(ctx) }()

	out, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
		result, runErr := tx.Run(ctx, cypher, params)
		if runErr != nil {
			return nil, runErr
		}
		summary, consumeErr := result.Consume(ctx)
		if consumeErr != nil {
			return nil, consumeErr
		}
		c := summary.Counters()
		return &CypherWriteSummary{
			NodesCreated:         c.NodesCreated(),
			NodesDeleted:         c.NodesDeleted(),
			RelationshipsCreated: c.RelationshipsCreated(),
			RelationshipsDeleted: c.RelationshipsDeleted(),
			PropertiesSet:        c.PropertiesSet(),
			LabelsAdded:          c.LabelsAdded(),
			LabelsRemoved:        c.LabelsRemoved(),
		}, nil
	})
	if err != nil {
		return nil, err
	}

	summary, ok := out.(*CypherWriteSummary)
	if !ok {
		return nil, fmt.Errorf("neo4j exec returned unexpected summary type: %T", out)
	}
	return summary, nil
}

// QueryCypher 在 Repository 上执行 Neo4j 读查询。
func (r *Repository) QueryCypher(ctx context.Context, cypher string, params map[string]interface{}) ([]map[string]interface{}, error) {
	r.mu.RLock()
	adapter := r.adapter
	r.mu.RUnlock()
	if adapter == nil {
		return nil, fmt.Errorf("adapter is not initialized")
	}
	neo, ok := adapter.(*Neo4jAdapter)
	if !ok {
		return nil, fmt.Errorf("query cypher requires neo4j adapter, got %T", adapter)
	}
	return neo.QueryCypher(ctx, cypher, params)
}

// ExecCypher 在 Repository 上执行 Neo4j 写查询。
func (r *Repository) ExecCypher(ctx context.Context, cypher string, params map[string]interface{}) (*CypherWriteSummary, error) {
	r.mu.RLock()
	adapter := r.adapter
	r.mu.RUnlock()
	if adapter == nil {
		return nil, fmt.Errorf("adapter is not initialized")
	}
	neo, ok := adapter.(*Neo4jAdapter)
	if !ok {
		return nil, fmt.Errorf("exec cypher requires neo4j adapter, got %T", adapter)
	}
	return neo.ExecCypher(ctx, cypher, params)
}

// GetQueryBuilderProvider 返回 Neo4j Cypher Query Builder Provider。
func (a *Neo4jAdapter) GetQueryBuilderProvider() QueryConstructorProvider {
	return NewNeo4jQueryConstructorProvider()
}

// GetDatabaseFeatures 返回 Neo4j 数据库特性声明。
func (a *Neo4jAdapter) GetDatabaseFeatures() *DatabaseFeatures {
	return NewNeo4jDatabaseFeatures()
}

// GetQueryFeatures 返回 Neo4j 查询特性声明。
func (a *Neo4jAdapter) GetQueryFeatures() *QueryFeatures {
	return NewNeo4jQueryFeatures()
}

// InspectFullTextRuntime 探测 Neo4j 全文能力（需要全文索引）。
func (a *Neo4jAdapter) InspectFullTextRuntime(ctx context.Context) (*FullTextRuntimeCapability, error) {
	available := a.driver != nil
	notes := "Neo4j supports full-text via fulltext indexes and procedures"
	if !available {
		notes = "Neo4j driver not connected; full-text capability assumed but runtime verification skipped"
	}
	return &FullTextRuntimeCapability{
		NativeSupported:       true,
		PluginChecked:         available,
		PluginAvailable:       available,
		PluginName:            "neo4j_fulltext_index",
		TokenizationSupported: true,
		TokenizationMode:      "native",
		Notes:                 notes,
	}, nil
}

// HasCustomFeatureImplementation 声明 Neo4j 可用的自定义能力。
func (a *Neo4jAdapter) HasCustomFeatureImplementation(feature string) bool {
	switch feature {
	case "graph_traversal", "document_join", "recursive_query", "full_text_search", "tokenized_full_text_search", "relationship_association_query", "relationship_with_payload", "bidirectional_relationship_semantics", "social_network_preset_model", "social_model_bidirectional_follow", "social_model_friendship", "social_model_forum_post", "social_model_one_to_one_chat", "social_model_group_chat_room", "social_model_chat_receipt", "social_model_chat_moderation", "social_model_message_emoji", "social_model_executor":
		return true
	default:
		return false
	}
}

// ExecuteCustomFeature 执行 Neo4j 自定义能力（返回可执行 Cypher/说明）。
func (a *Neo4jAdapter) ExecuteCustomFeature(ctx context.Context, feature string, input map[string]interface{}) (interface{}, error) {
	switch feature {
	case "graph_traversal", "recursive_query":
		startLabel, _ := input["start_label"].(string)
		relType, _ := input["relationship"].(string)
		depth, _ := input["max_depth"].(int)
		if depth <= 0 {
			depth = 3
		}
		if strings.TrimSpace(startLabel) == "" || strings.TrimSpace(relType) == "" {
			return nil, fmt.Errorf("neo4j graph_traversal requires start_label and relationship")
		}
		cypher := fmt.Sprintf("MATCH p=(n:%s)-[:%s*1..%d]->(m) RETURN p", startLabel, relType, depth)
		return map[string]interface{}{
			"engine":   "neo4j",
			"strategy": "native_graph_traversal",
			"cypher":   cypher,
		}, nil
	case "document_join":
		leftLabel, _ := input["left_label"].(string)
		rightLabel, _ := input["right_label"].(string)
		relType, _ := input["relationship"].(string)
		if leftLabel == "" || rightLabel == "" || relType == "" {
			return nil, fmt.Errorf("neo4j document_join requires left_label, right_label and relationship")
		}
		cypher := fmt.Sprintf("MATCH (a:%s)-[r:%s]->(b:%s) RETURN a, r, b", leftLabel, relType, rightLabel)
		return map[string]interface{}{
			"engine":   "neo4j",
			"strategy": "pattern_match_join",
			"cypher":   cypher,
		}, nil
	case "full_text_search", "tokenized_full_text_search":
		indexName, _ := input["index"].(string)
		query, _ := input["query"].(string)
		if strings.TrimSpace(indexName) == "" || strings.TrimSpace(query) == "" {
			return nil, fmt.Errorf("neo4j full_text_search requires index and query")
		}
		cypher := "CALL db.index.fulltext.queryNodes($index, $query) YIELD node, score RETURN node, score"
		return map[string]interface{}{
			"engine":   "neo4j",
			"strategy": "native_fulltext",
			"cypher":   cypher,
			"params": map[string]interface{}{
				"index": indexName,
				"query": query,
			},
		}, nil
	case "relationship_association_query":
		fromLabel, _ := input["from_label"].(string)
		toLabel, _ := input["to_label"].(string)
		relType, _ := input["relationship"].(string)
		direction, _ := input["direction"].(string)
		if strings.TrimSpace(fromLabel) == "" || strings.TrimSpace(toLabel) == "" || strings.TrimSpace(relType) == "" {
			return nil, fmt.Errorf("neo4j relationship_association_query requires from_label, to_label and relationship")
		}
		relPattern := "-[r:" + relType + "]->"
		switch strings.ToLower(strings.TrimSpace(direction)) {
		case "in":
			relPattern = "<-[r:" + relType + "]-"
		case "both", "bidirectional", "undirected":
			relPattern = "-[r:" + relType + "]-"
		}
		cypher := fmt.Sprintf("MATCH (a:%s)%s(b:%s) RETURN a, r, b", fromLabel, relPattern, toLabel)
		return map[string]interface{}{
			"engine":   "neo4j",
			"strategy": "relationship_association_query",
			"cypher":   cypher,
		}, nil
	case "relationship_with_payload":
		fromLabel, _ := input["from_label"].(string)
		toLabel, _ := input["to_label"].(string)
		relType, _ := input["relationship"].(string)
		if strings.TrimSpace(fromLabel) == "" || strings.TrimSpace(toLabel) == "" || strings.TrimSpace(relType) == "" {
			return nil, fmt.Errorf("neo4j relationship_with_payload requires from_label, to_label and relationship")
		}
		cypher := fmt.Sprintf("MATCH (a:%s {id: $from_id}), (b:%s {id: $to_id}) MERGE (a)-[r:%s]->(b) SET r += $payload RETURN a, r, b", fromLabel, toLabel, relType)
		return map[string]interface{}{
			"engine":   "neo4j",
			"strategy": "relationship_with_payload",
			"cypher":   cypher,
			"params": map[string]interface{}{
				"from_id": input["from_id"],
				"to_id":   input["to_id"],
				"payload": input["payload"],
			},
		}, nil
	case "bidirectional_relationship_semantics":
		userLabel, _ := input["user_label"].(string)
		requestType, _ := input["request_relationship"].(string)
		friendType, _ := input["friend_relationship"].(string)
		if strings.TrimSpace(userLabel) == "" {
			userLabel = "User"
		}
		if strings.TrimSpace(requestType) == "" {
			requestType = "KNOWS_REQUEST"
		}
		if strings.TrimSpace(friendType) == "" {
			friendType = "KNOWS"
		}
		cypher := fmt.Sprintf("MATCH (a:%s {id: $from_id})-[req:%s]->(b:%s {id: $to_id}) DELETE req MERGE (a)-[f1:%s]->(b) MERGE (b)-[f2:%s]->(a) SET f1.accepted_at = datetime(), f2.accepted_at = datetime() RETURN a, f1, b, f2", userLabel, requestType, userLabel, friendType, friendType)
		return map[string]interface{}{
			"engine":   "neo4j",
			"strategy": "bidirectional_relationship_semantics",
			"cypher":   cypher,
			"params": map[string]interface{}{
				"from_id": input["from_id"],
				"to_id":   input["to_id"],
			},
			"notes": "directional request to bidirectional friendship transition",
		}, nil
	case "social_model_bidirectional_follow":
		return a.buildSocialPresetModel("bidirectional_follow")
	case "social_model_friendship":
		return a.buildSocialPresetModel("friendship")
	case "social_model_forum_post":
		return a.buildSocialPresetModel("forum_post")
	case "social_model_one_to_one_chat":
		return a.buildSocialPresetModel("one_to_one_chat")
	case "social_model_group_chat_room":
		return a.buildSocialPresetModel("group_chat_room")
	case "social_model_chat_receipt":
		return a.buildSocialPresetModel("chat_receipt")
	case "social_model_chat_moderation":
		return a.buildSocialPresetModel("chat_moderation")
	case "social_model_message_emoji":
		return a.buildSocialPresetModel("message_emoji")
	case "social_network_preset_model":
		preset, _ := input["preset"].(string)
		if strings.TrimSpace(preset) == "" {
			preset = "bidirectional_follow"
		}
		return a.buildSocialPresetModel(preset)
	case "social_model_executor":
		preset, _ := input["preset"].(string)
		if strings.TrimSpace(preset) == "" {
			preset = "bidirectional_follow"
		}
		execute, _ := input["execute"].(bool)
		if execute && a.driver == nil {
			return nil, fmt.Errorf("neo4j driver not connected")
		}
		return a.buildSocialModelExecutor(ctx, preset, execute)
	default:
		return nil, fmt.Errorf("neo4j custom feature not supported: %s", feature)
	}
}

// resolvedSocialConfig 返回非空的社交网络配置（确保含默认值）。
func (a *Neo4jAdapter) resolvedSocialConfig() *Neo4jSocialNetworkConfig {
	if a.socialConfig != nil {
		return a.socialConfig
	}
	return resolvedNeo4jSocialNetworkConfig(nil)
}

func (a *Neo4jAdapter) buildSocialPresetModel(preset string) (map[string]interface{}, error) {
	cfg := a.resolvedSocialConfig()
	uL := cfg.UserLabel
	roomL := cfg.ChatRoomLabel
	msgL := cfg.ChatMessageLabel
	postL := cfg.PostLabel
	commentL := cfg.CommentLabel
	forumL := cfg.ForumLabel
	emojiL := cfg.EmojiLabel

	followsR := cfg.FollowsRelType
	friendR := cfg.FriendRelType
	friendReqR := cfg.FriendRequestRelType
	sentR := cfg.SentRelType
	memberOfR := cfg.MemberOfRelType
	inRoomR := cfg.InRoomRelType
	inRoomMsgR := cfg.InRoomMsgRelType
	mutedR := cfg.MutedInRelType
	bannedR := cfg.BannedInRelType
	readByR := cfg.ReadByRelType
	authoredR := cfg.AuthoredRelType
	createdR := cfg.CreatedRelType
	ftIndex := cfg.ChatMessageFulltextIndex

	uLC := strings.ToLower(uL)
	roomLC := strings.ToLower(roomL)
	msgLC := strings.ToLower(msgL)
	postLC := strings.ToLower(postL)
	forumLC := strings.ToLower(forumL)
	emojiLC := strings.ToLower(emojiL)

	switch strings.ToLower(strings.TrimSpace(preset)) {
	case "bidirectional_follow", "mutual_follow":
		return map[string]interface{}{
			"engine":   "neo4j",
			"strategy": "social_network_preset_model",
			"preset":   "bidirectional_follow",
			"constraints": []string{
				fmt.Sprintf("CREATE CONSTRAINT %s_id_unique IF NOT EXISTS FOR (u:%s) REQUIRE u.id IS UNIQUE", uLC, uL),
			},
			"queries": map[string]string{
				"create_user":      fmt.Sprintf("MERGE (u:%s {id: $user_id}) SET u.name = coalesce($name, u.name), u.created_at = coalesce(u.created_at, datetime()) RETURN u", uL),
				"follow":           fmt.Sprintf("MATCH (a:%s {id: $from_id}), (b:%s {id: $to_id}) MERGE (a)-[f:%s]->(b) SET f.created_at = coalesce(f.created_at, datetime()) RETURN a, f, b", uL, uL, followsR),
				"unfollow":         fmt.Sprintf("MATCH (a:%s {id: $from_id})-[f:%s]->(b:%s {id: $to_id}) DELETE f", uL, followsR, uL),
				"mutual_followers": fmt.Sprintf("MATCH (a:%s {id: $user_id})-[:%s]->(b:%s)-[:%s]->(a) RETURN b", uL, followsR, uL, followsR),
				"suggestions":      fmt.Sprintf("MATCH (a:%s {id: $user_id})-[:%s]->(:%s)-[:%s]->(candidate:%s) WHERE candidate.id <> $user_id AND NOT (a)-[:%s]->(candidate) RETURN candidate, count(*) AS score ORDER BY score DESC LIMIT $limit", uL, followsR, uL, followsR, uL, followsR),
			},
			"notes":             "Directed follow graph with built-in mutual-follow query for social discovery",
			"config_labels":     map[string]string{"user": uL},
			"config_rel_types":  map[string]string{"follows": followsR},
			"permission_levels": cfg.PermissionLevels,
		}, nil
	case "friendship", "friend":
		return map[string]interface{}{
			"engine":   "neo4j",
			"strategy": "social_network_preset_model",
			"preset":   "friendship",
			"constraints": []string{
				fmt.Sprintf("CREATE CONSTRAINT %s_id_unique IF NOT EXISTS FOR (u:%s) REQUIRE u.id IS UNIQUE", uLC, uL),
			},
			"queries": map[string]string{
				"send_request":   fmt.Sprintf("MATCH (a:%s {id: $from_id}), (b:%s {id: $to_id}) MERGE (a)-[r:%s]->(b) SET r.created_at = coalesce(r.created_at, datetime()), r.message = $message RETURN a, r, b", uL, uL, friendReqR),
				"accept_request": fmt.Sprintf("MATCH (a:%s {id: $from_id})-[r:%s]->(b:%s {id: $to_id}) DELETE r MERGE (a)-[f1:%s]->(b) MERGE (b)-[f2:%s]->(a) SET f1.since = datetime(), f2.since = datetime() RETURN a, f1, b, f2", uL, friendReqR, uL, friendR, friendR),
				"reject_request": fmt.Sprintf("MATCH (:%s {id: $from_id})-[r:%s]->(:%s {id: $to_id}) DELETE r", uL, friendReqR, uL),
				"list_friends":   fmt.Sprintf("MATCH (u:%s {id: $user_id})-[:%s]->(f:%s) RETURN f", uL, friendR, uL),
				"mutual_friends": fmt.Sprintf("MATCH (u1:%s {id: $user_a})-[:%s]->(m:%s)<-[:%s]-(u2:%s {id: $user_b}) RETURN m", uL, friendR, uL, friendR, uL),
			},
			"notes":             "Two-way friend model with explicit request/accept lifecycle",
			"config_labels":     map[string]string{"user": uL},
			"config_rel_types":  map[string]string{"friend": friendR, "friend_request": friendReqR},
			"permission_levels": cfg.PermissionLevels,
		}, nil
	case "forum_post", "forum", "post":
		return map[string]interface{}{
			"engine":   "neo4j",
			"strategy": "social_network_preset_model",
			"preset":   "forum_post",
			"constraints": []string{
				fmt.Sprintf("CREATE CONSTRAINT %s_id_unique IF NOT EXISTS FOR (u:%s) REQUIRE u.id IS UNIQUE", uLC, uL),
				fmt.Sprintf("CREATE CONSTRAINT %s_id_unique IF NOT EXISTS FOR (f:%s) REQUIRE f.id IS UNIQUE", forumLC, forumL),
				fmt.Sprintf("CREATE CONSTRAINT %s_id_unique IF NOT EXISTS FOR (p:%s) REQUIRE p.id IS UNIQUE", postLC, postL),
			},
			"queries": map[string]string{
				"create_forum":   fmt.Sprintf("MERGE (f:%s {id: $forum_id}) SET f.name = coalesce($name, f.name), f.created_at = coalesce(f.created_at, datetime()) RETURN f", forumL),
				"join_forum":     fmt.Sprintf("MATCH (u:%s {id: $user_id}), (f:%s {id: $forum_id}) MERGE (u)-[r:%s]->(f) SET r.joined_at = coalesce(r.joined_at, datetime()) RETURN u, r, f", uL, forumL, memberOfR),
				"create_post":    fmt.Sprintf("MATCH (u:%s {id: $author_id}), (f:%s {id: $forum_id}) MERGE (p:%s {id: $post_id}) SET p.title = $title, p.content = $content, p.created_at = datetime() MERGE (u)-[:%s]->(p) MERGE (p)-[:POSTED_IN]->(f) RETURN p", uL, forumL, postL, authoredR),
				"reply_post":     fmt.Sprintf("MATCH (u:%s {id: $author_id}), (p:%s {id: $post_id}) MERGE (c:%s {id: $comment_id}) SET c.content = $content, c.created_at = datetime() MERGE (u)-[:%s]->(c) MERGE (c)-[:REPLY_TO]->(p) RETURN c", uL, postL, commentL, authoredR),
				"like_post":      fmt.Sprintf("MATCH (u:%s {id: $user_id}), (p:%s {id: $post_id}) MERGE (u)-[l:LIKES_POST]->(p) SET l.created_at = coalesce(l.created_at, datetime()) RETURN u, l, p", uL, postL),
				"hot_posts":      fmt.Sprintf("MATCH (p:%s)-[:POSTED_IN]->(f:%s {id: $forum_id}) OPTIONAL MATCH (:%s)-[l:LIKES_POST]->(p) WITH p, count(l) AS likes RETURN p, likes ORDER BY likes DESC, p.created_at DESC LIMIT $limit", postL, forumL, uL),
				"forum_timeline": fmt.Sprintf("MATCH (u:%s {id: $user_id})-[:%s]->(f:%s)<-[:POSTED_IN]-(p:%s) RETURN p, f ORDER BY p.created_at DESC LIMIT $limit", uL, memberOfR, forumL, postL),
			},
			"notes":             "Forum domain model with post/comment/like relationships for community products",
			"config_labels":     map[string]string{"user": uL, "forum": forumL, "post": postL, "comment": commentL},
			"config_rel_types":  map[string]string{"member_of": memberOfR, "authored": authoredR},
			"permission_levels": cfg.PermissionLevels,
		}, nil
	case "one_to_one_chat", "direct_chat", "dm":
		sendMsgCondition := a.directChatCondition(cfg, "a", "b")
		canChatCondition := a.directChatCondition(cfg, "a", "b")
		return map[string]interface{}{
			"engine":   "neo4j",
			"strategy": "social_network_preset_model",
			"preset":   "one_to_one_chat",
			"constraints": []string{
				fmt.Sprintf("CREATE CONSTRAINT %s_id_unique IF NOT EXISTS FOR (u:%s) REQUIRE u.id IS UNIQUE", uLC, uL),
				fmt.Sprintf("CREATE CONSTRAINT %s_id_unique IF NOT EXISTS FOR (m:%s) REQUIRE m.id IS UNIQUE", msgLC, msgL),
				fmt.Sprintf("CREATE FULLTEXT INDEX %s IF NOT EXISTS FOR (m:%s) ON EACH [m.content]", ftIndex, msgL),
			},
			"queries": map[string]string{
				"send_direct_message":             fmt.Sprintf("MATCH (a:%s {id: $from_id}), (b:%s {id: $to_id}) WHERE %s CREATE (m:%s {id: $message_id, content: $content, created_at: datetime()}) MERGE (a)-[:%s]->(m) MERGE (m)-[:TO]->(b) RETURN m", uL, uL, sendMsgCondition, msgL, sentR),
				"list_direct_messages":            fmt.Sprintf("MATCH (s:%s)-[:%s]->(m:%s)-[:TO]->(r:%s) WHERE (s.id = $user_a AND r.id = $user_b) OR (s.id = $user_b AND r.id = $user_a) RETURN m, s, r ORDER BY m.created_at DESC LIMIT $limit", uL, sentR, msgL, uL),
				"search_direct_messages":          fmt.Sprintf("CALL db.index.fulltext.queryNodes('%s', $query) YIELD node, score MATCH (s:%s)-[:%s]->(node:%s)-[:TO]->(r:%s) WHERE (s.id = $user_a AND r.id = $user_b) OR (s.id = $user_b AND r.id = $user_a) RETURN node AS m, s, r, score ORDER BY score DESC, m.created_at DESC LIMIT $limit", ftIndex, uL, sentR, msgL, uL),
				"search_direct_messages_advanced": fmt.Sprintf("CALL db.index.fulltext.queryNodes('%s', $query) YIELD node, score MATCH (s:%s)-[:%s]->(node:%s)-[:TO]->(r:%s) WHERE ((s.id = $user_a AND r.id = $user_b) OR (s.id = $user_b AND r.id = $user_a)) AND node.deleted_at IS NULL AND ($start_at = '' OR node.created_at >= datetime($start_at)) AND ($end_at = '' OR node.created_at <= datetime($end_at)) OPTIONAL MATCH (node)-[:AT]->(mu:%s {id: $mentioned_user_id}) WITH node AS m, s, r, score, CASE WHEN $mentioned_user_id = '' THEN 0.0 WHEN mu IS NULL THEN 0.0 ELSE 0.5 END AS mention_boost RETURN m, s, r, score, mention_boost, score + mention_boost AS rank_score ORDER BY rank_score DESC, m.created_at DESC LIMIT $limit", ftIndex, uL, sentR, msgL, uL, uL),
				"can_chat_check":                  fmt.Sprintf("MATCH (a:%s {id: $from_id}), (b:%s {id: $to_id}) RETURN %s AS can_chat", uL, uL, canChatCondition),
				"reference_message":               fmt.Sprintf("MATCH (m:%s {id: $message_id}), (ref:%s {id: $ref_message_id}) MERGE (m)-[:REF]->(ref) RETURN m, ref", msgL, msgL),
			},
			"notes":                  "Direct chat permission is controlled by direct_chat_permission config; message is a middle node connecting sender and receiver",
			"direct_chat_permission": cfg.DirectChatPermission,
			"config_labels":          map[string]string{"user": uL, "chat_message": msgL},
			"config_rel_types":       map[string]string{"sent": sentR, "follows": followsR, "friend": friendR},
			"fulltext_index":         ftIndex,
			"permission_levels":      cfg.PermissionLevels,
		}, nil
	case "group_chat_room", "group_chat", "chat_room":
		joinRoomStrategy := cfg.JoinRoomStrategy
		var requestJoinQuery, approveJoinQuery string
		if joinRoomStrategy == "open" {
			// 开放模式：直接加入，无需审批
			requestJoinQuery = fmt.Sprintf("MATCH (u:%s {id: $user_id}), (room:%s {id: $room_id}) MERGE (u)-[in1:%s]->(room) SET in1.status = 'approved', in1.joined_at = coalesce(in1.joined_at, datetime()) MERGE (room)-[in2:%s]->(u) SET in2.status = 'approved', in2.joined_at = coalesce(in2.joined_at, datetime()) RETURN u, room", uL, roomL, inRoomR, inRoomR)
			approveJoinQuery = fmt.Sprintf("RETURN 'not_applicable_in_open_mode' AS status")
		} else {
			// 审批模式（默认）
			requestJoinQuery = fmt.Sprintf("MATCH (u:%s {id: $user_id}), (room:%s {id: $room_id}) MERGE (u)-[req:%s]->(room) SET req.status = coalesce(req.status, 'requested'), req.created_at = coalesce(req.created_at, datetime()) RETURN u, req, room", uL, roomL, inRoomR)
			approveJoinQuery = fmt.Sprintf("MATCH (u:%s {id: $user_id})-[req:%s]->(room:%s {id: $room_id}) SET req.status = 'approved', req.approved_at = datetime() MERGE (room)-[inback:%s]->(u) SET inback.status = 'approved', inback.approved_at = coalesce(inback.approved_at, datetime()) RETURN u, room", uL, inRoomR, roomL, inRoomR)
		}
		return map[string]interface{}{
			"engine":   "neo4j",
			"strategy": "social_network_preset_model",
			"preset":   "group_chat_room",
			"constraints": []string{
				fmt.Sprintf("CREATE CONSTRAINT %s_id_unique IF NOT EXISTS FOR (u:%s) REQUIRE u.id IS UNIQUE", uLC, uL),
				fmt.Sprintf("CREATE CONSTRAINT %s_id_unique IF NOT EXISTS FOR (r:%s) REQUIRE r.id IS UNIQUE", roomLC, roomL),
				fmt.Sprintf("CREATE CONSTRAINT %s_id_unique IF NOT EXISTS FOR (m:%s) REQUIRE m.id IS UNIQUE", msgLC, msgL),
				fmt.Sprintf("CREATE FULLTEXT INDEX %s IF NOT EXISTS FOR (m:%s) ON EACH [m.content]", ftIndex, msgL),
			},
			"queries": map[string]string{
				"create_room":                   fmt.Sprintf("MATCH (creator:%s {id: $creator_id}) MERGE (room:%s {id: $room_id}) SET room.name = coalesce($name, coalesce(room.name, 'room')), room.created_at = coalesce(room.created_at, datetime()) MERGE (creator)-[:%s]->(room) MERGE (creator)-[:%s]->(room) MERGE (room)-[:%s]->(creator) RETURN room, creator", uL, roomL, createdR, inRoomR, inRoomR),
				"request_join_room":             requestJoinQuery,
				"approve_join_room":             approveJoinQuery,
				"send_room_message":             fmt.Sprintf("MATCH (u:%s {id: $user_id})-[in1:%s]->(room:%s {id: $room_id})-[in2:%s]->(u) WHERE coalesce(in1.status, 'approved') <> 'requested' AND coalesce(in2.status, 'approved') <> 'requested' CREATE (m:%s {id: $message_id, content: $content, created_at: datetime()}) MERGE (u)-[:%s]->(m) MERGE (m)-[:%s]->(room) RETURN m, room", uL, inRoomR, roomL, inRoomR, msgL, sentR, inRoomMsgR),
				"at_user":                       fmt.Sprintf("MATCH (m:%s {id: $message_id}), (u:%s {id: $mentioned_user_id}) MERGE (m)-[:AT]->(u) RETURN m, u", msgL, uL),
				"ref_message":                   fmt.Sprintf("MATCH (m:%s {id: $message_id}), (ref:%s {id: $ref_message_id}) MERGE (m)-[:REF]->(ref) RETURN m, ref", msgL, msgL),
				"list_room_messages":            fmt.Sprintf("MATCH (room:%s {id: $room_id})<-[:%s]-(m:%s)<-[:%s]-(u:%s) RETURN m, u ORDER BY m.created_at DESC LIMIT $limit", roomL, inRoomMsgR, msgL, sentR, uL),
				"search_room_messages":          fmt.Sprintf("CALL db.index.fulltext.queryNodes('%s', $query) YIELD node, score MATCH (room:%s {id: $room_id})<-[:%s]-(node:%s)<-[:%s]-(u:%s) RETURN node AS m, u, room, score ORDER BY score DESC, m.created_at DESC LIMIT $limit", ftIndex, roomL, inRoomMsgR, msgL, sentR, uL),
				"search_room_messages_advanced": fmt.Sprintf("CALL db.index.fulltext.queryNodes('%s', $query) YIELD node, score MATCH (room:%s {id: $room_id})<-[:%s]-(node:%s)<-[:%s]-(u:%s) WHERE node.deleted_at IS NULL AND ($start_at = '' OR node.created_at >= datetime($start_at)) AND ($end_at = '' OR node.created_at <= datetime($end_at)) OPTIONAL MATCH (node)-[:AT]->(mu:%s {id: $mentioned_user_id}) WITH node AS m, u, room, score, CASE WHEN $mentioned_user_id = '' THEN 0.0 WHEN mu IS NULL THEN 0.0 ELSE 0.5 END AS mention_boost RETURN m, u, room, score, mention_boost, score + mention_boost AS rank_score ORDER BY rank_score DESC, m.created_at DESC LIMIT $limit", ftIndex, roomL, inRoomMsgR, msgL, sentR, uL, uL),
			},
			"notes":              "Group chat room model; join strategy and permission levels are configurable",
			"join_room_strategy": joinRoomStrategy,
			"config_labels":      map[string]string{"user": uL, "chat_room": roomL, "chat_message": msgL},
			"config_rel_types":   map[string]string{"created": createdR, "in_room": inRoomR, "in_room_msg": inRoomMsgR, "sent": sentR},
			"fulltext_index":     ftIndex,
			"permission_levels":  cfg.PermissionLevels,
		}, nil
	case "chat_receipt", "receipt", "message_receipt":
		return map[string]interface{}{
			"engine":   "neo4j",
			"strategy": "social_network_preset_model",
			"preset":   "chat_receipt",
			"constraints": []string{
				fmt.Sprintf("CREATE CONSTRAINT %s_id_unique IF NOT EXISTS FOR (u:%s) REQUIRE u.id IS UNIQUE", uLC, uL),
				fmt.Sprintf("CREATE CONSTRAINT %s_id_unique IF NOT EXISTS FOR (m:%s) REQUIRE m.id IS UNIQUE", msgLC, msgL),
				fmt.Sprintf("CREATE FULLTEXT INDEX %s IF NOT EXISTS FOR (m:%s) ON EACH [m.content]", ftIndex, msgL),
			},
			"queries": map[string]string{
				"mark_direct_message_read": fmt.Sprintf("MATCH (u:%s {id: $user_id}), (m:%s {id: $message_id})-[:TO]->(u) MERGE (u)-[r:%s]->(m) SET r.read_at = coalesce(r.read_at, datetime()) RETURN u, m, r", uL, msgL, readByR),
				"mark_room_message_read":   fmt.Sprintf("MATCH (u:%s {id: $user_id})-[in1:%s]->(room:%s)<-[in2:%s]-(u), (m:%s {id: $message_id})-[:%s]->(room) WHERE coalesce(in1.status, 'approved') <> 'requested' AND coalesce(in2.status, 'approved') <> 'requested' MERGE (u)-[r:%s]->(m) SET r.read_at = coalesce(r.read_at, datetime()) RETURN u, m, r", uL, inRoomR, roomL, inRoomR, msgL, inRoomMsgR, readByR),
				"list_direct_unread":       fmt.Sprintf("MATCH (sender:%s {id: $peer_id})-[:%s]->(m:%s)-[:TO]->(u:%s {id: $user_id}) WHERE NOT (u)-[:%s]->(m) RETURN m, sender ORDER BY m.created_at DESC LIMIT $limit", uL, sentR, msgL, uL, readByR),
				"list_room_unread":         fmt.Sprintf("MATCH (u:%s {id: $user_id})-[in1:%s]->(room:%s)<-[in2:%s]-(u), (sender:%s)-[:%s]->(m:%s)-[:%s]->(room) WHERE coalesce(in1.status, 'approved') <> 'requested' AND coalesce(in2.status, 'approved') <> 'requested' AND NOT (u)-[:%s]->(m) RETURN m, sender, room ORDER BY m.created_at DESC LIMIT $limit", uL, inRoomR, roomL, inRoomR, uL, sentR, msgL, inRoomMsgR, readByR),
			},
			"notes":             "Read receipt preset models message acknowledgment using READ_BY relation for both direct chat and room chat",
			"config_labels":     map[string]string{"user": uL, "chat_room": roomL, "chat_message": msgL},
			"config_rel_types":  map[string]string{"read_by": readByR, "sent": sentR, "in_room": inRoomR, "in_room_msg": inRoomMsgR},
			"permission_levels": cfg.PermissionLevels,
		}, nil
	case "chat_moderation", "moderation", "room_moderation":
		// 构建管理员检查条件（多个关系类型OR）
		modRelConditions := make([]string, 0, len(cfg.ModerationRelTypes))
		for _, relType := range cfg.ModerationRelTypes {
			modRelConditions = append(modRelConditions, fmt.Sprintf("(operator)-[:%s]->(room)", relType))
		}
		modCheck := strings.Join(modRelConditions, " OR ")
		if modCheck == "" {
			modCheck = fmt.Sprintf("(operator)-[:%s]->(room)", createdR)
		}
		modActionTypes := strings.Join([]string{mutedR, bannedR}, "|")
		return map[string]interface{}{
			"engine":   "neo4j",
			"strategy": "social_network_preset_model",
			"preset":   "chat_moderation",
			"constraints": []string{
				fmt.Sprintf("CREATE CONSTRAINT %s_id_unique IF NOT EXISTS FOR (u:%s) REQUIRE u.id IS UNIQUE", uLC, uL),
				fmt.Sprintf("CREATE CONSTRAINT %s_id_unique IF NOT EXISTS FOR (r:%s) REQUIRE r.id IS UNIQUE", roomLC, roomL),
			},
			"queries": map[string]string{
				"mute_member":             fmt.Sprintf("MATCH (operator:%s {id: $operator_id}), (room:%s {id: $room_id}), (target:%s {id: $target_id}) WHERE %s MERGE (target)-[mu:%s]->(room) SET mu.reason = $reason, mu.until_at = $until_at, mu.updated_at = datetime() RETURN target, room, mu", uL, roomL, uL, modCheck, mutedR),
				"unmute_member":           fmt.Sprintf("MATCH (operator:%s {id: $operator_id}), (room:%s {id: $room_id}) WHERE %s MATCH (target:%s {id: $target_id})-[mu:%s]->(room) DELETE mu RETURN target, room", uL, roomL, modCheck, uL, mutedR),
				"ban_member":              fmt.Sprintf("MATCH (operator:%s {id: $operator_id}), (room:%s {id: $room_id}), (target:%s {id: $target_id}) WHERE %s MERGE (target)-[ban:%s]->(room) SET ban.reason = $reason, ban.created_at = coalesce(ban.created_at, datetime()) WITH target, room OPTIONAL MATCH (target)-[in1:%s]->(room) DELETE in1 WITH target, room OPTIONAL MATCH (room)-[in2:%s]->(target) DELETE in2 RETURN target, room, ban", uL, roomL, uL, modCheck, bannedR, inRoomR, inRoomR),
				"unban_member":            fmt.Sprintf("MATCH (operator:%s {id: $operator_id}), (room:%s {id: $room_id}) WHERE %s MATCH (target:%s {id: $target_id})-[ban:%s]->(room) DELETE ban RETURN target, room", uL, roomL, modCheck, uL, bannedR),
				"can_send_room_message":   fmt.Sprintf("MATCH (u:%s {id: $user_id}), (room:%s {id: $room_id}) OPTIONAL MATCH (u)-[in1:%s]->(room) OPTIONAL MATCH (room)-[in2:%s]->(u) OPTIONAL MATCH (u)-[ban:%s]->(room) OPTIONAL MATCH (u)-[mu:%s]->(room) RETURN in1 IS NOT NULL AND in2 IS NOT NULL AND coalesce(in1.status, 'approved') <> 'requested' AND coalesce(in2.status, 'approved') <> 'requested' AND ban IS NULL AND (mu IS NULL OR mu.until_at IS NULL OR datetime(mu.until_at) < datetime()) AS can_send", uL, roomL, inRoomR, inRoomR, bannedR, mutedR),
				"list_moderation_actions": fmt.Sprintf("MATCH (u:%s)-[r:%s]->(room:%s {id: $room_id}) RETURN u, type(r) AS action, r ORDER BY coalesce(r.updated_at, r.created_at) DESC LIMIT $limit", uL, modActionTypes, roomL),
			},
			"notes":                "Moderation preset models mute/ban policies; moderation_rel_types controls who can moderate",
			"moderation_rel_types": cfg.ModerationRelTypes,
			"config_labels":        map[string]string{"user": uL, "chat_room": roomL},
			"config_rel_types":     map[string]string{"muted_in": mutedR, "banned_in": bannedR, "in_room": inRoomR},
			"permission_levels":    cfg.PermissionLevels,
		}, nil
	case "message_emoji", "chat_emoji", "emoji_placeholder":
		return map[string]interface{}{
			"engine":   "neo4j",
			"strategy": "social_network_preset_model",
			"preset":   "message_emoji",
			"constraints": []string{
				fmt.Sprintf("CREATE CONSTRAINT %s_id_unique IF NOT EXISTS FOR (m:%s) REQUIRE m.id IS UNIQUE", msgLC, msgL),
				fmt.Sprintf("CREATE CONSTRAINT %s_id_unique IF NOT EXISTS FOR (e:%s) REQUIRE e.id IS UNIQUE", emojiLC, emojiL),
				fmt.Sprintf("CREATE INDEX %s_symbol_index IF NOT EXISTS FOR (e:%s) ON (e.symbol)", emojiLC, emojiL),
			},
			"queries": map[string]string{
				"upsert_emoji":                 fmt.Sprintf("MERGE (e:%s {id: $emoji_id}) SET e.symbol = coalesce($symbol, e.symbol), e.name = coalesce($name, e.name), e.updated_at = datetime() RETURN e", emojiL),
				"attach_emoji_to_message":      fmt.Sprintf("MATCH (m:%s {id: $message_id}), (e:%s {id: $emoji_id}) MERGE (e)-[r:INCLUDED_BY {index: $index}]->(m) SET r.updated_at = datetime() RETURN e, r, m", msgL, emojiL),
				"detach_emoji_from_message":    fmt.Sprintf("MATCH (e:%s {id: $emoji_id})-[r:INCLUDED_BY {index: $index}]->(m:%s {id: $message_id}) DELETE r RETURN e, m", emojiL, msgL),
				"list_message_emojis":          fmt.Sprintf("MATCH (e:%s)-[r:INCLUDED_BY]->(m:%s {id: $message_id}) RETURN r.index AS index, e.id AS emoji_id, e.symbol AS symbol, e.name AS name ORDER BY index ASC", emojiL, msgL),
				"render_message_emoji_payload": fmt.Sprintf("MATCH (m:%s {id: $message_id}) OPTIONAL MATCH (e:%s)-[r:INCLUDED_BY]->(m) RETURN m.id AS message_id, m.content AS template_content, collect({index: r.index, emoji_id: e.id, symbol: e.symbol, name: e.name}) AS emojis", msgL, emojiL),
			},
			"notes":             "Message can embed emoji placeholders like {{0}}. INCLUDED_BY relation points from static Emoji node to ChatMessage.",
			"config_labels":     map[string]string{"chat_message": msgL, "emoji": emojiL},
			"permission_levels": cfg.PermissionLevels,
		}, nil
	default:
		return nil, fmt.Errorf("neo4j social preset model not supported: %s", preset)
	}
}

// directChatCondition 根据 DirectChatPermission 配置生成私信权限检查 Cypher 条件。
func (a *Neo4jAdapter) directChatCondition(cfg *Neo4jSocialNetworkConfig, fromVar, toVar string) string {
	friendR := cfg.FriendRelType
	followsR := cfg.FollowsRelType
	switch strings.ToLower(strings.TrimSpace(cfg.DirectChatPermission)) {
	case "friends_only":
		return fmt.Sprintf("(%s)-[:%s]->(%s) AND (%s)-[:%s]->(%s)", fromVar, friendR, toVar, toVar, friendR, fromVar)
	case "mutual_follow_only":
		return fmt.Sprintf("(%s)-[:%s]->(%s) AND (%s)-[:%s]->(%s)", fromVar, followsR, toVar, toVar, followsR, fromVar)
	case "open":
		return "true"
	default: // "mutual_follow_or_friend"
		return fmt.Sprintf("((%s)-[:%s]->(%s) AND (%s)-[:%s]->(%s)) OR ((%s)-[:%s]->(%s) AND (%s)-[:%s]->(%s))", fromVar, friendR, toVar, toVar, friendR, fromVar, fromVar, followsR, toVar, toVar, followsR, fromVar)
	}
}

func (a *Neo4jAdapter) buildSocialModelExecutor(ctx context.Context, preset string, execute bool) (interface{}, error) {
	// 获取预设模型定义
	modelDef, err := a.buildSocialPresetModel(preset)
	if err != nil {
		return nil, fmt.Errorf("failed to load preset model: %w", err)
	}

	defMap := modelDef
	constraints, _ := defMap["constraints"].([]string)
	queries, _ := defMap["queries"].(map[string]string)

	result := map[string]interface{}{
		"engine":       "neo4j",
		"strategy":     "social_model_executor",
		"preset":       preset,
		"constraints":  constraints,
		"sample_rules": queries,
	}

	if !execute {
		return result, nil
	}

	// 执行constraints和sample queries
	session := a.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: a.database})
	defer session.Close(ctx)

	executionResults := make(map[string]interface{})

	// 执行constraints
	constraintResults := make([]map[string]interface{}, 0, len(constraints))
	for _, constraint := range constraints {
		_, err := session.Run(ctx, constraint, nil)
		if err != nil {
			constraintResults = append(constraintResults, map[string]interface{}{
				"constraint": constraint,
				"status":     "error",
				"message":    err.Error(),
			})
		} else {
			constraintResults = append(constraintResults, map[string]interface{}{
				"constraint": constraint,
				"status":     "success",
			})
		}
	}
	executionResults["constraints"] = constraintResults

	// 执行sample queries（获取元数据，不执行数据修改）
	sampleQueryResults := make(map[string]interface{})
	for qName, qCypher := range queries {
		sampleQueryResults[qName] = map[string]interface{}{
			"cypher":      qCypher,
			"description": fmt.Sprintf("Sample query for %s operation in %s model", qName, preset),
			"parameters":  extractCypherParameters(qCypher),
		}
	}
	executionResults["sample_queries"] = sampleQueryResults

	result["execution_results"] = executionResults
	result["status"] = "executed"

	return result, nil
}

func extractCypherParameters(cypher string) []string {
	params := make(map[string]bool)
	reg := regexp.MustCompile(`\$(\w+)`)
	matches := reg.FindAllStringSubmatch(cypher, -1)
	for _, match := range matches {
		if len(match) > 1 {
			params[match[1]] = true
		}
	}
	result := make([]string, 0, len(params))
	for p := range params {
		result = append(result, p)
	}
	sort.Strings(result)
	return result
}

// Neo4jFactory AdapterFactory 实现。
type Neo4jFactory struct{}

func (f *Neo4jFactory) Name() string { return "neo4j" }

func (f *Neo4jFactory) Create(config *Config) (Adapter, error) {
	return NewNeo4jAdapter(config)
}

// init 自动注册 Neo4j 适配器。
func init() {
	RegisterAdapter(&Neo4jFactory{})
}
