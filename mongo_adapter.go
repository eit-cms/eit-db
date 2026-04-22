package db

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"text/template"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoAdapter MongoDB 适配器（最小可用版本：连接/健康检查）
type MongoAdapter struct {
	client               *mongo.Client
	database             string
	uri                  string
	relationJoinStrategy string
	hideThroughArtifacts bool
	logConfig            *MongoLogSystemConfig
}

// NewMongoAdapter 创建 MongoAdapter（不建立连接）
func NewMongoAdapter(config *Config) (*MongoAdapter, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	resolved := config.ResolvedMongoConfig()
	return &MongoAdapter{
		database:             resolved.Database,
		uri:                  resolved.URI,
		relationJoinStrategy: resolved.RelationJoinStrategy,
		hideThroughArtifacts: mongoHideThroughArtifactsEnabled(resolved),
		logConfig:            resolved.LogSystem,
	}, nil
}

// Connect 建立 MongoDB 连接
func (a *MongoAdapter) Connect(ctx context.Context, config *Config) error {
	if a.client != nil {
		return nil
	}

	uri := a.uri
	if config != nil {
		if err := config.Validate(); err != nil {
			return err
		}
		resolved := config.ResolvedMongoConfig()
		uri = resolved.URI
		a.database = resolved.Database
		a.relationJoinStrategy = resolved.RelationJoinStrategy
		a.hideThroughArtifacts = mongoHideThroughArtifactsEnabled(resolved)
		a.logConfig = resolved.LogSystem
	}

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return err
	}

	// 短连接测试
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx, nil); err != nil {
		_ = client.Disconnect(ctx)
		return err
	}

	a.client = client
	return nil
}

// Close 关闭连接
func (a *MongoAdapter) Close() error {
	if a.client == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return a.client.Disconnect(ctx)
}

// Ping 测试连接
func (a *MongoAdapter) Ping(ctx context.Context) error {
	if a.client == nil {
		return fmt.Errorf("mongodb client not connected")
	}
	return a.client.Ping(ctx, nil)
}

// Begin MongoDB 不支持 SQL 事务接口
func (a *MongoAdapter) Begin(ctx context.Context, opts ...interface{}) (Tx, error) {
	return nil, fmt.Errorf("mongodb: transactions are not supported in SQL interface")
}

// Query MongoDB 不支持 SQL Query
func (a *MongoAdapter) Query(ctx context.Context, sql string, args ...interface{}) (*sql.Rows, error) {
	return nil, fmt.Errorf("mongodb: sql query not supported")
}

// QueryRow MongoDB 不支持 SQL QueryRow
func (a *MongoAdapter) QueryRow(ctx context.Context, sql string, args ...interface{}) *sql.Row {
	return nil
}

// Exec MongoDB 不支持 SQL Exec
func (a *MongoAdapter) Exec(ctx context.Context, sql string, args ...interface{}) (sql.Result, error) {
	return nil, fmt.Errorf("mongodb: sql exec not supported")
}

// GetRawConn 返回 mongo.Client
func (a *MongoAdapter) GetRawConn() interface{} {
	return a.client
}

// RegisterScheduledTask MongoDB 暂不支持定时任务
func (a *MongoAdapter) RegisterScheduledTask(ctx context.Context, task *ScheduledTaskConfig) error {
	return NewScheduledTaskFallbackErrorWithReason("mongodb", ScheduledTaskFallbackReasonAdapterUnsupported, "native scheduled tasks not supported")
}

// UnregisterScheduledTask MongoDB 暂不支持定时任务
func (a *MongoAdapter) UnregisterScheduledTask(ctx context.Context, taskName string) error {
	return NewScheduledTaskFallbackErrorWithReason("mongodb", ScheduledTaskFallbackReasonAdapterUnsupported, "native scheduled tasks not supported")
}

// ListScheduledTasks MongoDB 暂不支持定时任务
func (a *MongoAdapter) ListScheduledTasks(ctx context.Context) ([]*ScheduledTaskStatus, error) {
	return nil, NewScheduledTaskFallbackErrorWithReason("mongodb", ScheduledTaskFallbackReasonAdapterUnsupported, "native scheduled tasks not supported")
}

// GetQueryBuilderProvider 返回 MongoDB BSON Query Builder Provider。
func (a *MongoAdapter) GetQueryBuilderProvider() QueryConstructorProvider {
	return NewMongoQueryConstructorProvider()
}

// ExecuteCompiledFindPlan 执行由 MongoQueryConstructor 生成的 Find 计划。
func (a *MongoAdapter) ExecuteCompiledFindPlan(ctx context.Context, query string) ([]map[string]interface{}, error) {
	if a.client == nil {
		return nil, fmt.Errorf("mongodb client not connected")
	}
	if !strings.HasPrefix(query, mongoCompiledQueryPrefix) {
		return nil, fmt.Errorf("invalid mongodb compiled query prefix")
	}

	payload := strings.TrimPrefix(query, mongoCompiledQueryPrefix)
	var plan MongoCompiledFindPlan
	if err := json.Unmarshal([]byte(payload), &plan); err != nil {
		return nil, fmt.Errorf("failed to parse mongodb compiled query: %w", err)
	}

	collection := strings.TrimSpace(plan.Collection)
	if collection == "" {
		return nil, fmt.Errorf("mongodb compiled query requires collection")
	}

	coll := a.client.Database(a.database).Collection(collection)

	filter := bson.M{}
	if plan.Filter != nil {
		filter = plan.Filter
	}

	// 含 lookup 阶段时改用 aggregate pipeline，以支持跨集合关联。
	if len(plan.Lookups) > 0 {
		strategy := normalizeMongoRelationJoinStrategy(a.relationJoinStrategy)
		pipeline := make([]bson.M, 0, 2+len(plan.Lookups)+3)
		if len(filter) > 0 {
			pipeline = append(pipeline, bson.M{"$match": filter})
		}

		for _, lk := range plan.Lookups {
			stage, ok := buildMongoLookupStage(lk, strategy)
			if !ok {
				continue
			}
			pipeline = append(pipeline, stage)

			as := strings.TrimSpace(lk.As)
			if lk.Semantic == JoinSemanticRequired {
				pipeline = append(pipeline, bson.M{"$match": bson.M{as + ".0": bson.M{"$exists": true}}})
			}
		}

		if a.hideThroughArtifacts {
			throughFields := collectThroughArtifactFields(plan.Lookups)
			if len(throughFields) > 0 {
				pipeline = append(pipeline, bson.M{"$unset": throughFields})
			}
		}

		if len(plan.Sort) > 0 {
			sortSpec := bson.D{}
			for _, item := range plan.Sort {
				field := strings.TrimSpace(item.Field)
				if field == "" {
					continue
				}
				direction := 1
				if item.Direction < 0 {
					direction = -1
				}
				sortSpec = append(sortSpec, bson.E{Key: field, Value: direction})
			}
			if len(sortSpec) > 0 {
				pipeline = append(pipeline, bson.M{"$sort": sortSpec})
			}
		}
		if plan.Offset != nil {
			pipeline = append(pipeline, bson.M{"$skip": int64(*plan.Offset)})
		}
		if plan.Limit != nil {
			pipeline = append(pipeline, bson.M{"$limit": int64(*plan.Limit)})
		}
		if len(plan.Projection) > 0 {
			projection := bson.M{}
			for _, field := range plan.Projection {
				trimmed := strings.TrimSpace(field)
				if trimmed == "" {
					continue
				}
				projection[trimmed] = 1
			}
			if len(projection) > 0 {
				pipeline = append(pipeline, bson.M{"$project": projection})
			}
		}

		cursor, err := coll.Aggregate(ctx, pipeline)
		if err != nil {
			return nil, err
		}
		defer cursor.Close(ctx)

		rows := make([]map[string]interface{}, 0)
		for cursor.Next(ctx) {
			entry := map[string]interface{}{}
			if err := cursor.Decode(&entry); err != nil {
				return nil, err
			}
			rows = append(rows, entry)
		}
		if err := cursor.Err(); err != nil {
			return nil, err
		}
		return rows, nil
	}

	findOpts := options.Find()
	if len(plan.Projection) > 0 {
		projection := bson.M{}
		for _, field := range plan.Projection {
			trimmed := strings.TrimSpace(field)
			if trimmed == "" {
				continue
			}
			projection[trimmed] = 1
		}
		if len(projection) > 0 {
			findOpts.SetProjection(projection)
		}
	}
	if len(plan.Sort) > 0 {
		sortSpec := bson.D{}
		for _, item := range plan.Sort {
			field := strings.TrimSpace(item.Field)
			if field == "" {
				continue
			}
			direction := 1
			if item.Direction < 0 {
				direction = -1
			}
			sortSpec = append(sortSpec, bson.E{Key: field, Value: direction})
		}
		if len(sortSpec) > 0 {
			findOpts.SetSort(sortSpec)
		}
	}
	if plan.Limit != nil {
		findOpts.SetLimit(int64(*plan.Limit))
	}
	if plan.Offset != nil {
		findOpts.SetSkip(int64(*plan.Offset))
	}

	cursor, err := coll.Find(ctx, filter, findOpts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	rows := make([]map[string]interface{}, 0)
	for cursor.Next(ctx) {
		entry := map[string]interface{}{}
		if err := cursor.Decode(&entry); err != nil {
			return nil, err
		}
		rows = append(rows, entry)
	}
	if err := cursor.Err(); err != nil {
		return nil, err
	}

	return rows, nil
}

func normalizeMongoRelationJoinStrategy(raw string) string {
	v := strings.ToLower(strings.TrimSpace(raw))
	if v == "pipeline" {
		return "pipeline"
	}
	return "lookup"
}

func mongoHideThroughArtifactsEnabled(cfg *MongoConnectionConfig) bool {
	if cfg == nil || cfg.HideThroughArtifacts == nil {
		return true
	}
	return *cfg.HideThroughArtifacts
}

func collectThroughArtifactFields(lookups []MongoLookupStage) []string {
	fields := make([]string, 0)
	seen := map[string]bool{}
	for _, lk := range lookups {
		as := strings.TrimSpace(lk.As)
		if as == "" {
			continue
		}
		if !lk.ThroughArtifact && !strings.HasSuffix(as, "_through") {
			continue
		}
		if !seen[as] {
			fields = append(fields, as)
			seen[as] = true
		}
	}
	return fields
}

func buildMongoLookupStage(lk MongoLookupStage, strategy string) (bson.M, bool) {
	from := strings.TrimSpace(lk.From)
	localField := strings.TrimSpace(lk.LocalField)
	foreignField := strings.TrimSpace(lk.ForeignField)
	as := strings.TrimSpace(lk.As)
	if from == "" || localField == "" || foreignField == "" || as == "" {
		return nil, false
	}

	if normalizeMongoRelationJoinStrategy(strategy) == "pipeline" {
		localRef := "$$local_join_value"
		arrayExpr := bson.M{"$cond": bson.A{
			bson.M{"$isArray": localRef},
			localRef,
			bson.A{localRef},
		}}
		return bson.M{"$lookup": bson.M{
			"from": from,
			"let":  bson.M{"local_join_value": "$" + localField},
			"pipeline": bson.A{
				bson.M{"$match": bson.M{"$expr": bson.M{"$in": bson.A{"$" + foreignField, arrayExpr}}}},
			},
			"as": as,
		}}, true
	}

	return bson.M{"$lookup": bson.M{
		"from":         from,
		"localField":   localField,
		"foreignField": foreignField,
		"as":           as,
	}}, true
}

// ExecuteCompiledWritePlan 执行由 MongoQueryConstructor 生成的写入计划。
func (a *MongoAdapter) ExecuteCompiledWritePlan(ctx context.Context, query string) (*QueryConstructorExecSummary, error) {
	if a.client == nil {
		return nil, fmt.Errorf("mongodb client not connected")
	}
	if !strings.HasPrefix(query, mongoCompiledWritePrefix) {
		return nil, fmt.Errorf("invalid mongodb compiled write prefix")
	}

	payload := strings.TrimPrefix(query, mongoCompiledWritePrefix)
	var plan MongoCompiledWritePlan
	if err := json.Unmarshal([]byte(payload), &plan); err != nil {
		return nil, fmt.Errorf("failed to parse mongodb compiled write: %w", err)
	}

	collection := strings.TrimSpace(plan.Collection)
	if collection == "" {
		return nil, fmt.Errorf("mongodb compiled write requires collection")
	}
	coll := a.client.Database(a.database).Collection(collection)

	summary := &QueryConstructorExecSummary{Counters: map[string]int{}}
	if plan.ReturnWriteDetail || plan.ReturnInsertedID {
		summary.Details = map[string]interface{}{}
	}

	switch strings.ToLower(strings.TrimSpace(plan.Operation)) {
	case "insert_one":
		if len(plan.Document) == 0 {
			return nil, fmt.Errorf("mongodb insert_one requires document")
		}
		res, err := coll.InsertOne(ctx, plan.Document)
		if err != nil {
			return nil, err
		}
		summary.RowsAffected = 1
		summary.Counters["inserted"] = 1
		if res != nil && res.InsertedID != nil {
			summary.Counters["inserted_id_present"] = 1
			if plan.ReturnInsertedID && summary.Details != nil {
				summary.Details["inserted_id"] = res.InsertedID
			}
		}
		if plan.ReturnWriteDetail && summary.Details != nil {
			summary.Details["inserted"] = 1
		}
		return summary, nil
	case "insert_many":
		if len(plan.Documents) == 0 {
			return nil, fmt.Errorf("mongodb insert_many requires documents")
		}
		docs := make([]interface{}, 0, len(plan.Documents))
		for _, d := range plan.Documents {
			docs = append(docs, d)
		}
		res, err := coll.InsertMany(ctx, docs)
		if err != nil {
			return nil, err
		}
		summary.RowsAffected = int64(len(plan.Documents))
		summary.Counters["inserted"] = len(plan.Documents)
		if res != nil {
			summary.Counters["inserted_ids"] = len(res.InsertedIDs)
			if plan.ReturnInsertedID && summary.Details != nil {
				summary.Details["inserted_ids"] = res.InsertedIDs
			}
			if plan.ReturnWriteDetail && summary.Details != nil {
				summary.Details["inserted_count"] = len(res.InsertedIDs)
			}
		}
		return summary, nil
	case "update_many":
		if len(plan.Update) == 0 {
			return nil, fmt.Errorf("mongodb update_many requires update document")
		}
		filter := bson.M{}
		if plan.Filter != nil {
			filter = plan.Filter
		}
		res, err := coll.UpdateMany(ctx, filter, plan.Update, options.Update().SetUpsert(plan.Upsert))
		if err != nil {
			return nil, err
		}
		if res != nil {
			summary.RowsAffected = int64(res.ModifiedCount)
			summary.Counters["matched"] = int(res.MatchedCount)
			summary.Counters["modified"] = int(res.ModifiedCount)
			summary.Counters["upserted"] = int(res.UpsertedCount)
			if plan.ReturnWriteDetail && summary.Details != nil {
				summary.Details["matched_count"] = res.MatchedCount
				summary.Details["modified_count"] = res.ModifiedCount
				summary.Details["upserted_count"] = res.UpsertedCount
				summary.Details["upserted_id"] = res.UpsertedID
			}
		}
		return summary, nil
	case "delete_many":
		filter := bson.M{}
		if plan.Filter != nil {
			filter = plan.Filter
		}
		res, err := coll.DeleteMany(ctx, filter)
		if err != nil {
			return nil, err
		}
		if res != nil {
			summary.RowsAffected = int64(res.DeletedCount)
			summary.Counters["deleted"] = int(res.DeletedCount)
			if plan.ReturnWriteDetail && summary.Details != nil {
				summary.Details["deleted_count"] = res.DeletedCount
			}
		}
		return summary, nil
	default:
		return nil, fmt.Errorf("mongodb compiled write operation not supported: %s", plan.Operation)
	}
}

// GetDatabaseFeatures MongoDB 特性声明（最小实现）
func (a *MongoAdapter) GetDatabaseFeatures() *DatabaseFeatures {
	return NewMongoDatabaseFeatures()
}

// GetQueryFeatures MongoDB 查询特性声明（最小实现）
func (a *MongoAdapter) GetQueryFeatures() *QueryFeatures {
	return NewMongoQueryFeatures()
}

// InspectFullTextRuntime MongoDB 运行时全文能力说明。
// 这里不依赖插件检测，默认给出应用层分词可用的能力声明。
func (a *MongoAdapter) InspectFullTextRuntime(ctx context.Context) (*FullTextRuntimeCapability, error) {
	return &FullTextRuntimeCapability{
		NativeSupported:       true,
		PluginChecked:         false,
		PluginAvailable:       false,
		PluginName:            "",
		TokenizationSupported: true,
		TokenizationMode:      "application",
		Notes:                 "MongoDB can use text index; app-layer tokenization is supported for fallback/boost",
	}, nil
}

// HasCustomFeatureImplementation 声明 MongoDB 可用的自定义能力
func (a *MongoAdapter) HasCustomFeatureImplementation(feature string) bool {
	switch feature {
	case "foreign_keys", "composite_foreign_keys", "custom_joiner", "document_join", "full_text_search", "tokenized_full_text_search", "log_hot_words", "log_special_tokenization", "log_hot_words_by_level", "log_hot_words_by_time_window", "article_draft_management", "article_template_rendering", "article_template_preset_library", "article_draft_query_plan":
		return true
	default:
		return false
	}
}

// ExecuteCustomFeature 执行 MongoDB 的自定义能力（当前提供 document join 方案）
func (a *MongoAdapter) ExecuteCustomFeature(ctx context.Context, feature string, input map[string]interface{}) (interface{}, error) {
	switch feature {
	case "foreign_keys", "composite_foreign_keys", "custom_joiner", "document_join":
		localCollection, _ := input["local_collection"].(string)
		foreignCollection, _ := input["foreign_collection"].(string)
		asField, _ := input["as"].(string)

		localFields, _ := input["local_fields"].([]string)
		foreignFields, _ := input["foreign_fields"].([]string)

		if localCollection == "" || foreignCollection == "" || len(localFields) == 0 || len(foreignFields) == 0 {
			return nil, fmt.Errorf("mongodb custom_joiner requires local_collection, foreign_collection, local_fields, foreign_fields")
		}

		if len(localFields) != len(foreignFields) {
			return nil, fmt.Errorf("mongodb custom_joiner requires same number of local_fields and foreign_fields")
		}

		if asField == "" {
			asField = "joined_docs"
		}

		andConditions := make([]map[string]interface{}, 0, len(localFields))
		for i := range localFields {
			andConditions = append(andConditions, map[string]interface{}{
				"$eq": []interface{}{"$$local_" + localFields[i], "$" + foreignFields[i]},
			})
		}

		letVars := make(map[string]interface{}, len(localFields))
		for _, localField := range localFields {
			letVars["local_"+localField] = "$" + localField
		}

		pipeline := []map[string]interface{}{
			{
				"$lookup": map[string]interface{}{
					"from": foreignCollection,
					"let":  letVars,
					"pipeline": []map[string]interface{}{
						{
							"$match": map[string]interface{}{
								"$expr": map[string]interface{}{"$and": andConditions},
							},
						},
					},
					"as": asField,
				},
			},
		}

		return map[string]interface{}{
			"engine":             "mongodb",
			"strategy":           "aggregation_lookup",
			"local_collection":   localCollection,
			"foreign_collection": foreignCollection,
			"pipeline":           pipeline,
		}, nil
	case "full_text_search", "tokenized_full_text_search":
		collection, _ := input["collection"].(string)
		query, _ := input["query"].(string)
		fields, _ := input["fields"].([]string)
		if collection == "" || strings.TrimSpace(query) == "" {
			return nil, fmt.Errorf("mongodb text search requires collection and query")
		}
		if len(fields) == 0 {
			fields = []string{"content"}
		}

		tokens := tokenizeSearchTerms(query)
		if len(tokens) == 0 {
			tokens = []string{query}
		}

		// 生成应用层分词增强的 $regex 查询计划（可由上层替换成 Atlas Search / $text）
		andConditions := make([]map[string]interface{}, 0, len(tokens))
		for _, token := range tokens {
			orGroup := make([]map[string]interface{}, 0, len(fields))
			for _, field := range fields {
				orGroup = append(orGroup, map[string]interface{}{
					field: map[string]interface{}{
						"$regex":   token,
						"$options": "i",
					},
				})
			}
			andConditions = append(andConditions, map[string]interface{}{"$or": orGroup})
		}

		pipeline := []map[string]interface{}{
			{"$match": map[string]interface{}{"$and": andConditions}},
		}

		return map[string]interface{}{
			"engine":     "mongodb",
			"strategy":   "tokenized_regex_pipeline",
			"collection": collection,
			"fields":     fields,
			"tokens":     tokens,
			"pipeline":   pipeline,
		}, nil
	case "log_special_tokenization":
		logCfg := a.resolvedLogConfig()
		texts := extractLogTextsFromInput(input)
		if len(texts) == 0 {
			if text, _ := input["text"].(string); strings.TrimSpace(text) != "" {
				texts = []string{text}
			}
		}
		if len(texts) == 0 {
			return nil, fmt.Errorf("mongodb log_special_tokenization requires text/texts/logs/documents")
		}

		rules := parseStringSlice(input["rules"])
		if len(rules) == 0 {
			rules = logCfg.DefaultTokenizationRules
		}

		items := make([]map[string]interface{}, 0, len(texts))
		for _, text := range texts {
			tokens, ruleHits := tokenizeLogWithRulesAndPatterns(text, rules, logCfg.CustomTokenizationPatterns)
			items = append(items, map[string]interface{}{
				"text":      text,
				"tokens":    tokens,
				"rule_hits": ruleHits,
			})
		}

		return map[string]interface{}{
			"engine":   "mongodb",
			"strategy": "log_special_tokenization",
			"rules":    rules,
			"items":    items,
		}, nil
	case "log_hot_words":
		logCfg := a.resolvedLogConfig()
		texts := extractLogTextsFromInput(input)
		if len(texts) == 0 {
			return nil, fmt.Errorf("mongodb log_hot_words requires logs/texts/documents")
		}

		topK := logCfg.DefaultTopK
		if v, ok := input["top_k"].(int); ok && v > 0 {
			topK = v
		}
		if v, ok := input["top_k"].(float64); ok && int(v) > 0 {
			topK = int(v)
		}

		minLen := logCfg.DefaultMinTokenLen
		if v, ok := input["min_token_len"].(int); ok && v > 0 {
			minLen = v
		}
		if v, ok := input["min_token_len"].(float64); ok && int(v) > 0 {
			minLen = int(v)
		}

		stopWords := buildStopWordsSetFromConfig(logCfg, parseStringSlice(input["stop_words"]))
		rules := parseStringSlice(input["rules"])
		if len(rules) == 0 {
			rules = logCfg.DefaultTokenizationRules
		}

		freq := make(map[string]int)
		// 预置自定义热词加成
		applyCustomHotWordBoost(freq, logCfg.CustomHotWords)
		for _, text := range texts {
			tokens, _ := tokenizeLogWithRulesAndPatterns(text, rules, logCfg.CustomTokenizationPatterns)
			for _, token := range tokens {
				normalized := strings.ToLower(strings.TrimSpace(token))
				if normalized == "" {
					continue
				}
				if len([]rune(normalized)) < minLen {
					continue
				}
				if stopWords[normalized] {
					continue
				}
				freq[normalized]++
			}
		}

		type kv struct {
			Token string
			Count int
		}
		ranked := make([]kv, 0, len(freq))
		for token, count := range freq {
			ranked = append(ranked, kv{Token: token, Count: count})
		}
		sort.Slice(ranked, func(i, j int) bool {
			if ranked[i].Count == ranked[j].Count {
				return ranked[i].Token < ranked[j].Token
			}
			return ranked[i].Count > ranked[j].Count
		})
		if topK < len(ranked) {
			ranked = ranked[:topK]
		}

		hotWords := make([]map[string]interface{}, 0, len(ranked))
		for _, item := range ranked {
			hotWords = append(hotWords, map[string]interface{}{
				"token": item.Token,
				"count": item.Count,
			})
		}

		return map[string]interface{}{
			"engine":              "mongodb",
			"strategy":            "log_hot_words",
			"total_logs":          len(texts),
			"top_k":               topK,
			"min_token_len":       minLen,
			"rules":               rules,
			"hot_words":           hotWords,
			"hot_word_collection": logCfg.HotWordCollection,
		}, nil
	case "log_hot_words_by_level":
		logCfg := a.resolvedLogConfig()
		texts := extractLogTextsFromInput(input)
		if len(texts) == 0 {
			return nil, fmt.Errorf("mongodb log_hot_words_by_level requires logs/texts/documents")
		}

		topK := logCfg.DefaultTopK
		if v, ok := input["top_k"].(int); ok && v > 0 {
			topK = v
		}
		if v, ok := input["top_k"].(float64); ok && int(v) > 0 {
			topK = int(v)
		}

		minLen := logCfg.DefaultMinTokenLen
		if v, ok := input["min_token_len"].(int); ok && v > 0 {
			minLen = v
		}
		if v, ok := input["min_token_len"].(float64); ok && int(v) > 0 {
			minLen = int(v)
		}

		stopWords := buildStopWordsSetFromConfig(logCfg, parseStringSlice(input["stop_words"]))
		rules := parseStringSlice(input["rules"])
		if len(rules) == 0 {
			rules = logCfg.DefaultTokenizationRules
		}

		levelField, _ := input["level_field"].(string)
		if levelField == "" {
			levelField = logCfg.DefaultLevelField
		}

		freqByLevel := make(map[string]map[string]int)
		for _, logEntry := range texts {
			level := extractLogLevel(logEntry, levelField)
			if _, ok := freqByLevel[level]; !ok {
				freqByLevel[level] = make(map[string]int)
				// 预置自定义热词加成
				applyCustomHotWordBoost(freqByLevel[level], logCfg.CustomHotWords)
			}

			tokens, _ := tokenizeLogWithRulesAndPatterns(logEntry, rules, logCfg.CustomTokenizationPatterns)
			for _, token := range tokens {
				normalized := strings.ToLower(strings.TrimSpace(token))
				if normalized == "" {
					continue
				}
				if len([]rune(normalized)) < minLen {
					continue
				}
				if stopWords[normalized] {
					continue
				}
				freqByLevel[level][normalized]++
			}
		}

		type kv struct {
			Token string
			Count int
		}

		resultByLevel := make(map[string][]map[string]interface{})
		for level, freq := range freqByLevel {
			ranked := make([]kv, 0, len(freq))
			for token, count := range freq {
				ranked = append(ranked, kv{Token: token, Count: count})
			}
			sort.Slice(ranked, func(i, j int) bool {
				if ranked[i].Count == ranked[j].Count {
					return ranked[i].Token < ranked[j].Token
				}
				return ranked[i].Count > ranked[j].Count
			})
			if topK < len(ranked) {
				ranked = ranked[:topK]
			}

			hotWords := make([]map[string]interface{}, 0, len(ranked))
			for _, item := range ranked {
				hotWords = append(hotWords, map[string]interface{}{
					"token": item.Token,
					"count": item.Count,
				})
			}
			resultByLevel[level] = hotWords
		}

		return map[string]interface{}{
			"engine":        "mongodb",
			"strategy":      "log_hot_words_by_level",
			"total_logs":    len(texts),
			"top_k":         topK,
			"level_field":   levelField,
			"min_token_len": minLen,
			"rules":         rules,
			"hot_words":     resultByLevel,
		}, nil
	case "log_hot_words_by_time_window":
		logCfg := a.resolvedLogConfig()
		texts := extractLogTextsFromInput(input)
		if len(texts) == 0 {
			return nil, fmt.Errorf("mongodb log_hot_words_by_time_window requires logs/texts/documents")
		}

		topK := logCfg.DefaultTopK
		if v, ok := input["top_k"].(int); ok && v > 0 {
			topK = v
		}
		if v, ok := input["top_k"].(float64); ok && int(v) > 0 {
			topK = int(v)
		}

		minLen := logCfg.DefaultMinTokenLen
		if v, ok := input["min_token_len"].(int); ok && v > 0 {
			minLen = v
		}
		if v, ok := input["min_token_len"].(float64); ok && int(v) > 0 {
			minLen = int(v)
		}

		timeWindow, _ := input["time_window"].(string)
		if timeWindow == "" {
			timeWindow = "hour"
		}

		timestampField, _ := input["timestamp_field"].(string)
		if timestampField == "" {
			timestampField = logCfg.DefaultTimeField
		}

		stopWords := buildStopWordsSetFromConfig(logCfg, parseStringSlice(input["stop_words"]))
		rules := parseStringSlice(input["rules"])
		if len(rules) == 0 {
			rules = logCfg.DefaultTokenizationRules
		}

		freqByWindow := groupLogsByTimeWindow(texts, timestampField, timeWindow)

		type kv struct {
			Token string
			Count int
		}

		resultByWindow := make(map[string][]map[string]interface{})
		for windowKey, logs := range freqByWindow {
			freq := make(map[string]int)
			// 预置自定义热词加成
			applyCustomHotWordBoost(freq, logCfg.CustomHotWords)
			for _, log := range logs {
				tokens, _ := tokenizeLogWithRulesAndPatterns(log, rules, logCfg.CustomTokenizationPatterns)
				for _, token := range tokens {
					normalized := strings.ToLower(strings.TrimSpace(token))
					if normalized == "" {
						continue
					}
					if len([]rune(normalized)) < minLen {
						continue
					}
					if stopWords[normalized] {
						continue
					}
					freq[normalized]++
				}
			}

			ranked := make([]kv, 0, len(freq))
			for token, count := range freq {
				ranked = append(ranked, kv{Token: token, Count: count})
			}
			sort.Slice(ranked, func(i, j int) bool {
				if ranked[i].Count == ranked[j].Count {
					return ranked[i].Token < ranked[j].Token
				}
				return ranked[i].Count > ranked[j].Count
			})
			if topK < len(ranked) {
				ranked = ranked[:topK]
			}

			hotWords := make([]map[string]interface{}, 0, len(ranked))
			for _, item := range ranked {
				hotWords = append(hotWords, map[string]interface{}{
					"token": item.Token,
					"count": item.Count,
				})
			}
			resultByWindow[windowKey] = hotWords
		}

		return map[string]interface{}{
			"engine":          "mongodb",
			"strategy":        "log_hot_words_by_time_window",
			"total_logs":      len(texts),
			"time_window":     timeWindow,
			"timestamp_field": timestampField,
			"top_k":           topK,
			"min_token_len":   minLen,
			"rules":           rules,
			"hot_words":       resultByWindow,
		}, nil
	case "article_draft_management":
		return executeArticleDraftManagement(input)
	case "article_template_rendering":
		return executeArticleTemplateRendering(input)
	case "article_template_preset_library":
		return executeArticleTemplatePresetLibrary(input)
	case "article_draft_query_plan":
		return buildArticleDraftQueryPlan(input)
	default:
		return nil, fmt.Errorf("mongodb custom feature not supported: %s", feature)
	}
}

func parseStringSlice(raw interface{}) []string {
	if raw == nil {
		return nil
	}
	if arr, ok := raw.([]string); ok {
		out := make([]string, 0, len(arr))
		for _, item := range arr {
			item = strings.TrimSpace(item)
			if item != "" {
				out = append(out, item)
			}
		}
		return out
	}
	if arr, ok := raw.([]interface{}); ok {
		out := make([]string, 0, len(arr))
		for _, item := range arr {
			s, ok := item.(string)
			if !ok {
				continue
			}
			s = strings.TrimSpace(s)
			if s != "" {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

func extractLogTextsFromInput(input map[string]interface{}) []string {
	if input == nil {
		return nil
	}
	if logs := parseStringSlice(input["logs"]); len(logs) > 0 {
		return logs
	}
	if texts := parseStringSlice(input["texts"]); len(texts) > 0 {
		return texts
	}

	messageField := "message"
	if v, ok := input["message_field"].(string); ok && strings.TrimSpace(v) != "" {
		messageField = strings.TrimSpace(v)
	}

	if docs, ok := input["documents"].([]map[string]interface{}); ok {
		out := make([]string, 0, len(docs))
		for _, doc := range docs {
			if s, ok := doc[messageField].(string); ok && strings.TrimSpace(s) != "" {
				out = append(out, s)
			}
		}
		return out
	}
	if docs, ok := input["documents"].([]interface{}); ok {
		out := make([]string, 0, len(docs))
		for _, raw := range docs {
			doc, ok := raw.(map[string]interface{})
			if !ok {
				continue
			}
			if s, ok := doc[messageField].(string); ok && strings.TrimSpace(s) != "" {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

// resolvedLogConfig 返回非空的日志系统配置（含默认值）。
func (a *MongoAdapter) resolvedLogConfig() *MongoLogSystemConfig {
	if a.logConfig != nil {
		return a.logConfig
	}
	return resolvedMongoLogSystemConfig(nil)
}

func buildStopWordsSet(extra []string) map[string]bool {
	defaults := []string{"the", "a", "an", "to", "for", "and", "or", "of", "in", "on", "is", "are", "at", "from", "with", "log", "info", "warn", "error", "debug"}
	out := make(map[string]bool, len(defaults)+len(extra))
	for _, word := range defaults {
		out[word] = true
	}
	for _, word := range extra {
		normalized := strings.ToLower(strings.TrimSpace(word))
		if normalized != "" {
			out[normalized] = true
		}
	}
	return out
}

func tokenizeLogWithRules(text string, rules []string) ([]string, map[string][]string) {
	base := tokenizeSearchTerms(text)
	out := make([]string, 0, len(base)+8)
	seen := make(map[string]struct{}, len(base)+8)
	for _, token := range base {
		normalized := strings.TrimSpace(token)
		if normalized == "" {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}

	ruleHits := make(map[string][]string)
	for _, rule := range rules {
		rule = strings.ToLower(strings.TrimSpace(rule))
		if rule == "" {
			continue
		}
		var re *regexp.Regexp
		switch rule {
		case "ip":
			re = regexp.MustCompile(`\b(?:\d{1,3}\.){3}\d{1,3}\b`)
		case "url":
			re = regexp.MustCompile(`https?://[^\s]+`)
		case "error_code":
			re = regexp.MustCompile(`\b(?:ERR|E|HTTP)[-_]?[A-Z0-9]{2,}\b`)
		case "trace_id":
			re = regexp.MustCompile(`\b[0-9a-fA-F]{16,32}\b`)
		case "hashtag":
			re = regexp.MustCompile(`#[-_\p{L}\p{N}]+`)
		default:
			continue
		}

		matches := re.FindAllString(text, -1)
		if len(matches) == 0 {
			continue
		}
		for _, m := range matches {
			m = strings.TrimSpace(m)
			if m == "" {
				continue
			}
			ruleHits[rule] = append(ruleHits[rule], m)
			if _, ok := seen[m]; ok {
				continue
			}
			seen[m] = struct{}{}
			out = append(out, m)
		}
	}

	return out, ruleHits
}

// buildStopWordsSetFromConfig 使用 MongoLogSystemConfig 构建停用词集合。
// 当 DisableBuiltinStopWords 为 true 时，仅使用 ExtraStopWords 和调用方传入的 extra。
func buildStopWordsSetFromConfig(cfg *MongoLogSystemConfig, extra []string) map[string]bool {
	var baseList []string
	if cfg == nil || !cfg.DisableBuiltinStopWords {
		baseList = []string{"the", "a", "an", "to", "for", "and", "or", "of", "in", "on", "is", "are", "at", "from", "with", "log", "info", "warn", "error", "debug"}
	}
	out := make(map[string]bool, len(baseList)+len(extra))
	for _, word := range baseList {
		out[word] = true
	}
	if cfg != nil {
		for _, word := range cfg.ExtraStopWords {
			normalized := strings.ToLower(strings.TrimSpace(word))
			if normalized != "" {
				out[normalized] = true
			}
		}
	}
	for _, word := range extra {
		normalized := strings.ToLower(strings.TrimSpace(word))
		if normalized != "" {
			out[normalized] = true
		}
	}
	return out
}

// tokenizeLogWithRulesAndPatterns 在 tokenizeLogWithRules 基础上支持自定义正则规则。
// customPatterns 中的规则名会覆盖同名内置规则；rules 列表里的自定义规则名通过 customPatterns 解析。
func tokenizeLogWithRulesAndPatterns(text string, rules []string, customPatterns map[string]string) ([]string, map[string][]string) {
	base := tokenizeSearchTerms(text)
	out := make([]string, 0, len(base)+8)
	seen := make(map[string]struct{}, len(base)+8)
	for _, token := range base {
		normalized := strings.TrimSpace(token)
		if normalized == "" {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}

	ruleHits := make(map[string][]string)
	for _, rule := range rules {
		rule = strings.ToLower(strings.TrimSpace(rule))
		if rule == "" {
			continue
		}
		var re *regexp.Regexp

		// 优先使用自定义正则
		if customPatterns != nil {
			if pattern, ok := customPatterns[rule]; ok && strings.TrimSpace(pattern) != "" {
				compiled, err := regexp.Compile(pattern)
				if err == nil {
					re = compiled
				}
			}
		}
		// 未命中自定义规则则使用内置规则
		if re == nil {
			switch rule {
			case "ip":
				re = regexp.MustCompile(`\b(?:\d{1,3}\.){3}\d{1,3}\b`)
			case "url":
				re = regexp.MustCompile(`https?://[^\s]+`)
			case "error_code":
				re = regexp.MustCompile(`\b(?:ERR|E|HTTP)[-_]?[A-Z0-9]{2,}\b`)
			case "trace_id":
				re = regexp.MustCompile(`\b[0-9a-fA-F]{16,32}\b`)
			case "hashtag":
				re = regexp.MustCompile(`#[-_\p{L}\p{N}]+`)
			default:
				// 既无内置规则也无自定义规则，跳过
				continue
			}
		}

		matches := re.FindAllString(text, -1)
		if len(matches) == 0 {
			continue
		}
		for _, m := range matches {
			m = strings.TrimSpace(m)
			if m == "" {
				continue
			}
			ruleHits[rule] = append(ruleHits[rule], m)
			if _, ok := seen[m]; ok {
				continue
			}
			seen[m] = struct{}{}
			out = append(out, m)
		}
	}

	return out, ruleHits
}

// applyCustomHotWordBoost 将 CustomHotWords 的加成分值叠加到频率 map 上。
func applyCustomHotWordBoost(freq map[string]int, customHotWords map[string]int) {
	for word, boost := range customHotWords {
		normalized := strings.ToLower(strings.TrimSpace(word))
		if normalized != "" && boost > 0 {
			freq[normalized] += boost
		}
	}
}

func extractLogLevel(logEntry string, levelField string) string {
	// 尝试从JSON中提取level字段
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(logEntry), &data); err == nil {
		if level, ok := data[levelField].(string); ok && level != "" {
			return strings.ToUpper(strings.TrimSpace(level))
		}
	}

	// 从文本中提取常见的日志级别
	lowerEntry := strings.ToLower(logEntry)
	levels := []string{"error", "warn", "warning", "info", "debug", "trace", "critical", "panic", "fatal"}
	for _, level := range levels {
		if strings.Contains(lowerEntry, "["+level+"]") || strings.Contains(lowerEntry, " "+level+" ") {
			return strings.ToUpper(level)
		}
	}

	return "UNKNOWN"
}

func groupLogsByTimeWindow(texts []string, timestampField string, timeWindow string) map[string][]string {
	result := make(map[string][]string)
	for _, logEntry := range texts {
		var timestamp time.Time

		// 尝试从JSON中提取时间戳
		var data map[string]interface{}
		if err := json.Unmarshal([]byte(logEntry), &data); err == nil {
			if ts, ok := data[timestampField]; ok {
				// 尝试解析为字符串
				if tsStr, ok := ts.(string); ok {
					if t, err := time.Parse(time.RFC3339, tsStr); err == nil {
						timestamp = t
					} else if t, err := time.Parse("2006-01-02 15:04:05", tsStr); err == nil {
						timestamp = t
					} else if t, err := time.Parse(time.RFC1123Z, tsStr); err == nil {
						timestamp = t
					}
				}
			}
		}

		// 如果解析失败，使用当前时间
		if timestamp.IsZero() {
			timestamp = time.Now()
		}

		// 按时间窗口分组
		var windowKey string
		switch strings.ToLower(strings.TrimSpace(timeWindow)) {
		case "day":
			windowKey = timestamp.Format("2006-01-02")
		case "hour":
			fallthrough
		default:
			windowKey = timestamp.Format("2006-01-02 15:00:00")
		}

		if _, ok := result[windowKey]; !ok {
			result[windowKey] = make([]string, 0)
		}
		result[windowKey] = append(result[windowKey], logEntry)
	}

	return result
}

func executeArticleDraftManagement(input map[string]interface{}) (interface{}, error) {
	// 获取操作类型
	operation, _ := input["operation"].(string)
	if operation == "" {
		operation = "info"
	}

	// 获取文章数据
	article, _ := input["article"].(map[string]interface{})
	if article == nil {
		article = make(map[string]interface{})
	}

	// 获取草稿标记
	isDraft, _ := input["is_draft"].(bool)

	// 获取元数据
	tags := parseStringSlice(input["tags"])
	category, _ := input["category"].(string)
	priority := 0
	if v, ok := input["priority"].(int); ok {
		priority = v
	} else if v, ok := input["priority"].(float64); ok {
		priority = int(v)
	}

	switch strings.ToLower(operation) {
	case "create":
		// 创建草稿文章
		article["created_at"] = time.Now()
		article["updated_at"] = time.Now()
		article["is_draft"] = true
		article["version"] = 1
		article["tags"] = tags
		article["category"] = category
		article["priority"] = priority
		article["edit_count"] = 0

		return map[string]interface{}{
			"engine":    "mongodb",
			"strategy":  "article_draft_management",
			"operation": "create",
			"article":   article,
			"status":    "draft_created",
		}, nil

	case "update":
		// 更新草稿文章
		article["updated_at"] = time.Now()
		article["is_draft"] = true
		editCount := 0
		if v, ok := article["edit_count"].(float64); ok {
			editCount = int(v) + 1
		} else if v, ok := article["edit_count"].(int); ok {
			editCount = v + 1
		}
		article["edit_count"] = editCount
		if len(tags) > 0 {
			article["tags"] = tags
		}
		if category != "" {
			article["category"] = category
		}
		if priority > 0 {
			article["priority"] = priority
		}

		return map[string]interface{}{
			"engine":     "mongodb",
			"strategy":   "article_draft_management",
			"operation":  "update",
			"article":    article,
			"status":     "draft_updated",
			"edit_count": editCount,
		}, nil

	case "publish":
		// 发布文章（从草稿变为已发布）
		article["published_at"] = time.Now()
		article["updated_at"] = time.Now()
		article["is_draft"] = false
		if v, ok := article["version"].(float64); ok {
			article["version"] = int(v) + 1
		} else if v, ok := article["version"].(int); ok {
			article["version"] = v + 1
		} else {
			article["version"] = 1
		}

		return map[string]interface{}{
			"engine":    "mongodb",
			"strategy":  "article_draft_management",
			"operation": "publish",
			"article":   article,
			"status":    "article_published",
		}, nil

	case "archive", "restore":
		// 归档或恢复文章
		article["archived_at"] = time.Now()
		article["is_archived"] = (operation == "archive")
		article["updated_at"] = time.Now()

		status := "article_archived"
		if operation == "restore" {
			article["is_archived"] = false
			status = "article_restored"
		}

		return map[string]interface{}{
			"engine":    "mongodb",
			"strategy":  "article_draft_management",
			"operation": operation,
			"article":   article,
			"status":    status,
		}, nil

	case "info":
		// 返回文章信息和状态
		return map[string]interface{}{
			"engine":    "mongodb",
			"strategy":  "article_draft_management",
			"operation": "info",
			"is_draft":  isDraft,
			"article":   article,
			"metadata": map[string]interface{}{
				"tags":     tags,
				"category": category,
				"priority": priority,
			},
		}, nil

	case "query_plan", "list_plan", "filter_plan":
		return buildArticleDraftQueryPlan(input)

	default:
		return nil, fmt.Errorf("mongodb article_draft_management: unsupported operation '%s'", operation)
	}
}

func executeArticleTemplateRendering(input map[string]interface{}) (interface{}, error) {
	// 获取模板
	templateStr, _ := input["template"].(string)
	presetName, _ := input["template_preset"].(string)
	if strings.EqualFold(strings.TrimSpace(presetName), "list") {
		return executeArticleTemplatePresetLibrary(map[string]interface{}{"operation": "list"})
	}
	if strings.TrimSpace(templateStr) == "" {
		if strings.TrimSpace(presetName) == "" {
			return nil, fmt.Errorf("mongodb article_template_rendering requires template or template_preset")
		}
		resolvedTemplate, err := getArticleTemplatePreset(presetName)
		if err != nil {
			return nil, err
		}
		templateStr = resolvedTemplate
	}

	// 获取数据
	data, _ := input["data"].(map[string]interface{})
	if data == nil {
		data = make(map[string]interface{})
	}

	// 获取模板名称
	templateName, _ := input["template_name"].(string)
	if templateName == "" {
		templateName = "article"
	}

	// 获取支持功能
	enableFunctions, _ := input["enable_functions"].(bool)
	policy := parseArticleTemplateSecurityPolicy(input)
	if policy.MaxTemplateSize > 0 && len([]byte(templateStr)) > policy.MaxTemplateSize {
		return nil, fmt.Errorf("mongodb article_template_rendering: template size exceeds max_template_size=%d", policy.MaxTemplateSize)
	}

	// 创建模板
	tmpl := template.New(templateName)
	if policy.StrictVariables {
		tmpl = tmpl.Option("missingkey=error")
	}

	// 添加自定义函数（需在 Parse 前注册）
	if enableFunctions {
		funcMap := template.FuncMap{
			"upper": strings.ToUpper,
			"lower": strings.ToLower,
			"title": strings.Title,
			"trim":  strings.TrimSpace,
			"len": func(s string) int {
				return len([]rune(s))
			},
			"contains": strings.Contains,
			"join":     strings.Join,
			"split":    strings.Split,
		}
		allowed := policy.AllowedFunctions
		if len(allowed) > 0 {
			filtered := template.FuncMap{}
			for _, name := range allowed {
				name = strings.TrimSpace(name)
				if fn, ok := funcMap[name]; ok {
					filtered[name] = fn
				}
			}
			funcMap = filtered
		}
		if len(funcMap) > 0 {
			tmpl = tmpl.Funcs(funcMap)
		}
	}

	parsed, err := tmpl.Parse(templateStr)
	if err != nil {
		return nil, fmt.Errorf("mongodb article_template_rendering: failed to parse template: %w", err)
	}

	// 渲染模板
	var output bytes.Buffer
	if err := parsed.Execute(&output, data); err != nil {
		return nil, fmt.Errorf("mongodb article_template_rendering: failed to render template: %w", err)
	}

	renderedContent := output.String()

	// 获取文章元数据（可选）
	article, _ := input["article"].(map[string]interface{})

	result := map[string]interface{}{
		"engine":          "mongodb",
		"strategy":        "article_template_rendering",
		"template_name":   templateName,
		"template_preset": presetName,
		"rendered_output": renderedContent,
		"data_used":       data,
		"security_policy": map[string]interface{}{
			"max_template_size": policy.MaxTemplateSize,
			"strict_variables":  policy.StrictVariables,
			"allowed_functions": policy.AllowedFunctions,
		},
	}

	if article != nil {
		result["article_id"] = article["id"]
		result["article_title"] = article["title"]
	}

	return result, nil
}

func executeArticleTemplatePresetLibrary(input map[string]interface{}) (interface{}, error) {
	operation, _ := input["operation"].(string)
	if operation == "" {
		operation = "list"
	}
	preset, _ := input["preset"].(string)
	if preset == "" {
		preset, _ = input["template_preset"].(string)
	}

	presets := map[string]string{
		"blog": `# {{.title}}

> 作者：{{.author}} | 发布时间：{{.published_at}}

{{.summary}}

{{.content}}

标签：{{join .tags ", "}}`,
		"news": `{{.title}}

导语：{{.lead}}

正文：
{{.content}}

记者：{{.reporter}} | 来源：{{.source}}`,
		"knowledge_base": `## {{.title}}

### 问题描述
{{.problem}}

### 解决方案
{{.solution}}

### 注意事项
{{.notes}}`,
	}

	if strings.EqualFold(strings.TrimSpace(operation), "list") {
		names := make([]string, 0, len(presets))
		for name := range presets {
			names = append(names, name)
		}
		sort.Strings(names)
		return map[string]interface{}{
			"engine":       "mongodb",
			"strategy":     "article_template_preset_library",
			"operation":    "list",
			"preset_names": names,
		}, nil
	}

	resolved, err := getArticleTemplatePresetWithSource(preset, presets)
	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"engine":          "mongodb",
		"strategy":        "article_template_preset_library",
		"operation":       "get",
		"template_preset": preset,
		"template":        resolved,
	}, nil
}

type articleTemplateSecurityPolicy struct {
	MaxTemplateSize  int
	StrictVariables  bool
	AllowedFunctions []string
}

func parseArticleTemplateSecurityPolicy(input map[string]interface{}) articleTemplateSecurityPolicy {
	policy := articleTemplateSecurityPolicy{
		MaxTemplateSize: 64 * 1024,
		StrictVariables: false,
	}

	if v, ok := input["max_template_size"].(int); ok && v > 0 {
		policy.MaxTemplateSize = v
	}
	if v, ok := input["max_template_size"].(float64); ok && int(v) > 0 {
		policy.MaxTemplateSize = int(v)
	}

	if rawPolicy, ok := input["security_policy"].(map[string]interface{}); ok {
		if v, ok := rawPolicy["max_template_size"].(int); ok && v > 0 {
			policy.MaxTemplateSize = v
		}
		if v, ok := rawPolicy["max_template_size"].(float64); ok && int(v) > 0 {
			policy.MaxTemplateSize = int(v)
		}
		if v, ok := rawPolicy["strict_variables"].(bool); ok {
			policy.StrictVariables = v
		}
		policy.AllowedFunctions = parseStringSlice(rawPolicy["allowed_functions"])
	}

	if v, ok := input["strict_variables"].(bool); ok {
		policy.StrictVariables = v
	}
	if allowed := parseStringSlice(input["allowed_functions"]); len(allowed) > 0 {
		policy.AllowedFunctions = allowed
	}

	return policy
}

func getArticleTemplatePreset(preset string) (string, error) {
	presets := map[string]string{
		"blog": `# {{.title}}

> 作者：{{.author}} | 发布时间：{{.published_at}}

{{.summary}}

{{.content}}

标签：{{join .tags ", "}}`,
		"news": `{{.title}}

导语：{{.lead}}

正文：
{{.content}}

记者：{{.reporter}} | 来源：{{.source}}`,
		"knowledge_base": `## {{.title}}

### 问题描述
{{.problem}}

### 解决方案
{{.solution}}

### 注意事项
{{.notes}}`,
	}
	return getArticleTemplatePresetWithSource(preset, presets)
}

func getArticleTemplatePresetWithSource(preset string, presets map[string]string) (string, error) {
	name := strings.ToLower(strings.TrimSpace(preset))
	if name == "" {
		name = "blog"
	}
	tpl, ok := presets[name]
	if !ok {
		return "", fmt.Errorf("mongodb article template preset not supported: %s", preset)
	}
	return tpl, nil
}

func buildArticleDraftQueryPlan(input map[string]interface{}) (interface{}, error) {
	collection, _ := input["collection"].(string)
	if strings.TrimSpace(collection) == "" {
		collection = "articles"
	}

	status, _ := input["status"].(string)
	status = strings.ToLower(strings.TrimSpace(status))
	if status == "" {
		status = "draft"
	}

	filter := map[string]interface{}{}
	switch status {
	case "draft", "editing":
		filter["is_draft"] = true
		filter["is_archived"] = map[string]interface{}{"$ne": true}
	case "pending_publish", "review":
		filter["is_draft"] = true
		filter["review_status"] = "approved"
		filter["is_archived"] = map[string]interface{}{"$ne": true}
	case "published":
		filter["is_draft"] = false
		filter["published_at"] = map[string]interface{}{"$exists": true}
		filter["is_archived"] = map[string]interface{}{"$ne": true}
	case "archived":
		filter["is_archived"] = true
	case "all":
		// no-op
	default:
		return nil, fmt.Errorf("mongodb article_draft_query_plan: unsupported status '%s'", status)
	}

	if category, _ := input["category"].(string); strings.TrimSpace(category) != "" {
		filter["category"] = category
	}
	if authorID, _ := input["author_id"].(string); strings.TrimSpace(authorID) != "" {
		filter["author_id"] = authorID
	}

	projection := map[string]interface{}{
		"title":        1,
		"author_id":    1,
		"category":     1,
		"is_draft":     1,
		"is_archived":  1,
		"updated_at":   1,
		"published_at": 1,
	}
	if fields := parseStringSlice(input["fields"]); len(fields) > 0 {
		projection = map[string]interface{}{}
		for _, f := range fields {
			projection[f] = 1
		}
	}

	limit := 20
	if v, ok := input["limit"].(int); ok && v > 0 {
		limit = v
	}
	if v, ok := input["limit"].(float64); ok && int(v) > 0 {
		limit = int(v)
	}

	skip := 0
	if v, ok := input["skip"].(int); ok && v >= 0 {
		skip = v
	}
	if v, ok := input["skip"].(float64); ok && int(v) >= 0 {
		skip = int(v)
	}

	sortBy, _ := input["sort_by"].(string)
	if strings.TrimSpace(sortBy) == "" {
		sortBy = "updated_at"
	}
	sortOrder := -1
	if v, ok := input["sort_order"].(int); ok && (v == 1 || v == -1) {
		sortOrder = v
	}

	return map[string]interface{}{
		"engine":   "mongodb",
		"strategy": "article_draft_query_plan",
		"status":   status,
		"query_plan": map[string]interface{}{
			"collection": collection,
			"filter":     filter,
			"projection": projection,
			"sort": map[string]interface{}{
				sortBy: sortOrder,
			},
			"limit": limit,
			"skip":  skip,
		},
	}, nil
}

// MongoFactory AdapterFactory 实现
type MongoFactory struct{}

func (f *MongoFactory) Name() string { return "mongodb" }

func (f *MongoFactory) Create(config *Config) (Adapter, error) {
	return NewMongoAdapter(config)
}

func init() {
	RegisterAdapter(&MongoFactory{})
}
