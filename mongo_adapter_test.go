package db

import (
	"context"
	"os"
	"strings"
	"testing"
)

func TestMongoConfigValidationRequiresURI(t *testing.T) {
	cfg := &Config{
		Adapter:  "mongodb",
		Database: "test_db",
	}
	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected validation error for missing options.uri")
	}
}

func TestMongoAdapterFactory(t *testing.T) {
	cfg := &Config{
		Adapter:  "mongodb",
		Database: "test_db",
		Options: map[string]interface{}{
			"uri": "mongodb://localhost:27017",
		},
	}

	repo, err := NewRepository(cfg)
	if err != nil {
		t.Fatalf("NewRepository failed: %v", err)
	}
	if repo == nil {
		t.Fatalf("expected repo, got nil")
	}
}

func TestMongoAdapterPing(t *testing.T) {
	uri := os.Getenv("MONGODB_URI")
	if uri == "" {
		t.Skip("MONGODB_URI not set; skipping integration test")
	}

	cfg := &Config{
		Adapter:  "mongodb",
		Database: "test_db",
		Options: map[string]interface{}{
			"uri": uri,
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

func TestMongoCustomFeatureLogHotWords(t *testing.T) {
	a := &MongoAdapter{}
	result, err := a.ExecuteCustomFeature(context.Background(), "log_hot_words", map[string]interface{}{
		"logs": []string{
			"ERROR timeout on /api/v1/login trace=abcdef1234567890",
			"WARN timeout on /api/v1/login trace=abcdef1234567890",
			"ERROR redis timeout while loading profile",
		},
		"top_k": 5,
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	payload, ok := result.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map payload, got %T", result)
	}
	if payload["strategy"] != "log_hot_words" {
		t.Fatalf("expected log_hot_words strategy, got %v", payload["strategy"])
	}
	hotWords, ok := payload["hot_words"].([]map[string]interface{})
	if !ok || len(hotWords) == 0 {
		t.Fatalf("expected non-empty hot words, got %+v", payload["hot_words"])
	}
}

func TestMongoCustomFeatureLogSpecialTokenization(t *testing.T) {
	a := &MongoAdapter{}
	result, err := a.ExecuteCustomFeature(context.Background(), "log_special_tokenization", map[string]interface{}{
		"text":  "ERROR ERR_TIMEOUT from 10.0.0.8 at https://api.example.com trace=abcdef1234567890",
		"rules": []string{"ip", "url", "error_code", "trace_id"},
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	payload, ok := result.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map payload, got %T", result)
	}
	if payload["strategy"] != "log_special_tokenization" {
		t.Fatalf("expected log_special_tokenization strategy, got %v", payload["strategy"])
	}
	items, ok := payload["items"].([]map[string]interface{})
	if !ok || len(items) != 1 {
		t.Fatalf("expected one tokenization item, got %+v", payload["items"])
	}
	hits, ok := items[0]["rule_hits"].(map[string][]string)
	if !ok {
		t.Fatalf("expected rule_hits map, got %T", items[0]["rule_hits"])
	}
	if len(hits["ip"]) == 0 || len(hits["url"]) == 0 || len(hits["error_code"]) == 0 {
		t.Fatalf("expected ip/url/error_code hits, got %+v", hits)
	}
}

func TestMongoCustomFeatureLogHotWordsByLevel(t *testing.T) {
	a := &MongoAdapter{}
	result, err := a.ExecuteCustomFeature(context.Background(), "log_hot_words_by_level", map[string]interface{}{
		"logs": []string{
			`{"level":"ERROR","message":"timeout on /api/v1/login trace=abcdef1234567890"}`,
			`{"level":"ERROR","message":"redis timeout while loading profile"}`,
			`{"level":"WARN","message":"timeout on /api/v1/login trace=abcdef1234567890"}`,
			`{"level":"INFO","message":"user logged in from 192.168.1.1"}`,
		},
		"level_field": "level",
		"top_k":       5,
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	payload, ok := result.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map payload, got %T", result)
	}
	if payload["strategy"] != "log_hot_words_by_level" {
		t.Fatalf("expected log_hot_words_by_level strategy, got %v", payload["strategy"])
	}
	hotWords, ok := payload["hot_words"].(map[string][]map[string]interface{})
	if !ok {
		t.Fatalf("expected hot_words map[string][]..., got %T", payload["hot_words"])
	}
	if len(hotWords) == 0 {
		t.Fatalf("expected non-empty hot words by level, got %+v", payload["hot_words"])
	}
	// 验证每个日志级别都被正确分类
	if errorWords, ok := hotWords["ERROR"]; !ok || len(errorWords) == 0 {
		t.Fatalf("expected ERROR level hot words, got %+v", hotWords)
	}
}

func TestMongoCustomFeatureLogHotWordsByTimeWindow(t *testing.T) {
	a := &MongoAdapter{}
	result, err := a.ExecuteCustomFeature(context.Background(), "log_hot_words_by_time_window", map[string]interface{}{
		"logs": []string{
			`{"timestamp":"2024-01-15T10:30:00Z","message":"timeout on /api/v1/login"}`,
			`{"timestamp":"2024-01-15T10:45:00Z","message":"redis timeout while loading"}`,
			`{"timestamp":"2024-01-15T11:30:00Z","message":"database connection error"}`,
		},
		"timestamp_field": "timestamp",
		"time_window":     "hour",
		"top_k":           5,
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	payload, ok := result.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map payload, got %T", result)
	}
	if payload["strategy"] != "log_hot_words_by_time_window" {
		t.Fatalf("expected log_hot_words_by_time_window strategy, got %v", payload["strategy"])
	}
	hotWords, ok := payload["hot_words"].(map[string][]map[string]interface{})
	if !ok {
		t.Fatalf("expected hot_words map[string][]..., got %T", payload["hot_words"])
	}
	if len(hotWords) == 0 {
		t.Fatalf("expected non-empty hot words by time window, got %+v", payload["hot_words"])
	}
	// 验证时间窗口被正确分组
	if payload["time_window"] != "hour" {
		t.Fatalf("expected time_window to be 'hour', got %v", payload["time_window"])
	}
}

func TestMongoCustomFeatureArticleDraftManagement(t *testing.T) {
	a := &MongoAdapter{}

	createResult, err := a.ExecuteCustomFeature(context.Background(), "article_draft_management", map[string]interface{}{
		"operation": "create",
		"article": map[string]interface{}{
			"title":   "My First Article",
			"content": "This is the content of my article",
		},
		"tags":     []string{"golang", "database"},
		"category": "tech",
		"priority": 5,
	})
	if err != nil {
		t.Fatalf("expected no error on create, got %v", err)
	}

	createPayload, ok := createResult.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map payload, got %T", createResult)
	}
	if createPayload["status"] != "draft_created" {
		t.Fatalf("expected draft_created status, got %v", createPayload["status"])
	}

	article, _ := createPayload["article"].(map[string]interface{})
	if isDraft, ok := article["is_draft"].(bool); !ok || !isDraft {
		t.Fatalf("expected article to be draft, got is_draft=%v", isDraft)
	}

	updateResult, err := a.ExecuteCustomFeature(context.Background(), "article_draft_management", map[string]interface{}{
		"operation": "update",
		"article": map[string]interface{}{
			"title":      "My First Article (Updated)",
			"content":    "Updated content",
			"is_draft":   true,
			"edit_count": 0,
		},
	})
	if err != nil {
		t.Fatalf("expected no error on update, got %v", err)
	}

	updatePayload, _ := updateResult.(map[string]interface{})
	if updatePayload["status"] != "draft_updated" {
		t.Fatalf("expected draft_updated status, got %v", updatePayload["status"])
	}
	if editCount, ok := updatePayload["edit_count"].(int); !ok || editCount != 1 {
		t.Fatalf("expected edit_count to be 1, got %v", editCount)
	}

	publishResult, err := a.ExecuteCustomFeature(context.Background(), "article_draft_management", map[string]interface{}{
		"operation": "publish",
		"article": map[string]interface{}{
			"title":   "My First Article (Published)",
			"content": "Published content",
			"version": 1,
		},
	})
	if err != nil {
		t.Fatalf("expected no error on publish, got %v", err)
	}

	publishPayload, _ := publishResult.(map[string]interface{})
	if publishPayload["status"] != "article_published" {
		t.Fatalf("expected article_published status, got %v", publishPayload["status"])
	}
}

func TestMongoCustomFeatureArticleTemplateRendering(t *testing.T) {
	a := &MongoAdapter{}

	result, err := a.ExecuteCustomFeature(context.Background(), "article_template_rendering", map[string]interface{}{
		"template": "Title: {{.title}}\\nAuthor: {{.author}}\\nContent: {{.content}}",
		"data": map[string]interface{}{
			"title":   "Introduction to MongoDB",
			"author":  "Alice",
			"content": "MongoDB is a NoSQL database...",
		},
		"template_name": "article_display",
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	payload, ok := result.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map payload, got %T", result)
	}

	if payload["strategy"] != "article_template_rendering" {
		t.Fatalf("expected article_template_rendering strategy, got %v", payload["strategy"])
	}

	renderedOutput, ok := payload["rendered_output"].(string)
	if !ok || len(renderedOutput) == 0 {
		t.Fatalf("expected non-empty rendered_output, got %v", payload["rendered_output"])
	}

	if !strings.Contains(renderedOutput, "Introduction to MongoDB") {
		t.Fatalf("expected 'Introduction to MongoDB' in rendered output, got: %s", renderedOutput)
	}
}

func TestMongoCustomFeatureArticleTemplateRenderingWithFunctions(t *testing.T) {
	a := &MongoAdapter{}

	result, err := a.ExecuteCustomFeature(context.Background(), "article_template_rendering", map[string]interface{}{
		"template": "Title: {{upper .title}}\\nContent: {{trim .content}}",
		"data": map[string]interface{}{
			"title":   "go programming",
			"content": "  Learn Go language  ",
		},
		"template_name":   "with_functions",
		"enable_functions": true,
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	payload, ok := result.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map payload, got %T", result)
	}

	renderedOutput := payload["rendered_output"].(string)
	if !strings.Contains(renderedOutput, "GO PROGRAMMING") {
		t.Fatalf("expected 'GO PROGRAMMING' (upper case) in output, got: %s", renderedOutput)
	}
	if !strings.Contains(renderedOutput, "Learn Go language") {
		t.Fatalf("expected trimmed 'Learn Go language' in output, got: %s", renderedOutput)
	}
}

func TestMongoCustomFeatureArticleTemplatePresetLibrary(t *testing.T) {
	a := &MongoAdapter{}

	listOut, err := a.ExecuteCustomFeature(context.Background(), "article_template_preset_library", map[string]interface{}{
		"operation": "list",
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	listPayload, ok := listOut.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map payload, got %T", listOut)
	}
	names, ok := listPayload["preset_names"].([]string)
	if !ok || len(names) < 3 {
		t.Fatalf("expected >=3 preset names, got %+v", listPayload["preset_names"])
	}

	getOut, err := a.ExecuteCustomFeature(context.Background(), "article_template_preset_library", map[string]interface{}{
		"operation": "get",
		"preset":    "blog",
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	getPayload, ok := getOut.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map payload, got %T", getOut)
	}
	tpl, ok := getPayload["template"].(string)
	if !ok || !strings.Contains(tpl, "{{.title}}") {
		t.Fatalf("expected blog template content, got %v", getPayload["template"])
	}
}

func TestMongoCustomFeatureArticleTemplateRenderingWithPresetAndSecurity(t *testing.T) {
	a := &MongoAdapter{}

	out, err := a.ExecuteCustomFeature(context.Background(), "article_template_rendering", map[string]interface{}{
		"template_preset":   "news",
		"template_name":     "news_render",
		"enable_functions":  false,
		"strict_variables":  true,
		"max_template_size": 4096,
		"data": map[string]interface{}{
			"title":    "Breaking News",
			"lead":     "Lead section",
			"content":  "Main body",
			"reporter": "Reporter A",
			"source":   "Newsroom",
		},
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	payload, ok := out.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map payload, got %T", out)
	}
	if payload["template_preset"] != "news" {
		t.Fatalf("expected preset news, got %v", payload["template_preset"])
	}
	rendered, _ := payload["rendered_output"].(string)
	if !strings.Contains(rendered, "Breaking News") {
		t.Fatalf("expected rendered preset output, got %s", rendered)
	}

	_, err = a.ExecuteCustomFeature(context.Background(), "article_template_rendering", map[string]interface{}{
		"template":         "{{upper .title}}",
		"enable_functions": true,
		"allowed_functions": []string{"trim"},
		"data": map[string]interface{}{
			"title": "test",
		},
	})
	if err == nil {
		t.Fatalf("expected error when upper is not in allowed_functions")
	}
}

func TestMongoCustomFeatureArticleDraftQueryPlan(t *testing.T) {
	a := &MongoAdapter{}

	out, err := a.ExecuteCustomFeature(context.Background(), "article_draft_query_plan", map[string]interface{}{
		"status":   "published",
		"category": "tech",
		"author_id": "u1",
		"limit":    10,
		"skip":     5,
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	payload, ok := out.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map payload, got %T", out)
	}
	if payload["strategy"] != "article_draft_query_plan" {
		t.Fatalf("expected article_draft_query_plan strategy, got %v", payload["strategy"])
	}
	queryPlan, ok := payload["query_plan"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected query_plan map, got %T", payload["query_plan"])
	}
	filter, ok := queryPlan["filter"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected filter map, got %T", queryPlan["filter"])
	}
	if v, ok := filter["is_draft"].(bool); !ok || v {
		t.Fatalf("expected published filter is_draft=false, got %+v", filter["is_draft"])
	}
	if filter["category"] != "tech" || filter["author_id"] != "u1" {
		t.Fatalf("expected category/author_id in filter, got %+v", filter)
	}

	out2, err := a.ExecuteCustomFeature(context.Background(), "article_draft_management", map[string]interface{}{
		"operation": "query_plan",
		"status":    "draft",
	})
	if err != nil {
		t.Fatalf("expected no error for draft_management query_plan, got %v", err)
	}
	payload2, _ := out2.(map[string]interface{})
	if payload2["strategy"] != "article_draft_query_plan" {
		t.Fatalf("expected article_draft_query_plan strategy, got %v", payload2["strategy"])
	}
}
