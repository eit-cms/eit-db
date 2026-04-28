package db

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

func init() {
	MustRegisterAdapterDescriptor("arango", AdapterDescriptor{
		Factory: func(cfg *Config) (Adapter, error) {
			a, err := NewArangoAdapter(cfg)
			if err != nil {
				return nil, err
			}
			if err := a.Connect(context.Background(), cfg); err != nil {
				return nil, err
			}
			return a, nil
		},
		ValidateConfig: func(cfg *Config) error {
			arangoCfg := cfg.ResolvedArangoConfig()
			cfg.Arango = arangoCfg
			u, err := url.ParseRequestURI(strings.TrimSpace(arangoCfg.URI))
			if err != nil {
				return fmt.Errorf("arango: invalid uri: %w", err)
			}
			if strings.TrimSpace(u.Scheme) == "" || strings.TrimSpace(u.Host) == "" {
				return fmt.Errorf("arango: uri must include scheme and host")
			}
			if strings.TrimSpace(arangoCfg.Database) == "" {
				return fmt.Errorf("arango: database must not be empty")
			}
			return nil
		},
		DefaultConfig: func() *Config {
			cfg := newDefaultAdapterConfig("arango")
			cfg.Arango = &ArangoConnectionConfig{
				URI:            "http://localhost:8529",
				Database:       "_system",
				Username:       "root",
				TimeoutSeconds: 10,
			}
			return cfg
		},
		Metadata: func() AdapterMetadata {
			return builtinAdapterMetadata("arango", "document", "arangodb", "arangodb")
		},
		ExecuteQueryConstructor: func(ctx context.Context, adapter Adapter, query string, args []interface{}) (*QueryConstructorExecutionResult, bool, error) {
			arangoAdapter, ok := adapter.(*ArangoAdapter)
			if !ok {
				return nil, false, nil
			}
			if !looksLikeReadAQL(query) {
				return nil, true, fmt.Errorf("ExecuteQueryConstructor is query-only for AQL write/mixed statements; use ExecuteAuto")
			}

			rows, queryErr := arangoAdapter.ExecuteAQL(ctx, query, buildCypherParamsFromArgs(args))
			if queryErr != nil {
				return nil, true, queryErr
			}

			return &QueryConstructorExecutionResult{
				Statement: query,
				Args:      copyQueryArgs(args),
				Rows:      rows,
			}, true, nil
		},
		ExecuteQueryConstructorAuto: func(ctx context.Context, adapter Adapter, query string, args []interface{}) (*QueryConstructorAutoExecutionResult, bool, error) {
			arangoAdapter, ok := adapter.(*ArangoAdapter)
			if !ok {
				return nil, false, nil
			}

			rows, execErr := arangoAdapter.ExecuteAQL(ctx, query, buildCypherParamsFromArgs(args))
			if execErr != nil {
				return nil, true, execErr
			}

			if looksLikeReadAQL(query) {
				return &QueryConstructorAutoExecutionResult{
					Mode:      "query",
					Statement: query,
					Args:      copyQueryArgs(args),
					Rows:      rows,
				}, true, nil
			}

			return &QueryConstructorAutoExecutionResult{
				Mode:      "exec",
				Statement: query,
				Args:      copyQueryArgs(args),
				Exec: &QueryConstructorExecSummary{
					RowsAffected: int64(len(rows)),
					Counters: map[string]int{
						"result_rows": len(rows),
					},
					Details: map[string]interface{}{
						"result_rows": rows,
					},
				},
			}, true, nil
		},
	})
}

// ArangoAdapter ArangoDB 适配器（MVP）。
//
// 当前阶段支持：
// 1. Connect/Ping/Close/GetRawConn
// 2. 最小 AQL 透传执行（ExecuteAQL）
//
// 当前阶段不支持 SQL 接口与事务语义。
type ArangoAdapter struct {
	client  *http.Client
	baseURL string
	config  *ArangoConnectionConfig
}

// NewArangoAdapter 创建 Arango 适配器（不建立连接）。
func NewArangoAdapter(config *Config) (*ArangoAdapter, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	resolved := config.ResolvedArangoConfig()
	return &ArangoAdapter{config: resolved}, nil
}

// Connect 建立连接并做最小探活。
func (a *ArangoAdapter) Connect(ctx context.Context, config *Config) error {
	if a.client != nil {
		return nil
	}

	cfg := a.config
	if config != nil {
		if err := config.Validate(); err != nil {
			return err
		}
		cfg = config.ResolvedArangoConfig()
		a.config = cfg
	}

	u, err := url.ParseRequestURI(strings.TrimSpace(cfg.URI))
	if err != nil {
		return fmt.Errorf("arango: invalid uri: %w", err)
	}
	baseURL := strings.TrimRight(u.String(), "/")
	timeout := time.Duration(cfg.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	client := &http.Client{Timeout: timeout}

	a.client = client
	a.baseURL = baseURL
	if err := a.Ping(ctx); err != nil {
		a.client = nil
		a.baseURL = ""
		return err
	}
	return nil
}

// Close 关闭适配器连接。
func (a *ArangoAdapter) Close() error {
	a.client = nil
	a.baseURL = ""
	return nil
}

// Ping 测试 ArangoDB 连通性。
func (a *ArangoAdapter) Ping(ctx context.Context) error {
	if a.client == nil {
		return fmt.Errorf("arango client not connected")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.baseURL+"/_api/version", nil)
	if err != nil {
		return err
	}
	a.applyAuth(req)

	resp, err := a.client.Do(req)
	if err != nil {
		return fmt.Errorf("arango ping failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("arango ping failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	return nil
}

// ExecuteAQL 执行最小 AQL 查询透传。
func (a *ArangoAdapter) ExecuteAQL(ctx context.Context, aql string, bindVars map[string]interface{}) ([]map[string]interface{}, error) {
	if a.client == nil {
		return nil, fmt.Errorf("arango client not connected")
	}
	if strings.TrimSpace(aql) == "" {
		return nil, fmt.Errorf("aql query cannot be empty")
	}

	payload := map[string]interface{}{
		"query": aql,
	}
	if bindVars != nil {
		payload["bindVars"] = bindVars
	}

	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	dbName := url.PathEscape(a.config.Database)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.baseURL+"/_db/"+dbName+"/_api/cursor", bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	a.applyAuth(req)

	resp, err := a.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= http.StatusBadRequest {
		return nil, fmt.Errorf("arango aql request failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var out struct {
		Error        bool                     `json:"error"`
		ErrorMessage string                   `json:"errorMessage"`
		HasMore      bool                     `json:"hasMore"`
		Result       []map[string]interface{} `json:"result"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	if out.Error {
		return nil, fmt.Errorf("arango aql execution failed: %s", out.ErrorMessage)
	}
	if out.HasMore {
		return nil, fmt.Errorf("arango aql cursor pagination is not supported in MVP")
	}
	return out.Result, nil
}

// GetRawConn 返回底层 HTTP 客户端。
func (a *ArangoAdapter) GetRawConn() interface{} {
	if a.client == nil {
		return nil
	}
	return a.client
}

// Query 当前不支持 SQL 查询。
func (a *ArangoAdapter) Query(_ context.Context, _ string, _ ...interface{}) (*sql.Rows, error) {
	return nil, fmt.Errorf("arango adapter does not support SQL Query")
}

// QueryRow 当前不支持 SQL 查询。
func (a *ArangoAdapter) QueryRow(_ context.Context, _ string, _ ...interface{}) *sql.Row {
	return nil
}

// Exec 当前不支持 SQL 执行。
func (a *ArangoAdapter) Exec(_ context.Context, _ string, _ ...interface{}) (sql.Result, error) {
	return nil, fmt.Errorf("arango adapter does not support SQL Exec directly; use ExecuteAQL")
}

// Begin 当前不支持事务接口。
func (a *ArangoAdapter) Begin(_ context.Context, _ ...interface{}) (Tx, error) {
	return nil, fmt.Errorf("arango adapter transaction Begin is not supported in MVP")
}

func (a *ArangoAdapter) RegisterScheduledTask(_ context.Context, _ *ScheduledTaskConfig) error {
	return NewScheduledTaskFallbackErrorWithReason("arango", ScheduledTaskFallbackReasonAdapterUnsupported, "native scheduled tasks not supported")
}

func (a *ArangoAdapter) UnregisterScheduledTask(_ context.Context, _ string) error {
	return NewScheduledTaskFallbackErrorWithReason("arango", ScheduledTaskFallbackReasonAdapterUnsupported, "native scheduled tasks not supported")
}

func (a *ArangoAdapter) ListScheduledTasks(_ context.Context) ([]*ScheduledTaskStatus, error) {
	return nil, NewScheduledTaskFallbackErrorWithReason("arango", ScheduledTaskFallbackReasonAdapterUnsupported, "native scheduled tasks not supported")
}

// GetQueryBuilderProvider 当前阶段仅支持 AQL 透传，不提供 QueryConstructorProvider。
func (a *ArangoAdapter) GetQueryBuilderProvider() QueryConstructorProvider {
	return nil
}

func (a *ArangoAdapter) GetDatabaseFeatures() *DatabaseFeatures {
	return NewArangoDatabaseFeatures()
}

func (a *ArangoAdapter) GetQueryFeatures() *QueryFeatures {
	return NewArangoQueryFeatures()
}

// InspectArangoRuntime 探测 Arango 运行时能力。
func (a *ArangoAdapter) InspectArangoRuntime(ctx context.Context) (*ArangoRuntimeCapability, error) {
	if a.client == nil {
		return nil, fmt.Errorf("arango client not connected")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.baseURL+"/_api/version", nil)
	if err != nil {
		return nil, err
	}
	a.applyAuth(req)

	resp, err := a.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("arango runtime inspect failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= http.StatusBadRequest {
		return nil, fmt.Errorf("arango runtime inspect failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var payload struct {
		Server  string `json:"server"`
		Version string `json:"version"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, err
	}

	isArango := strings.EqualFold(strings.TrimSpace(payload.Server), "arango")
	version := strings.TrimSpace(payload.Version)

	notes := ""
	if a.config != nil {
		notes = "database=" + strings.TrimSpace(a.config.Database)
	}

	return &ArangoRuntimeCapability{
		NativeSupported: isArango && version != "",
		Version:         version,
		Notes:           notes,
	}, nil
}

func (a *ArangoAdapter) requestArangoAPI(ctx context.Context, method string, path string, payload interface{}) (string, int, error) {
	if a.client == nil {
		return "", 0, fmt.Errorf("arango client not connected")
	}
	if strings.TrimSpace(path) == "" {
		return "", 0, fmt.Errorf("arango api path must not be empty")
	}
	method = strings.TrimSpace(strings.ToUpper(method))
	if method == "" {
		method = http.MethodGet
	}

	var bodyReader io.Reader
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			return "", 0, err
		}
		bodyReader = bytes.NewReader(raw)
	}

	requestURL := a.baseURL
	if strings.HasPrefix(path, "/") {
		requestURL += path
	} else {
		requestURL += "/" + path
	}

	req, err := http.NewRequestWithContext(ctx, method, requestURL, bodyReader)
	if err != nil {
		return "", 0, err
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	a.applyAuth(req)

	resp, err := a.client.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", resp.StatusCode, err
	}
	return string(body), resp.StatusCode, nil
}

func (a *ArangoAdapter) applyAuth(req *http.Request) {
	if req == nil || a.config == nil {
		return
	}
	if strings.TrimSpace(a.config.Username) == "" {
		return
	}
	credential := a.config.Username + ":" + a.config.Password
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(credential)))
}
