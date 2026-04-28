package db

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"
)

// Blueprint 描述一个跨数据集编排单元。
//
// 该抽象是 Query 增强与跨库编排的上位元信息载体，当前阶段仅用于：
// 1. 配置加载
// 2. 结构校验
// 3. 注册管理
// 4. 为运行时提供路由提示元信息
//
// 当前不直接驱动查询执行。
type Blueprint struct {
	ID          string                `json:"id" yaml:"id"`
	Version     string                `json:"version" yaml:"version"`
	Owner       string                `json:"owner" yaml:"owner"`
	Datasets    []BlueprintDataset    `json:"datasets,omitempty" yaml:"datasets,omitempty"`
	Entities    []BlueprintEntity     `json:"entities,omitempty" yaml:"entities,omitempty"`
	Relations   []BlueprintRelation   `json:"relations,omitempty" yaml:"relations,omitempty"`
	Modules     []BlueprintModuleSlot `json:"modules,omitempty" yaml:"modules,omitempty"`
	CachePolicy BlueprintCachePolicy  `json:"cache_policy,omitempty" yaml:"cache_policy,omitempty"`
	Metadata    map[string]string     `json:"metadata,omitempty" yaml:"metadata,omitempty"`
}

// BlueprintDataset 描述 Blueprint 参与的数据集边界。
type BlueprintDataset struct {
	Name      string `json:"name" yaml:"name"`
	Adapter   string `json:"adapter" yaml:"adapter"`
	Namespace string `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	Database  string `json:"database,omitempty" yaml:"database,omitempty"`
	Role      string `json:"role,omitempty" yaml:"role,omitempty"`
	Managed   bool   `json:"managed,omitempty" yaml:"managed,omitempty"`
}

// BlueprintEntity 描述统一实体与多源映射。
type BlueprintEntity struct {
	Name    string                  `json:"name" yaml:"name"`
	Sources []BlueprintEntitySource `json:"sources,omitempty" yaml:"sources,omitempty"`
}

// BlueprintEntitySource 描述实体在某个 dataset 中的落点。
type BlueprintEntitySource struct {
	Dataset    string            `json:"dataset" yaml:"dataset"`
	Resource   string            `json:"resource" yaml:"resource"`
	Schema     string            `json:"schema,omitempty" yaml:"schema,omitempty"`
	PrimaryKey string            `json:"primary_key,omitempty" yaml:"primary_key,omitempty"`
	FieldMap   map[string]string `json:"field_map,omitempty" yaml:"field_map,omitempty"`
}

// BlueprintRelation 描述实体间关系语义。
type BlueprintRelation struct {
	Name          string  `json:"name" yaml:"name"`
	FromEntity    string  `json:"from_entity" yaml:"from_entity"`
	ToEntity      string  `json:"to_entity" yaml:"to_entity"`
	Type          string  `json:"type" yaml:"type"`
	SourceDataset string  `json:"source_dataset,omitempty" yaml:"source_dataset,omitempty"`
	Confidence    float64 `json:"confidence,omitempty" yaml:"confidence,omitempty"`
}

// BlueprintModuleSlot 描述查询模块的挂载点。
type BlueprintModuleSlot struct {
	Name           string `json:"name" yaml:"name"`
	Entity         string `json:"entity,omitempty" yaml:"entity,omitempty"`
	Purpose        string `json:"purpose,omitempty" yaml:"purpose,omitempty"`
	DefaultDataset string `json:"default_dataset,omitempty" yaml:"default_dataset,omitempty"`
	QueryRef       string `json:"query_ref,omitempty" yaml:"query_ref,omitempty"`
}

// BlueprintCachePolicy 描述跨库缓存策略。
type BlueprintCachePolicy struct {
	L1Enabled      bool `json:"l1_enabled,omitempty" yaml:"l1_enabled,omitempty"`
	L2Enabled      bool `json:"l2_enabled,omitempty" yaml:"l2_enabled,omitempty"`
	L3Enabled      bool `json:"l3_enabled,omitempty" yaml:"l3_enabled,omitempty"`
	CrossSource    bool `json:"cross_source,omitempty" yaml:"cross_source,omitempty"`
	TTLSeconds     int  `json:"ttl_seconds,omitempty" yaml:"ttl_seconds,omitempty"`
	WriteBackAsync bool `json:"write_back_async,omitempty" yaml:"write_back_async,omitempty"`
}

// BlueprintRouteHint 描述 Blueprint 为当前 Repository 解析出的路由提示。
type BlueprintRouteHint struct {
	BlueprintID       string   `json:"blueprint_id"`
	Entity            string   `json:"entity,omitempty"`
	Module            string   `json:"module,omitempty"`
	Adapter           string   `json:"adapter,omitempty"`
	CandidateDatasets []string `json:"candidate_datasets,omitempty"`
	DefaultDataset    string   `json:"default_dataset,omitempty"`
	ManagedPreferred  bool     `json:"managed_preferred,omitempty"`
	Notes             string   `json:"notes,omitempty"`
}

// Validate 校验 Blueprint 的最小结构正确性。
func (b *Blueprint) Validate() error {
	if b == nil {
		return fmt.Errorf("blueprint cannot be nil")
	}
	if strings.TrimSpace(b.ID) == "" {
		return fmt.Errorf("blueprint id must not be empty")
	}
	if strings.TrimSpace(b.Version) == "" {
		return fmt.Errorf("blueprint version must not be empty")
	}
	if len(b.Datasets) == 0 {
		return fmt.Errorf("blueprint %s must define at least one dataset", b.ID)
	}
	if len(b.Entities) == 0 {
		return fmt.Errorf("blueprint %s must define at least one entity", b.ID)
	}

	datasetNames := make(map[string]BlueprintDataset, len(b.Datasets))
	for _, ds := range b.Datasets {
		name := strings.TrimSpace(ds.Name)
		if name == "" {
			return fmt.Errorf("blueprint %s contains dataset with empty name", b.ID)
		}
		if _, exists := datasetNames[name]; exists {
			return fmt.Errorf("blueprint %s contains duplicate dataset %s", b.ID, name)
		}
		if strings.TrimSpace(ds.Adapter) == "" {
			return fmt.Errorf("blueprint %s dataset %s adapter must not be empty", b.ID, name)
		}
		datasetNames[name] = ds
	}

	entityNames := make(map[string]BlueprintEntity, len(b.Entities))
	for _, entity := range b.Entities {
		name := strings.TrimSpace(entity.Name)
		if name == "" {
			return fmt.Errorf("blueprint %s contains entity with empty name", b.ID)
		}
		if _, exists := entityNames[name]; exists {
			return fmt.Errorf("blueprint %s contains duplicate entity %s", b.ID, name)
		}
		if len(entity.Sources) == 0 {
			return fmt.Errorf("blueprint %s entity %s must define at least one source", b.ID, name)
		}
		for _, source := range entity.Sources {
			if strings.TrimSpace(source.Dataset) == "" {
				return fmt.Errorf("blueprint %s entity %s contains source with empty dataset", b.ID, name)
			}
			if _, ok := datasetNames[strings.TrimSpace(source.Dataset)]; !ok {
				return fmt.Errorf("blueprint %s entity %s references unknown dataset %s", b.ID, name, source.Dataset)
			}
			if strings.TrimSpace(source.Resource) == "" {
				return fmt.Errorf("blueprint %s entity %s source resource must not be empty", b.ID, name)
			}
		}
		entityNames[name] = entity
	}

	for _, rel := range b.Relations {
		if strings.TrimSpace(rel.Name) == "" {
			return fmt.Errorf("blueprint %s contains relation with empty name", b.ID)
		}
		if _, ok := entityNames[strings.TrimSpace(rel.FromEntity)]; !ok {
			return fmt.Errorf("blueprint %s relation %s references unknown from_entity %s", b.ID, rel.Name, rel.FromEntity)
		}
		if _, ok := entityNames[strings.TrimSpace(rel.ToEntity)]; !ok {
			return fmt.Errorf("blueprint %s relation %s references unknown to_entity %s", b.ID, rel.Name, rel.ToEntity)
		}
		if strings.TrimSpace(rel.SourceDataset) != "" {
			if _, ok := datasetNames[strings.TrimSpace(rel.SourceDataset)]; !ok {
				return fmt.Errorf("blueprint %s relation %s references unknown source_dataset %s", b.ID, rel.Name, rel.SourceDataset)
			}
		}
		if rel.Confidence < 0 || rel.Confidence > 1 {
			return fmt.Errorf("blueprint %s relation %s confidence must be between 0 and 1", b.ID, rel.Name)
		}
	}

	for _, mod := range b.Modules {
		if strings.TrimSpace(mod.Name) == "" {
			return fmt.Errorf("blueprint %s contains module slot with empty name", b.ID)
		}
		if strings.TrimSpace(mod.Entity) != "" {
			if _, ok := entityNames[strings.TrimSpace(mod.Entity)]; !ok {
				return fmt.Errorf("blueprint %s module %s references unknown entity %s", b.ID, mod.Name, mod.Entity)
			}
		}
		if strings.TrimSpace(mod.DefaultDataset) != "" {
			if _, ok := datasetNames[strings.TrimSpace(mod.DefaultDataset)]; !ok {
				return fmt.Errorf("blueprint %s module %s references unknown default_dataset %s", b.ID, mod.Name, mod.DefaultDataset)
			}
		}
	}

	return nil
}

// FindEntity 返回指定实体。
func (b *Blueprint) FindEntity(name string) (BlueprintEntity, bool) {
	if b == nil {
		return BlueprintEntity{}, false
	}
	target := strings.TrimSpace(name)
	for _, entity := range b.Entities {
		if strings.TrimSpace(entity.Name) == target {
			return entity, true
		}
	}
	return BlueprintEntity{}, false
}

// FindModule 返回指定模块。
func (b *Blueprint) FindModule(name string) (BlueprintModuleSlot, bool) {
	if b == nil {
		return BlueprintModuleSlot{}, false
	}
	target := strings.TrimSpace(name)
	for _, mod := range b.Modules {
		if strings.TrimSpace(mod.Name) == target {
			return mod, true
		}
	}
	return BlueprintModuleSlot{}, false
}

// CandidateDatasetsForEntity 返回实体的候选数据集。
func (b *Blueprint) CandidateDatasetsForEntity(entityName string, adapterName string) []BlueprintDataset {
	if b == nil {
		return nil
	}
	entity, ok := b.FindEntity(entityName)
	if !ok {
		return nil
	}
	adapterName = normalizeAdapterName(adapterName)
	datasetIndex := make(map[string]BlueprintDataset, len(b.Datasets))
	for _, ds := range b.Datasets {
		datasetIndex[strings.TrimSpace(ds.Name)] = ds
	}
	out := make([]BlueprintDataset, 0, len(entity.Sources))
	for _, source := range entity.Sources {
		ds, exists := datasetIndex[strings.TrimSpace(source.Dataset)]
		if !exists {
			continue
		}
		if adapterName != "" && normalizeAdapterName(ds.Adapter) != adapterName {
			continue
		}
		out = append(out, ds)
	}
	return out
}

// BlueprintRegistry 维护 Blueprint 注册表。
type BlueprintRegistry struct {
	mu         sync.RWMutex
	blueprints map[string]*Blueprint
}

// NewBlueprintRegistry 创建 Blueprint 注册表。
func NewBlueprintRegistry() *BlueprintRegistry {
	return &BlueprintRegistry{blueprints: make(map[string]*Blueprint)}
}

// Register 注册 Blueprint。
func (r *BlueprintRegistry) Register(bp *Blueprint) error {
	if r == nil {
		return fmt.Errorf("blueprint registry is nil")
	}
	if err := bp.Validate(); err != nil {
		return err
	}
	id := strings.TrimSpace(bp.ID)
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.blueprints[id]; exists {
		return fmt.Errorf("blueprint %s already registered", id)
	}
	clone := *bp
	r.blueprints[id] = &clone
	return nil
}

// MustRegister 注册失败时 panic。
func (r *BlueprintRegistry) MustRegister(bp *Blueprint) {
	if err := r.Register(bp); err != nil {
		panic(err)
	}
}

// Get 返回 Blueprint。
func (r *BlueprintRegistry) Get(id string) (*Blueprint, bool) {
	if r == nil {
		return nil, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	bp, ok := r.blueprints[strings.TrimSpace(id)]
	return bp, ok
}

// ListIDs 返回所有 Blueprint ID。
func (r *BlueprintRegistry) ListIDs() []string {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]string, 0, len(r.blueprints))
	for id := range r.blueprints {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// SetBlueprintRegistry 将 Blueprint 注册表挂载到 Repository。
func (r *Repository) SetBlueprintRegistry(registry *BlueprintRegistry) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.blueprintRegistry = registry
	r.mu.Unlock()
}

// GetBlueprintRegistry 返回 Repository 当前持有的 Blueprint 注册表。
func (r *Repository) GetBlueprintRegistry() *BlueprintRegistry {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.blueprintRegistry
}

// ResolveRouteHint 为当前 Repository 解析 Blueprint 路由提示。
func (r *Repository) ResolveBlueprintRouteHint(bp *Blueprint, entityName string, moduleName string) (*BlueprintRouteHint, error) {
	if bp == nil {
		return nil, fmt.Errorf("blueprint cannot be nil")
	}
	if err := bp.Validate(); err != nil {
		return nil, err
	}
	meta := r.GetAdapterMetadata()
	adapterName := normalizeAdapterName(meta.Name)

	candidates := bp.CandidateDatasetsForEntity(entityName, adapterName)
	candidateNames := make([]string, 0, len(candidates))
	managedPreferred := false
	for _, ds := range candidates {
		candidateNames = append(candidateNames, ds.Name)
		if ds.Managed {
			managedPreferred = true
		}
	}

	hint := &BlueprintRouteHint{
		BlueprintID:       bp.ID,
		Entity:            strings.TrimSpace(entityName),
		Module:            strings.TrimSpace(moduleName),
		Adapter:           adapterName,
		CandidateDatasets: candidateNames,
		ManagedPreferred:  managedPreferred,
	}

	if moduleName != "" {
		if mod, ok := bp.FindModule(moduleName); ok {
			hint.DefaultDataset = strings.TrimSpace(mod.DefaultDataset)
		}
	}
	if hint.DefaultDataset == "" && len(candidateNames) > 0 {
		hint.DefaultDataset = candidateNames[0]
	}
	if len(candidateNames) == 0 {
		hint.Notes = "no candidate dataset matches current repository adapter"
	}
	return hint, nil
}

// ResolveBlueprintRouteHintByID 通过 Repository 上挂载的 BlueprintRegistry 解析路由提示。
func (r *Repository) ResolveBlueprintRouteHintByID(blueprintID string, entityName string, moduleName string) (*BlueprintRouteHint, error) {
	if r == nil {
		return nil, fmt.Errorf("repository cannot be nil")
	}
	registry := r.GetBlueprintRegistry()
	if registry == nil {
		return nil, fmt.Errorf("blueprint registry is not configured")
	}
	bp, ok := registry.Get(blueprintID)
	if !ok {
		return nil, fmt.Errorf("blueprint %s not found", strings.TrimSpace(blueprintID))
	}
	return r.ResolveBlueprintRouteHint(bp, entityName, moduleName)
}

// LoadBlueprintFromBytes 从 JSON/YAML 内容加载 Blueprint。
func LoadBlueprintFromBytes(data []byte) (*Blueprint, error) {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" {
		return nil, fmt.Errorf("blueprint payload cannot be empty")
	}
	var bp Blueprint
	var err error
	if strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[") {
		err = json.Unmarshal(data, &bp)
	} else {
		err = yaml.Unmarshal(data, &bp)
	}
	if err != nil {
		return nil, err
	}
	return &bp, bp.Validate()
}

// LoadBlueprintFile 从文件加载 Blueprint。
func LoadBlueprintFile(path string) (*Blueprint, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("blueprint file path cannot be empty")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	bp, err := LoadBlueprintFromBytes(data)
	if err != nil {
		return nil, fmt.Errorf("load blueprint %s failed: %w", filepath.Base(path), err)
	}
	return bp, nil
}
