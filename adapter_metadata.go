package db

import (
	"reflect"
	"strings"
	"sync"
)

// AdapterMetadata 描述适配器的统一元信息。
type AdapterMetadata struct {
	Name       string
	DriverKind string
	Vendor     string
	Version    string
	Aliases    []string
	Extra      map[string]string
}

// AdapterMetadataProvider 允许适配器实例主动提供元信息。
type AdapterMetadataProvider interface {
	Metadata() AdapterMetadata
}

var (
	adapterConcreteTypeIndexMu sync.RWMutex
	adapterConcreteTypeIndex   = make(map[reflect.Type]string)
)

func rememberAdapterConcreteType(adapter Adapter, adapterType string) {
	if adapter == nil {
		return
	}
	t := reflect.TypeOf(adapter)
	if t == nil {
		return
	}
	name := normalizeAdapterName(adapterType)
	if name == "" {
		return
	}

	adapterConcreteTypeIndexMu.Lock()
	adapterConcreteTypeIndex[t] = name
	adapterConcreteTypeIndexMu.Unlock()
}

func lookupAdapterTypeByConcreteType(adapter Adapter) (string, bool) {
	if adapter == nil {
		return "", false
	}
	t := reflect.TypeOf(adapter)
	if t == nil {
		return "", false
	}

	adapterConcreteTypeIndexMu.RLock()
	name, ok := adapterConcreteTypeIndex[t]
	adapterConcreteTypeIndexMu.RUnlock()
	return name, ok
}

func normalizeAdapterMetadata(meta AdapterMetadata, defaultName string) AdapterMetadata {
	meta.Name = normalizeAdapterName(meta.Name)
	if meta.Name == "" {
		meta.Name = normalizeAdapterName(defaultName)
	}
	meta.DriverKind = strings.ToLower(strings.TrimSpace(meta.DriverKind))
	meta.Vendor = strings.TrimSpace(meta.Vendor)
	meta.Version = strings.TrimSpace(meta.Version)

	if len(meta.Aliases) > 0 {
		aliases := make([]string, 0, len(meta.Aliases))
		seen := make(map[string]struct{}, len(meta.Aliases))
		for _, raw := range meta.Aliases {
			alias := normalizeAdapterName(raw)
			if alias == "" || alias == meta.Name {
				continue
			}
			if _, exists := seen[alias]; exists {
				continue
			}
			seen[alias] = struct{}{}
			aliases = append(aliases, alias)
		}
		meta.Aliases = aliases
	}

	if meta.Extra != nil {
		normalizedExtra := make(map[string]string, len(meta.Extra))
		for k, v := range meta.Extra {
			key := strings.TrimSpace(k)
			if key == "" {
				continue
			}
			normalizedExtra[key] = strings.TrimSpace(v)
		}
		meta.Extra = normalizedExtra
	}

	if meta.Name == "" {
		meta.Name = "unknown"
	}
	return meta
}

func reflectAdapterNameHeuristic(adapter Adapter) string {
	if adapter == nil {
		return "unknown"
	}
	t := reflect.TypeOf(adapter)
	if t == nil {
		return "unknown"
	}
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	name := strings.ToLower(strings.TrimSpace(t.Name()))
	if name == "" {
		return "unknown"
	}
	name = strings.TrimSuffix(name, "adapter")
	name = strings.TrimSuffix(name, "db")
	name = strings.Trim(name, "_-")
	if name == "" {
		return "unknown"
	}
	return name
}

func resolveAdapterMetadata(adapterType string, adapter Adapter) AdapterMetadata {
	normalizedType := normalizeAdapterName(adapterType)
	if normalizedType != "" {
		if desc, ok := LookupAdapterDescriptor(normalizedType); ok && desc.Metadata != nil {
			return normalizeAdapterMetadata(desc.Metadata(), normalizedType)
		}
		return normalizeAdapterMetadata(AdapterMetadata{Name: normalizedType}, normalizedType)
	}

	if provider, ok := adapter.(AdapterMetadataProvider); ok {
		meta := provider.Metadata()
		if normalizeAdapterName(meta.Name) != "" {
			return normalizeAdapterMetadata(meta, "")
		}
	}

	if indexed, ok := lookupAdapterTypeByConcreteType(adapter); ok {
		if desc, found := LookupAdapterDescriptor(indexed); found && desc.Metadata != nil {
			return normalizeAdapterMetadata(desc.Metadata(), indexed)
		}
		return normalizeAdapterMetadata(AdapterMetadata{Name: indexed}, indexed)
	}

	if provider, ok := adapter.(AdapterMetadataProvider); ok {
		return normalizeAdapterMetadata(provider.Metadata(), reflectAdapterNameHeuristic(adapter))
	}

	return normalizeAdapterMetadata(AdapterMetadata{Name: reflectAdapterNameHeuristic(adapter)}, "unknown")
}

// ResolveAdapterMetadata 解析适配器元信息（公开 API）。
//
// 解析优先级：
// 1) adapterType 对应 descriptor.Metadata
// 2) adapterType 名称本身
// 3) AdapterMetadataProvider
// 4) concrete type 索引映射
// 5) 反射名称启发式兜底
func ResolveAdapterMetadata(adapterType string, adapter Adapter) AdapterMetadata {
	return resolveAdapterMetadata(adapterType, adapter)
}

func (r *Repository) resolveAdapterMetadata(adapter Adapter) AdapterMetadata {
	if r == nil {
		return resolveAdapterMetadata("", adapter)
	}
	return resolveAdapterMetadata(r.adapterType, adapter)
}

// GetAdapterMetadata 返回当前 Repository 的适配器元信息。
func (r *Repository) GetAdapterMetadata() AdapterMetadata {
	if r == nil {
		return ResolveAdapterMetadata("", nil)
	}
	r.mu.RLock()
	adapter := r.adapter
	adapterType := r.adapterType
	r.mu.RUnlock()
	return ResolveAdapterMetadata(adapterType, adapter)
}
