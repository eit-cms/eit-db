package db

import "context"

// ArangoRuntimeCapability Arango 运行时能力探测结果。
type ArangoRuntimeCapability struct {
	// Arango 服务是否可用。
	NativeSupported bool

	// 运行时检测到的 Arango 版本。
	Version string

	// 额外说明。
	Notes string
}

// ArangoRuntimeInspector 适配器可选接口：用于运行时探测 Arango 能力。
type ArangoRuntimeInspector interface {
	InspectArangoRuntime(ctx context.Context) (*ArangoRuntimeCapability, error)
}
