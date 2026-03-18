package db

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestPresetsRemainAppLayer 通过关键字守卫，避免 Adapter 细节混入 Presets。
// 这是启发式检查：命中关键字时提醒评审是否发生了分层越界。
func TestPresetsRemainAppLayer(t *testing.T) {
	files, err := filepath.Glob("*preset*.go")
	if err != nil {
		t.Fatalf("glob preset files failed: %v", err)
	}

	forbiddenTokens := []string{
		"go.mongodb.org/mongo-driver",
		"github.com/neo4j/neo4j-go-driver",
		"github.com/lib/pq",
		"github.com/go-sql-driver/mysql",
		"github.com/microsoft/go-mssqldb",
		"github.com/mattn/go-sqlite3",
	}

	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}

		b, readErr := os.ReadFile(file)
		if readErr != nil {
			t.Fatalf("read file %s failed: %v", file, readErr)
		}

		content := strings.ToLower(string(b))
		for _, token := range forbiddenTokens {
			if strings.Contains(content, token) {
				t.Fatalf("preset layering violation: file %s imports adapter-layer dependency %q", file, token)
			}
		}
	}
}
