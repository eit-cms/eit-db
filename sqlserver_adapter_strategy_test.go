package db

import "testing"

func TestSQLServerAdapterProviderUsesConfiguredManyToManyStrategy(t *testing.T) {
	cfg := &Config{
		Adapter: "sqlserver",
		SQLServer: &SQLServerConnectionConfig{
			Host:                     "localhost",
			Port:                     1433,
			Username:                 "sa",
			Database:                 "master",
			ManyToManyStrategy:       "recursive_cte",
			RecursiveCTEDepth:        15,
			RecursiveCTEMaxRecursion: 300,
			DSN:                      "sqlserver://sa:pass@localhost:1433?database=master&encrypt=disable",
		},
	}

	a := &SQLServerAdapter{config: cfg}
	provider := a.GetQueryBuilderProvider()
	sqlProvider, ok := provider.(*DefaultSQLQueryConstructorProvider)
	if !ok {
		t.Fatalf("expected DefaultSQLQueryConstructorProvider, got %T", provider)
	}
	dialect, ok := sqlProvider.dialect.(*SQLServerDialect)
	if !ok {
		t.Fatalf("expected SQLServerDialect, got %T", sqlProvider.dialect)
	}
	if dialect.SQLManyToManyStrategy() != "recursive_cte" {
		t.Fatalf("expected dialect strategy recursive_cte, got %s", dialect.SQLManyToManyStrategy())
	}
	if dialect.SQLRecursiveCTEDepth() != 15 || dialect.SQLRecursiveCTEMaxRecursion() != 300 {
		t.Fatalf("expected recursive cte options depth=15 max=300, got depth=%d max=%d", dialect.SQLRecursiveCTEDepth(), dialect.SQLRecursiveCTEMaxRecursion())
	}
}
