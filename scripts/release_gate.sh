#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

MODE="${1:-quick}"
ENABLE_LINT="${EIT_GATE_ENABLE_LINT:-auto}"   # auto|on|off
RUN_OPTIONAL_DB="${EIT_GATE_RUN_OPTIONAL_DB:-0}" # 0|1
CHECK_SCHEMA_PATH="${EIT_GATE_CHECK_SCHEMA_PATH:-0}" # 0|1

if [[ "$MODE" != "quick" && "$MODE" != "full" ]]; then
  echo "Usage: scripts/release_gate.sh [quick|full]"
  exit 2
fi

step() {
  local name="$1"
  echo ""
  echo "==> ${name}"
}

run_lint() {
  case "$ENABLE_LINT" in
    off)
      echo "lint disabled (EIT_GATE_ENABLE_LINT=off)"
      return 0
      ;;
    auto)
      if ! command -v golangci-lint >/dev/null 2>&1; then
        echo "golangci-lint not found, skipping lint (auto mode)"
        return 0
      fi
      ;;
    on)
      if ! command -v golangci-lint >/dev/null 2>&1; then
        echo "golangci-lint not found, but lint is required (EIT_GATE_ENABLE_LINT=on)"
        return 1
      fi
      ;;
    *)
      echo "invalid EIT_GATE_ENABLE_LINT value: ${ENABLE_LINT}"
      return 2
      ;;
  esac

  step "Lint"
  (cd "$ROOT_DIR" && golangci-lint run ./...)
}

run_root_tests() {
  step "Root unit/regression tests"
  (cd "$ROOT_DIR" && go test -count=1 ./...)
}

run_key_integration_sqlite() {
  step "Key integration tests (SQLite application suite)"
  (cd "$ROOT_DIR/adapter-application-tests" && go test -count=1 -run SQLite ./...)
}

run_optional_db_tests() {
  if [[ "$RUN_OPTIONAL_DB" != "1" ]]; then
    echo "optional DB tests skipped (set EIT_GATE_RUN_OPTIONAL_DB=1 to enable)"
    return 0
  fi

  step "Optional DB integration tests (application suite)"
  (cd "$ROOT_DIR/adapter-application-tests" && go test -count=1 ./...)

  step "Optional DB backend tests"
  (cd "$ROOT_DIR/adapter-backend-tests" && go test -count=1 ./...)
}

run_schema_path_audit() {
  if [[ "$CHECK_SCHEMA_PATH" != "1" ]]; then
    echo "schema-path audit skipped (set EIT_GATE_CHECK_SCHEMA_PATH=1 to enable)"
    return 0
  fi

  step "Architecture invariant audit (Schema Builder DDL path)"

  local mysql_file="$ROOT_DIR/mysql_dynamic_table.go"
  local sqlite_file="$ROOT_DIR/sqlite_dynamic_table.go"
  local pg_file="$ROOT_DIR/postgres_dynamic_table.go"
  local framework_file="$ROOT_DIR/framework_tables.go"
  local migration_v1_file="$ROOT_DIR/migration.go"
  local migration_v2_file="$ROOT_DIR/migration_v2.go"

  for f in "$mysql_file" "$sqlite_file" "$pg_file" "$framework_file" "$migration_v1_file" "$migration_v2_file"; do
    if [[ ! -f "$f" ]]; then
      echo "required file missing for schema-path audit: $f"
      return 1
    fi
  done

  # Dynamic table create paths should use config -> Schema -> buildCreateTableSQL.
  grep -q "schema := config.toSchema(tableName)" "$mysql_file" || { echo "mysql dynamic table does not convert config to schema"; return 1; }
  grep -q "createSQL := buildCreateTableSQL(repo, schema)" "$mysql_file" || { echo "mysql dynamic table does not use buildCreateTableSQL"; return 1; }

  grep -q "schema := config.toSchema(tableName)" "$sqlite_file" || { echo "sqlite dynamic table does not convert config to schema"; return 1; }
  grep -q "createSQL := buildCreateTableSQL(repo, schema)" "$sqlite_file" || { echo "sqlite dynamic table does not use buildCreateTableSQL"; return 1; }

  grep -q "schema := config.toSchema(tableName)" "$pg_file" || { echo "postgres dynamic table manual path does not convert config to schema"; return 1; }
  grep -q "createSQL := buildCreateTableSQL(repo, schema)" "$pg_file" || { echo "postgres dynamic table manual path does not use buildCreateTableSQL"; return 1; }
  grep -q "columnsSQL := h.buildDynamicTableColumnsSQL(config)" "$pg_file" || { echo "postgres trigger path does not use shared schema-builder columns"; return 1; }

  # Prevent accidental reintroduction of adapter-local field type mappers.
  local legacy_mapper_pattern="func \(h \*MySQLDynamicTableHook\) mapFieldType|func \(h \*SQLiteDynamicTableHook\) mapFieldType|func \(h \*PostgreSQLDynamicTableHook\) mapFieldType"
  if command -v rg >/dev/null 2>&1; then
    if rg -n "$legacy_mapper_pattern" "$ROOT_DIR" >/dev/null; then
      echo "legacy adapter-local dynamic-table field mappers found"
      return 1
    fi
  else
    if grep -R -n -E "$legacy_mapper_pattern" "$ROOT_DIR"/*.go >/dev/null; then
      echo "legacy adapter-local dynamic-table field mappers found"
      return 1
    fi
  fi

  # Framework tables should still route through Schema Builder create SQL.
  grep -q "createSQL := buildCreateTableSQL(repo, schema)" "$framework_file" || { echo "framework table helper does not use buildCreateTableSQL"; return 1; }
  grep -q "ensureFrameworkTableUsingSchema" "$migration_v1_file" || { echo "migration v1 does not route framework table creation through schema helper"; return 1; }
  grep -q "ensureFrameworkTableUsingSchema" "$migration_v2_file" || { echo "migration v2 does not route framework table creation through schema helper"; return 1; }

  echo "schema-path audit passed"
}

main() {
  echo "Running release gate in ${MODE} mode"
  echo "Root: ${ROOT_DIR}"

  run_root_tests
  run_key_integration_sqlite
  run_schema_path_audit

  if [[ "$MODE" == "full" ]]; then
    run_optional_db_tests
  fi

  run_lint

  echo ""
  echo "Release gate PASSED"
}

main
