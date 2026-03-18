# Release Gate

This document defines the minimal release-blocking checks for v1.0 and how to run them locally and in CI.

## Scope

The release gate is intentionally small and deterministic:

1. Root unit/regression tests: `go test -count=1 ./...`
2. Key integration tests (SQLite application suite): `adapter-application-tests` with `-run SQLite`
3. Architecture invariant check: framework/internal table DDL and dynamic-table DDL must be generated via Schema Builder path (can run as script audit when enabled)
4. Lint stage (optional): runs only when enabled

If any stage fails, the gate fails immediately with non-zero exit code.

## Architecture Invariant (Release Blocking)

Before release, verify these items are true:

1. Migration framework tables (for example `schema_migrations`) are created through Schema Builder-based DDL generation.
2. Dynamic table creation in SQL adapters is routed through Schema Builder generation path.
3. No duplicated handwritten adapter-specific `CREATE TABLE` branch exists for the same framework/internal table behavior.

Suggested quick audit command:

```bash
rg "CREATE TABLE" *_dynamic_table.go migration*.go framework_tables.go
```

Review intent:

1. `migration*.go` / `framework_tables.go` should represent the canonical framework-table create path.
2. `*_dynamic_table.go` should call shared Schema conversion + builder path instead of per-adapter column-string assembly.

Script-assisted audit:

```bash
EIT_GATE_CHECK_SCHEMA_PATH=1 bash scripts/release_gate.sh quick
```

## Script

Use [scripts/release_gate.sh](../scripts/release_gate.sh).

```bash
# Fast gate for daily checks and PRs
bash scripts/release_gate.sh quick

# Full gate (includes optional DB suites when enabled)
bash scripts/release_gate.sh full
```

## Environment Flags

- `EIT_GATE_ENABLE_LINT`: `auto` (default), `on`, `off`
  - `auto`: run lint only when `golangci-lint` exists
  - `on`: require `golangci-lint`, fail if missing
  - `off`: skip lint
- `EIT_GATE_RUN_OPTIONAL_DB`: `0` (default), `1`
  - `1` enables broader DB integration suites in `full` mode
- `EIT_GATE_CHECK_SCHEMA_PATH`: `0` (default), `1`
  - `1` enables architecture invariant audit for Schema Builder DDL path

Example:

```bash
EIT_GATE_ENABLE_LINT=on EIT_GATE_RUN_OPTIONAL_DB=1 EIT_GATE_CHECK_SCHEMA_PATH=1 bash scripts/release_gate.sh full
```

## v1.0 Candidate Baseline Command

For v1.0 release candidate rehearsal and Go/No-Go checks, use the frozen baseline command below:

```bash
EIT_GATE_ENABLE_LINT=auto EIT_GATE_RUN_OPTIONAL_DB=1 EIT_GATE_CHECK_SCHEMA_PATH=1 bash scripts/release_gate.sh full
```

Record the exact command, environment, and artifact links in the rehearsal note.

## CI Trigger and Failure Policy

Workflow: `.github/workflows/release-gate.yml`

Triggers:

1. `pull_request` to `main`
2. `push` to `main`
3. manual `workflow_dispatch`

Failure strategy:

1. Single required gate job.
2. Any command failure exits the job.
3. No soft-fail for release-blocking checks.
