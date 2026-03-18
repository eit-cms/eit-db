# Release Gate Rehearsal - 2026-03-14

## Context

- Timestamp: 2026-03-14 10:53:34 CST
- Branch context: current working branch in local repository
- Goal: candidate release gate rehearsal for v1.0 M0-4

## Commands

```bash
cd /Users/huyingjie/dev/go/eit-db
EIT_GATE_ENABLE_LINT=off EIT_GATE_RUN_OPTIONAL_DB=1 bash scripts/release_gate.sh full
```

## Result

- Status: PASSED
- Root unit/regression tests: passed
- Key integration tests (SQLite application suite): passed
- Optional DB integration tests (application suite): passed
- Optional DB backend tests: passed
- Lint: intentionally disabled (`EIT_GATE_ENABLE_LINT=off`)

## Issue Found During Rehearsal and Fix

An initial rehearsal attempt exposed a blocker:

- `adapter-application-tests/sqlite_integration_test.go` still assumed `GetRawConn()` returned `*gorm.DB`.

This conflicted with the v1.0 boundary that adapters expose standard driver connections. The test suite was refactored to use `*sql.DB` and SQL statements directly, then re-run successfully.

## Notes

- This rehearsal validates the release gate pipeline and local execution flow.
- For release candidates requiring strict lint gate, run with:

```bash
EIT_GATE_ENABLE_LINT=on EIT_GATE_RUN_OPTIONAL_DB=1 bash scripts/release_gate.sh full
```
