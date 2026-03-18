# Release Gate Rehearsal - 2026-03-14 (v1.0 Baseline)

## Context

- Timestamp: 2026-03-14
- Goal: execute frozen v1.0 baseline gate command after API stability checklist freeze.

## Frozen Baseline Command

```bash
cd /Users/huyingjie/dev/go/eit-db
EIT_GATE_ENABLE_LINT=auto EIT_GATE_RUN_OPTIONAL_DB=1 EIT_GATE_CHECK_SCHEMA_PATH=1 bash scripts/release_gate.sh full
```

## Result

- Status: PASSED
- Root unit/regression tests: passed
- Key integration tests (SQLite application suite): passed
- Architecture invariant audit (Schema Builder path): passed
- Optional DB integration tests (application suite): passed
- Optional DB backend tests: passed
- Lint stage: skipped in auto mode because `golangci-lint` was not found

## Notes

1. This rehearsal uses the exact baseline command documented in release gate docs.
2. Current gate is healthy under `auto` lint policy.
3. If release requires strict lint enforcement, rerun with `EIT_GATE_ENABLE_LINT=on` in an environment with `golangci-lint` installed.

## Artifacts

- API stability checklist: `docs/API_STABILITY_v1_0.md`
- Readiness checklist: `docs/V1_0_READINESS_CHECKLIST.md`
- Release gate spec: `docs/RELEASE_GATE.md`
