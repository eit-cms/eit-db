# Cross-Table Query Redesign

## Goals

1. Make cross-table query behavior explicit and dialect-aware.
2. Keep defaults safe while allowing deterministic overrides.
3. Align PostgreSQL alias semantics and SQL Server temp-table strategy in one unified API.

## What Is Already Implemented

1. QueryConstructor now includes JOIN APIs and cross-table strategy API.
2. PostgreSQL and SQL Server default JOIN alias output uses AS when alias is present.
3. SQL Server supports strategy marker prefer_temp_table in generated SQL output.

## New API Surface

- FromAlias(alias)
- Join(table, onClause, alias...)
- LeftJoin(table, onClause, alias...)
- RightJoin(table, onClause, alias...)
- CrossJoin(table, alias...)
- CrossTableStrategy(strategy)

### CrossTableStrategy

- auto: use dialect default behavior.
- prefer_temp_table: prefer temp-table pipeline for complex cross-table query.
- force_direct_join: force direct JOIN SQL generation.

## Dialect Defaults

### PostgreSQL

- Default alias style in cross-table SQL: AS.
- Auto alias generation when alias is omitted.
- Keep direct JOIN as default execution shape.

### SQL Server

- Default alias style in cross-table SQL: AS.
- Auto alias generation when alias is omitted.
- Strategy auto defaults to direct JOIN currently.
- Strategy prefer_temp_table is now modeled and observable (marker), enabling safe incremental rollout.

## Next Implementation Steps

1. Add cost-based trigger for SQL Server prefer_temp_table in auto mode.
2. Implement real temp-table rewrite pipeline:
   - phase 1: source subset materialization
   - phase 2: indexed temp table join
   - phase 3: final projection and cleanup
3. Add capability guardrails via QueryFeatures and runtime checks.
4. Add E2E benchmarks comparing direct JOIN vs temp-table path.

## Safety Rules

1. Never rewrite to temp-table for single small-table join.
2. Keep deterministic SQL output under force_direct_join.
3. Keep strategy behavior visible in generated SQL for observability and debugging.
