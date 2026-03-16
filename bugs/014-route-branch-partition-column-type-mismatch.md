# BUG-014: Route branch FileSystemSink partition columns cause type mismatch

## Affected Examples
- `24-lambda-architecture` — FileSystemSink with `partitionBy={["DATE(event_time)", "HOUR(event_time)"]}`

## Flink Error
```
Column types of query result and sink for 'default_catalog.default_database.raw' do not match.
```

## Root Cause
The FileSystemSink in a Route.Branch has computed partition columns (`dt DATE`, `hr INT`) added to the DDL, and the DML is wrapped with partition projections (`SELECT *, CAST(...) AS dt, HOUR(...) AS hr`). However, the sink schema resolved by `resolveSinkMetadata` (used for DDL generation) doesn't include the computed partition columns — it only has the 8 source columns. The DDL then has 10 columns (8 + 2 partition) but the schema resolution returns 8.

The partition column addition happens in `generateSinkDdl` (physical columns appended to DDL), and the DML wrapping happens in `collectRouteDml`. But the schema used for the DDL column definitions comes from `resolveSinkMetadata` which doesn't know about partition columns. This creates a mismatch between:
- DDL: 8 source columns + 2 partition columns = 10
- INSERT: `SELECT *, dt, hr FROM (source query)` = 10 columns

The actual mismatch may be in column ordering or type inference for the non-partition columns within the Route branch context.

## Where to Fix
- `src/codegen/sql-generator.ts` — verify that Route branch schema resolution and partition column addition are consistent
- The partition columns should be excluded from the schema-based column definitions (since they're added separately by the partition logic), or the schema should include them

## Verification
```bash
pnpm test:explain  # example 24 should pass after fix
```
