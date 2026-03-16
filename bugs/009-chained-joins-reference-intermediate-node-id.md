# BUG-009: Chained joins reference intermediate node ID as table name

## Affected Examples
- `08-multi-stream-join`

## Flink Error
```
Object 'IntervalJoin_3' not found
```

## Root Cause
When two joins are chained (the output of the first join feeds into the second), the second join's `left` prop references the first join's node ID (`IntervalJoin_3`). The `resolveRef()` function in `src/codegen/sql-generator.ts` simply quotes this node ID as a table name (`` `IntervalJoin_3` ``), but no DDL table with that name exists — the first join is an intermediate query result.

The `buildIntervalJoinQuery` function calls `resolveRef(leftId, nodeIndex)` which returns `` `IntervalJoin_3` `` without inlining the first join's SQL as a subquery.

## Generated SQL
```sql
SELECT * FROM `IntervalJoin_3`   -- ← not a real table
  LEFT JOIN `conversions` ON ...
```

### Expected SQL
The first join should be inlined as a subquery:
```sql
SELECT * FROM (
  SELECT * FROM `page_views` LEFT JOIN `clicks` ON ...
) LEFT JOIN `conversions` ON ...
```

Or use a CTE:
```sql
WITH `views_clicks` AS (
  SELECT * FROM `page_views` LEFT JOIN `clicks` ON ...
)
SELECT * FROM `views_clicks` LEFT JOIN `conversions` ON ...
```

## Where to Fix
- `src/codegen/sql-generator.ts` — `resolveRef()` (line ~3549) or the join query builders
- When `resolveRef` encounters a node that is not a Source (i.e., a Join or Transform), it should inline that node's SQL as a subquery or emit a CTE
- Alternative: handle this in `buildSiblingChainQuery` by ensuring chained joins are processed through the VirtualRef injection path

## Verification
```bash
pnpm test:explain  # example 08 should pass after fix
```
