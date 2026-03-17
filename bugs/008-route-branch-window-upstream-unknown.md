# BUG-008: Window inside Route branch generates SELECT * FROM unknown [FIXED]

## Affected Examples
- `21-branching-multi-sink` — Route.Default branch with TumbleWindow
- `28-branching-iot` — Route.Default branch with TumbleWindow (same pattern, manifests as type mismatch)

## Flink Error
```
# Example 21:
Encountered "unknown" at line 4, column 15.

# Example 28:
Column types of query result and sink for 'sensor_metrics_5min' do not match.
```

## Root Cause
When a `Route.Branch` or `Route.Default` contains a `TumbleWindow` with an `Aggregate` child, the `buildBranchQuery` function doesn't inject the Route's upstream into the Window node. The Window has children (the Aggregate), so `buildBranchQuery` calls `buildQuery(Window)` directly. But `buildQuery(Window)` → `buildWindowQuery()` → `getUpstream()` finds no source children → returns `SELECT * FROM unknown`.

In `buildSiblingChainQuery`, this is handled by injecting a `VirtualRef` node as a child alongside the Aggregate. But `buildBranchQuery` doesn't have this injection logic for Window nodes.

For example 28, the `SELECT * FROM unknown` is inside a CTE (from the BUG-003 fix), so Flink doesn't report "unknown" — instead it reports a column type mismatch because the query produces raw source columns instead of aggregated columns.

## Generated SQL (example 21)
```sql
INSERT INTO `regional_metrics_per_minute`
WITH `_windowed_input` AS (
SELECT order_id AS `order_id`, ... FROM (
SELECT * FROM unknown   -- ← Route upstream not injected into Window
)
)
SELECT * FROM TABLE(
  TUMBLE(TABLE `_windowed_input`, DESCRIPTOR(`order_time`), INTERVAL '1' MINUTE)
);
```

## Where to Fix
- `src/codegen/sql-generator.ts` — `buildBranchQuery()` or the branch chain logic in `collectRouteDml()`
- When a branch transform is a Window with Aggregate children, inject the upstream as a VirtualRef child (same pattern as `buildSiblingChainQuery` lines ~1618-1631)

## Verification
```bash
pnpm test:explain  # examples 21, 28 should pass after fix
```
