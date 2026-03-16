# BUG-002: Joins generate `SELECT * FROM unknown` instead of actual join SQL

## Affected Examples
- `06-interval-join` — IntervalJoin
- `08-multi-stream-join` — multiple joins chained
- `15-cep-fraud-detection` — CEP pattern (likely uses internal join logic)
- `16-temporal-join` — TemporalJoin
- `18-broadcast-join` — BroadcastJoin
- `21-branching-multi-sink` — contains a join in one branch
- `25-batch-reporting` — contains a join

## Flink Error
```
Encountered "unknown" at line 3, column 15.
```
(Flink parser hits the literal identifier `unknown` where a table name is expected)

## Root Cause
The SQL generator's join handling emits `SELECT * FROM unknown` as a placeholder when it cannot resolve the join's input streams. This affects IntervalJoin, TemporalJoin, BroadcastJoin, and multi-stream joins.

The issue is in `src/codegen/sql-generator.ts` — when generating the DML for a join node, the code references the joined subquery as `unknown` instead of generating the actual `JOIN ... ON ...` clause.

## Generated SQL (06-interval-join)
```sql
INSERT INTO `order_fulfillment`
SELECT order_id AS `order_id`, user_id AS `user_id`, amount AS `amount`,
       carrier AS `carrier`, ship_time - order_time AS `fulfillment_time` FROM (
SELECT * FROM unknown   -- ← should be the actual interval join query
);
```

### Expected SQL
```sql
INSERT INTO `order_fulfillment`
SELECT o.order_id, o.user_id, o.amount, s.carrier,
       s.ship_time - o.order_time AS fulfillment_time
FROM orders o
JOIN shipments s ON o.order_id = s.order_id
  AND s.ship_time BETWEEN o.order_time AND o.order_time + INTERVAL '1' HOUR;
```

## Where to Fix
- `src/codegen/sql-generator.ts` — the DML generation path for join nodes
- Look for where `unknown` is being emitted as a table reference — this is the fallback when the join source resolution fails
- Each join type (IntervalJoin, TemporalJoin, BroadcastJoin) needs its own SQL generation logic that produces the correct JOIN ... ON syntax

## Verification
```bash
pnpm test:explain  # examples 06, 08, 15, 16, 18, 21, 25 should pass after fix
```
