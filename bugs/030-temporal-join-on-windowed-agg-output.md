# BUG-030: TemporalJoin after a windowed Aggregate loses its time-attribute through the CTE alias [FIXED]

## Affected Templates/Pipelines
- `iot-factory/iot-predictive-maintenance` — SlideWindow → Aggregate
  (with `select: { windowEnd: 'window_end' }`) → TemporalJoin
  (`asOf: "windowEnd"`).

Surfaced after BUG-023 cleared the STDDEV_POP / reserved-word failures
on this same pipeline.

## Flink Error
```
Temporal table join currently only supports 'FOR SYSTEM_TIME AS OF'
left table's time attribute field
```

Observed against the following generated SQL (abbreviated):

```sql
WITH `Aggregate_99` AS (
  SELECT ..., window_end AS `windowEnd`
  FROM (SELECT * FROM TABLE(HOP(...)))
  GROUP BY ..., window_end
)
SELECT `Aggregate_99`.*, `iot_device_registry`.*
FROM `Aggregate_99`
LEFT JOIN `iot_device_registry`
  FOR SYSTEM_TIME AS OF `Aggregate_99`.`windowEnd`
  ON `Aggregate_99`.`deviceId` = `iot_device_registry`.`deviceId`
```

## Root Cause (hypothesis)

Flink's `FOR SYSTEM_TIME AS OF <col>` requires `<col>` to carry a
rowtime-attribute marker. In a windowed GROUP-BY aggregation, the output
columns `window_start` / `window_end` / `window_time` *are* time
attributes — but only on the *immediate* output of the aggregate.

Two things in the emitted SQL interfere:

1. **CTE wrapping.** The aggregate sits inside a `WITH Aggregate_99 AS
   (...)` CTE whose outer query joins against a temporal table. Across
   some Calcite/Flink versions, time-attribute metadata does not
   propagate through CTE boundaries unless explicitly preserved.

2. **Column alias.** `window_end AS windowEnd` renames the column. Even
   when the CTE boundary is benign, the rename can strip the
   time-attribute flag in older planner rules.

The net effect: `Aggregate_99.windowEnd` is a plain `TIMESTAMP(3)`, not
a time attribute, and `LogicalCorrelateToJoinFromTemporalTableRule`
rejects it.

## Investigation Summary

Tested several codegen workarounds against Flink 2.0.1, in this order:

1. **Inline subquery instead of CTE** (`WITH agg AS (...)` → `FROM
   (...) AS agg`). Didn't help — the time attribute is stripped at
   the subquery alias boundary too.
2. **Drop the rename** (`window_end AS windowEnd` → bare `window_end`
   in projection + template uses `asOf: "window_end"`). Didn't help —
   even with an unaliased projection, Flink 2.0 treats
   `alias.window_end` outside the immediate aggregate output as a
   plain `TIMESTAMP(3)`.
3. **Use `window_time`** (the canonical rowtime attribute column,
   `window_end - 1 ms`) instead of `window_end`. This did work — the
   temporal join validated, the versioned-table then required a
   watermark on `DeviceRegistrySchema`, and after adding that the
   entire pipeline passed EXPLAIN.

**But:** enabling `window_time` unconditionally in the windowed-agg
projection broke a dozen other pipelines (example tests, explain
fixtures) that already work with just `window_start, window_end` —
Flink complains about `window_time`-in-projection-not-in-group-by or
mismatched sink DDLs. Making the addition conditional on downstream
presence of `TemporalJoin { asOf: "window_time" }` adds real
complexity for one pipeline's benefit.

## Where Fixed

Took the structurally cleaner path: **pivot the template from
TemporalJoin → LookupJoin** in
`src/cli/templates/iot-factory.ts::iot-predictive-maintenance`.

- `DeviceRegistrySchema` is now fronted by a `JdbcSource` with an
  `lookupCache` rather than a Kafka CDC stream. A slow-changing
  dimension like `device_registry` is better modelled as a lookup
  table anyway; event-time temporal joins are the right tool for
  versioned streams (inventory, prices), not for registries.
- `LookupJoin` runs on synthetic proctime and doesn't require a
  rowtime attribute on either side, sidestepping the Flink 2.0
  subquery-boundary issue entirely.
- Template note added explaining the codegen / Flink-version reason
  for preferring LookupJoin here.

The codegen gained two small, reusable improvements along the way,
both kept (tested against the full EXPLAIN suite; no regressions):

- `buildAggregateQuery` now detects forward-nesting
  (`Aggregate { children: Window }`) and delegates to `buildWindowQuery`
  with a pseudo-Window carrying the Aggregate as a child. Emits the
  canonical `SELECT … FROM TABLE(TVF(…)) GROUP BY …` pattern in one
  statement instead of the old `SELECT … FROM (SELECT * FROM TABLE(…))
  GROUP BY …`. Shorter SQL, fewer subquery boundaries.
- `resolveNodeSchema`'s Aggregate case now appends `window_start` and
  `window_end` to the output schema when the child is a Window, so
  auto-inferred sink DDLs no longer undercount columns after the
  unified TVF projection above.

## Verification
```bash
REQUIRE_SQL_GATEWAY=1 pnpm vitest run \
  src/cli/templates/__tests__/template-explain.test.ts
```
All 21 template pipelines pass EXPLAIN — `SKIP` set is empty.

```bash
pnpm vitest run
```
1110 passed, 5 skipped, only the pre-existing `completion.test.ts`
snapshot still fails (unrelated to this work).
