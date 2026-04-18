# BUG-030: TemporalJoin after a windowed Aggregate loses its time-attribute through the CTE alias

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

## Where to Fix (investigation TODOs)

- `src/codegen/sql-generator.ts` — inspect the emitter that wraps
  aggregates as CTEs when they feed a join. Options:
  1. **Drop the rename.** Emit `window_end` as `window_end` through to
     the join site; translate `asOf: "windowEnd"` to
     `asOf: "window_end"` at the codegen boundary so the template stays
     pretty. Cheapest, but removes user-facing control over the alias.
  2. **Inline instead of CTE.** When the aggregate feeds a TemporalJoin
     whose `asOf` references the window column, emit the aggregate
     directly as a subquery rather than a WITH CTE. In some Flink
     versions this preserves the time attribute.
  3. **Explicit WATERMARK.** Rewrite the join-side table as a view with
     a synthetic `WATERMARK FOR windowEnd AS windowEnd` declaration.
     Heavier; touches more code paths.

- `src/codegen/schema-introspect.ts` — when resolving the schema of an
  Aggregate whose children include a Window, track which output column
  is the rowtime-attribute (`window_time`/`window_end`) so downstream
  TemporalJoin emission can reference the un-aliased name.

- Reproducer outside the DSL (useful for narrowing):
  ```sql
  -- Does Flink preserve window_end as a time attribute through this CTE?
  WITH agg AS (
    SELECT k, window_end
    FROM TABLE(TUMBLE(TABLE t, DESCRIPTOR(ts), INTERVAL '5' MINUTE))
    GROUP BY k, window_end
  )
  SELECT agg.*, dim.*
  FROM agg LEFT JOIN dim FOR SYSTEM_TIME AS OF agg.window_end ...;
  ```
  Try both with and without `AS`-aliasing `window_end` to confirm which
  transformation loses the attribute.

## Not a Pure Flink Limitation
The equivalent pattern using a direct subquery (no CTE, no rename)
typically succeeds on the same Flink version. So this is a codegen
issue that can be fixed without changes to Flink.

## Verification
```bash
REQUIRE_SQL_GATEWAY=1 pnpm vitest run \
  src/cli/templates/__tests__/template-explain.test.ts --reporter=verbose
# iot-factory/iot-predictive-maintenance should move from ↓ to ✓
```

Remove the `iot-predictive-maintenance` entry from the `SKIP` set in
`src/cli/templates/__tests__/template-explain.test.ts` and mark this
file `[FIXED]` once the fix lands.
