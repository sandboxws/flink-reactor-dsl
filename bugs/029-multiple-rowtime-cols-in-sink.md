# BUG-029: Kafka sink rejects two rowtime attribute columns from upstream join [FIXED]

## Affected Examples
- `ecom-order-enrichment` (ecommerce template) — surfaced after BUG-019 fix

## Flink Error
```
The query contains more than one rowtime attribute column
[orderTime, itemTime] for writing into table
'default_catalog.default_database.ecom_order_enriched'.
```

## Root Cause
After BUG-019 made the interval-join SQL syntactically valid, the
TemporalJoin at the top of `ecom-order-enrichment` produces a row that
carries *two* event-time attributes — `orderTime` (from OrderSchema,
watermark on `orderTime`) and `itemTime` (from OrderItemSchema,
watermark on `itemTime`). The downstream `KafkaSink` infers its
schema from the upstream, so the sink DDL ends up with both as
`TIMESTAMP(3)` rowtime columns.

Flink allows at most one rowtime attribute per sink table; see
`ValidationException: The query contains more than one rowtime
attribute column`.

## Where Fixed

`src/codegen/sql-generator.ts`. Wrapped at the **sink boundary**, not
at the IntervalJoin level.

### Why not at IntervalJoin

First attempt was to `CAST` the right side's rowtime column inside the
IntervalJoin's projection. That fixed `ecom-order-enrichment` but broke
`rides-trip-tracking`: that pipeline feeds the IntervalJoin output into
a `MATCH_RECOGNIZE` with `orderBy: "eventTime"`, which requires
`eventTime` to retain its rowtime-attribute marker. Demoting the
attribute at the join level broke downstream time-dependent operators.

The rule is: rowtime attributes must survive through intermediate
operators (MATCH_RECOGNIZE, windows, temporal joins may legitimately
depend on any of them) and only be demoted where Flink's sink contract
actually requires it — at the `INSERT` projection.

### The sink-boundary fix

Added two things to `generateDml` / `collectSinkDml` /
`collectRouteDml`:

1. **Source watermark enumeration.** `generateDml` walks the pipeline's
   `nodeIndex` once, collecting every `watermark.column` declared on a
   `Source` node. Passed down to the sink emitters as
   `rowtimeCols: ReadonlySet<string>`.

2. **`wrapSinkQueryForMultiRowtime` helper.** Before each INSERT is
   finalised, checks whether the sink's resolved schema contains two
   or more columns that are watermark attributes on any upstream
   source. If so, wraps the query with:

   ```sql
   SELECT col1, col2, ..., CAST(rt2 AS <type>) AS rt2, ... FROM (<upstream>)
   ```

   Keeps the first rowtime column in schema order, casts the rest to
   plain TIMESTAMP — demoting the attribute only at the sink write
   boundary. Zero effect on pipelines with ≤1 rowtime column.

Applied both in `collectSinkDml` (direct sink path) and
`collectRouteDml` (route-branch sinks). Scalar output unchanged for
pipelines with a single rowtime — no snapshot regressions outside the
one IntervalJoin fixture test that *intentionally* exercises two
rowtime columns (snapshot rebaselined to include the new wrap).

## Verification
```bash
flink-reactor cluster up
REQUIRE_SQL_GATEWAY=1 pnpm vitest run \
  src/cli/templates/__tests__/template-explain.test.ts -t ecom-order-enrichment
```
Moves `ecommerce/ecom-order-enrichment` from `↓` → `✓`. Full suite:
+1 pass, -1 skip (1108 → 1109; 7 → 6). `rides-trip-tracking` still
passes — proof that the fix correctly preserves rowtime through
intermediate operators.

## Related
- BUG-019 — interval-join table refs fixed, which uncovered this
  follow-up.
- BUG-024 — `rides-trip-tracking` fix that exposed why the IntervalJoin-
  level approach fails (MATCH_RECOGNIZE needs the rowtime downstream).
