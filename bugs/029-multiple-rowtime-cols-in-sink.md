# BUG-029: Kafka sink rejects two rowtime attribute columns from upstream join

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

## Where to Fix
Two possible approaches — pick based on preference:

**(a) Sink-side DDL projection.** When inferring a KafkaSink schema
from upstream, demote all-but-one rowtime attribute to plain
`TIMESTAMP(3)` (strip the watermark metadata). The sink doesn't need
rowtime attributes at write time. Likely lives in `generateSinkDdl` /
`resolveSinkMetadata` in `src/codegen/sql-generator.ts`.

**(b) Template-level projection.** Add a `Map` step before the sink
that aliases `itemTime` to a plain-timestamp expression (e.g.,
`CAST(itemTime AS TIMESTAMP(3))`). Surgical but doesn't generalise.

Option (a) is the structural fix; (b) is a workaround.

## Verification
```bash
flink-reactor cluster up
REQUIRE_SQL_GATEWAY=1 pnpm vitest run \
  src/cli/templates/__tests__/template-explain.test.ts -t ecom-order-enrichment
```
Expect `ecom-order-enrichment` to pass EXPLAIN once this is fixed;
remove it from `SKIP` then.

## Related
- BUG-019 — interval-join table refs fixed, which uncovered this
  follow-up.
