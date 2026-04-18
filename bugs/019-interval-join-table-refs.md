# BUG-019: IntervalJoin template references undeclared table aliases [FIXED]

## Affected Examples
- `ecom-order-enrichment` (ecommerce template)

## Flink Error
```
SQL parse failed. Encountered "." at line N — unexpected token.
```
or a variant like `Column 'orders.orderId' not found in any table`,
depending on which of the two template bugs Flink trips over first.

## Generated SQL (before fix)
```sql
WITH `IntervalJoin_9` AS (
  SELECT * FROM `ecom_orders_2` JOIN `ecom_order_items`
    ON orders.orderId = items.orderId
    AND `ecom_order_items`.`itemTime`
        BETWEEN `ecom_orders_2`.orders.orderTime
            AND `ecom_orders_2`.orders.orderTime + INTERVAL '30' SECOND
)
...
```

Two problems visible:
- `orders.` / `items.` in the ON clause reference SQL table aliases
  that were never declared — the tables are registered as
  `ecom_orders_2` / `ecom_order_items`.
- `ecom_orders_2.orders.orderTime` is a three-part reference — the
  codegen prepends `${left.ref}.` to `interval.from`, which is already
  qualified in the template (`"orders.orderTime"`).

## Root Cause
This is a **template-authoring bug**, not a codegen bug. The established
convention — see `06-interval-join` in the example snapshots — is:

1. The `on` clause references operands by their **registered table id**
   (the KafkaSource's `name` prop, or the topic-derived id when `name`
   is omitted).
2. `interval.from` / `interval.to` are **unqualified** column names or
   expressions; the emitter prepends the left table's ref itself.

The `ecom-order-enrichment` template writes `orders.orderId` / `items.orderId`
in the ON clause, assuming the JS variable names will become SQL aliases.
They don't. And it writes `orders.orderTime` in the interval bounds,
which the emitter then double-qualifies to `ecom_orders_2.orders.orderTime`.

The topic `ecom.orders` is also used as a sink in `pump-ecom` — the
shared id counter across pipelines in the same synth run means the
source here resolves to `ecom_orders_2` (second user of `ecom_orders`),
compounding the implicit assumption.

## Where Fixed

**Template** — `src/cli/templates/ecommerce.ts`, `ecom-order-enrichment`:
- Set explicit `name` on each `KafkaSource` (`"orders"`, `"items"`,
  `"products"`) so table ids match the ON-clause aliases and are stable
  across scaffolded pipelines.
- Dropped the `orders.` prefix from `interval.from` / `interval.to` so
  the emitter qualifies with the left ref without collision.
- Added a `watermark` to `ProductSchema` on `updateTime` so it's a
  valid versioned table for the downstream event-time TemporalJoin.

**Codegen** — `src/codegen/sql-generator.ts`:
- Extended `sameNameJoinKey` to recognise the qualified form
  `X.col = Y.col` (in addition to the existing `col = col` shorthand).
- Applied the BUG-017 projection-pruning pattern to
  `buildIntervalJoinQuery`, so a shared join key no longer produces a
  duplicate column that breaks the downstream sink. Mirrors what
  TemporalJoin already does.

## Follow-up

Fix is shipped but `ecom-order-enrichment` is not yet unblocked —
the above changes made the SQL syntactically valid, which in turn
surfaced BUG-029 (two rowtime attributes in the sink). The pipeline
remains in the `SKIP` set pointing at BUG-029 until that's resolved.

## Verification
```bash
REQUIRE_SQL_GATEWAY=1 pnpm vitest run \
  src/cli/templates/__tests__/template-explain.test.ts -t ecom-order-enrichment
```
Error transitions from `SQL parse failed` / `Column not found` to the
rowtime-attribute validation captured by BUG-029.
