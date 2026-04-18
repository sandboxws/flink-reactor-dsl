# BUG-025: `ecom-revenue-analytics` aggregates by `category`, but `OrderSchema` has no such field

## Affected Templates/Pipelines
- `ecommerce/ecom-revenue-analytics` — `Aggregate groupBy={['category']}` over a
  `KafkaSource` bound to `OrderSchema`.

Surfaced after BUG-015 was fixed: with `WINDOW_END` no longer breaking
validation first, Flink now reaches the `category` reference and fails
because the column doesn't exist in the upstream table.

## Flink Error
```
SQL validation failed. From line 10, column 12 to line 10, column 21:
Column 'category' not found in any table
```

The failing generated SQL (excerpt):
```sql
INSERT INTO `revenue_by_category`
SELECT `category`, SUM(amount) AS `totalRevenue`, COUNT(*) AS `orderCount`,
       window_start AS `windowStart`, window_end AS `windowEnd`
FROM (
  WITH `_windowed_input` AS (
    SELECT * FROM `ecom_order_enriched_2`
    WHERE 1 = 1
  )
  SELECT * FROM TABLE(
    HOP(TABLE `_windowed_input`, DESCRIPTOR(`orderTime`),
        INTERVAL '1' MINUTE, INTERVAL '5' MINUTE)
  )
)
GROUP BY `category`, window_start, window_end
```

## Root Cause
This is a **template authoring bug**, not a codegen bug. In
`src/cli/templates/ecommerce.ts`:

- The `OrderSchema` (lines ~109-119) declares fields:
  `orderId, customerId, amount, currency, status, orderTime`. No `category`.
- The `category` field exists on `ProductSchema` (line ~136).
- `pipelines/ecom-revenue-analytics/index.tsx` consumes the
  `ecom.order-enriched` Kafka topic but binds it to `OrderSchema`. The
  topic name implies an *enriched* (joined) view that should include
  product attributes like `category`.

The codegen faithfully emits SQL based on the declared schema, so Flink
correctly rejects the `GROUP BY category` reference.

## Where to Fix
`src/cli/templates/ecommerce.ts`. Two reasonable options:

1. **Introduce `OrderEnrichedSchema`** in `schemas/ecommerce.ts` —
   includes `OrderSchema` fields plus `category` (and likely `productId`,
   `productName`, etc., to model the enrichment realistically). Update
   `pipelines/ecom-revenue-analytics/index.tsx` to import and use it.
   Preferred — better models the topic semantics.

2. **Add `category` to `OrderSchema`** — minimal change, but conflates
   raw-order and enriched-order shapes. Will cause confusion for
   `ecom-order-enrichment` (which performs the actual enrichment from
   `OrderSchema` + `ProductSchema`). Not recommended.

## Verification
```bash
flink-reactor cluster up
REQUIRE_SQL_GATEWAY=1 pnpm vitest run src/cli/templates/__tests__/template-explain.test.ts
```
`ecom-revenue-analytics` should move out of the `SKIP` set and pass
EXPLAIN.
