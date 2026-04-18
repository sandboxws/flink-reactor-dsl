# BUG-025: `ecom-revenue-analytics` aggregates by `category`, but `OrderSchema` has no such field [FIXED]

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

## Where Fixed
`src/cli/templates/ecommerce.ts`. Took Option 1 from the investigation
notes — introduced a new `OrderEnrichedSchema` export in
`schemas/ecommerce.ts` with the full union of `OrderSchema` fields plus
the denormalised product attributes the enrichment pipeline adds
(`productId`, `productName`, `category`, `quantity`, `unitPrice`), and
a watermark on `orderTime`.

`pipelines/ecom-revenue-analytics/index.tsx` was updated to import
`OrderEnrichedSchema` instead of `OrderSchema` and bind it to the
`ecom.order-enriched` source. `OrderSchema` is unchanged — it remains
the contract for raw orders on `ecom.orders` used by `pump-ecom` and
the enrichment pipeline's input side.

## Why Not Option 2 (add `category` to `OrderSchema`)
Would have been a 1-line change but conflates raw-order and
enriched-order shapes. Every raw-order producer (including `pump-ecom`'s
DataGen) would then be obliged to generate a `category` field that
doesn't belong to its contract, and downstream consumers of the raw
topic would get a misleading signal that `category` is available
pre-enrichment. Keeping the two schemas distinct encodes topic
semantics in the type system.

## Verification
```bash
flink-reactor cluster up
REQUIRE_SQL_GATEWAY=1 pnpm vitest run \
  src/cli/templates/__tests__/template-explain.test.ts -t ecom-revenue-analytics
```
Moves `ecommerce/ecom-revenue-analytics` from `↓` → `✓`.
