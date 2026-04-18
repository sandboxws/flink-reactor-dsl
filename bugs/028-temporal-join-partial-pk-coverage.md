# BUG-028: Temporal join condition omits part of the temporal table's primary key

## Affected Templates/Pipelines
- `grocery-delivery/grocery-order-fulfillment`

Surfaced after BUG-017 cleared the temporal-join ambiguity for this
pipeline. Flink now rejects the join semantics:

## Flink Error
```
Temporal table's primary key [storeId0,productId] must be included in the
equivalence condition of temporal join, but current temporal join
condition is [storeId=storeId].
```

## Root Cause
This is a **template authoring bug**, not a codegen bug.

`StoreInventorySchema` declares `primaryKey: ['storeId', 'productId']` —
"inventory tracked per-store-per-product". The pipeline joins inventory
by `storeId` only (`on: "storeId = storeId"`). Flink's temporal join
contract requires *all* PK columns in the equivalence condition; without
`productId`, the join is ambiguous (one order × N products in inventory).

The orders side (`GroceryOrderSchema`) has no `productId` column — it
tracks order totals (`itemCount`, `totalAmount`), not per-line-item
detail. So the template has a structural gap: there's no order-line
table to bridge the join.

## Where to Fix
`src/cli/templates/grocery-delivery.ts`. Two reasonable options:

1. **Reduce inventory PK to `[storeId]`** — semantically wrong (loses
   per-product granularity) but minimal change. Bad option.

2. **Introduce `OrderLineSchema`** with fields `(orderId, storeId,
   productId, quantity, lineTime)` and a Kafka topic
   `grocery.order-lines`. Restructure `grocery-order-fulfillment` to
   join order-lines × inventory on `(storeId, productId)`. Preferred.

   ```ts
   const orderLines = KafkaSource({ topic: "grocery.order-lines", schema: OrderLineSchema, ... });
   const enriched = TemporalJoin({
     stream: orderLines,
     temporal: inventory,
     on: "storeId = storeId AND productId = productId",  // composite
     asOf: "lineTime",
   });
   ```

   Note: this also exercises a *compound* `on` clause, which the BUG-017
   fix's regex doesn't auto-qualify — the template would need to write
   the qualifications explicitly:
   `on: "\`grocery_order_lines\`.storeId = \`grocery_store_inventory\`.storeId AND \`grocery_order_lines\`.productId = \`grocery_store_inventory\`.productId"`.
   This may motivate a future codegen extension to handle compound ON
   auto-qualification.

## Verification
```bash
REQUIRE_SQL_GATEWAY=1 pnpm vitest run src/cli/templates/__tests__/template-explain.test.ts
```
`grocery-order-fulfillment` should pass EXPLAIN.
