# BUG-028: Temporal join condition omits part of the temporal table's primary key [FIXED]

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

## Where Fixed

Took Option 2 from the investigation notes. Changes span both the
template and the codegen:

### Template — `src/cli/templates/grocery-delivery.ts`

1. **New `OrderLineSchema`** in `schemas/grocery.ts` with
   `(orderId, storeId, productId, quantity, lineTime)` and a watermark
   on `lineTime`. Models one record per product-line within an order.
2. **New sim topic `grocery.order-lines`** registered alongside the
   existing `grocery.orders` entry.
3. **`grocery-order-fulfillment` pipeline rewritten.** Drops the
   `orders` KafkaSource in favour of an `orderLines` KafkaSource, sets
   explicit `name:` props (`"orderLines"`, `"inventory"`) so the
   TemporalJoin's compound ON can reference them directly, and joins
   on the full composite PK:

   ```ts
   on: "orderLines.storeId = inventory.storeId AND orderLines.productId = inventory.productId",
   asOf: "lineTime",
   ```

### Codegen — `src/codegen/sql-generator.ts`

The compound ON surfaced a real codegen gap: `sameNameJoinKey` was
scalar-only, so compound same-name joins fell through the "simple"
branch and emitted `SELECT *` — which Flink expanded into N+2 columns
(right-side duplicates auto-renamed `storeId0`, `productId0`), while
the auto-inferred sink DDL correctly deduplicates to N. Result:
column-count mismatch on the sink.

The fix generalises the helpers:

- `sameNameJoinKey` → `sameNameJoinKeys` returns `string[] | null`.
  Splits the ON clause on ` AND ` and accepts a chain only when every
  sub-clause is a same-name equality (unqualified or qualified).
- `buildJoinProjectionSkippingRightCol` →
  `buildJoinProjectionSkippingRightCols` takes `readonly string[]`.
- All five call sites (regular Join, TemporalJoin, LookupJoin,
  IntervalJoin, and the sink-DDL inference that derives the join
  result's primary key) now emit `a.c1 = b.c1 AND a.c2 = b.c2 …` for
  the qualified-ON and skip all shared columns from the right side's
  projection. Scalar output is byte-identical to the old path.

## Verification
```bash
REQUIRE_SQL_GATEWAY=1 pnpm vitest run \
  src/cli/templates/__tests__/template-explain.test.ts -t grocery-order-fulfillment
```
Moves `grocery-delivery/grocery-order-fulfillment` from `↓` → `✓`.
Full suite: +1 pass, -1 skip (1107 → 1108; 8 → 7), no snapshot
regressions.
