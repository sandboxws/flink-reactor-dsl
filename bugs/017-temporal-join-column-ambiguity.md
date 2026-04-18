# BUG-017: TemporalJoin emits ambiguous ON clause and duplicate-column SELECT [FIXED]

## Affected Templates/Pipelines (originally identified)
- `banking/bank-fraud-detection`
- `grocery-delivery/grocery-order-fulfillment`

Both written with the natural `on: "col = col"` pattern (same column name on
both sides). The codegen passed it through verbatim, producing SQL Flink
rejected as ambiguous; even after qualification, `SELECT *` produced a
duplicate join column that broke downstream sinks.

## Flink Errors (two layers, fixed in order)

### Layer 1 — Ambiguous bare-column reference in ON
```
SQL validation failed. From line 3, column 122 to line 3, column 128:
Column 'storeId' is ambiguous
```
Generated SQL fragment:
```sql
LEFT JOIN `grocery_store_inventory`
  FOR SYSTEM_TIME AS OF `grocery_orders`.`orderTime`
  ON storeId = storeId
```

### Layer 2 — Duplicate join column in SELECT *
After qualifying ON, Flink reported a schema mismatch:
```
Cause: Different number of columns.
Query schema: [orderId, storeId, customerId, ..., storeId0, productId, ...]
Sink  schema: [orderId, storeId, customerId, ..., productId, ...]
```
`SELECT *` from the join produced both sides' `storeId` columns, the second
auto-renamed `storeId0`, breaking column count alignment with the sink DDL.

## Where Fixed — `src/codegen/sql-generator.ts`

`buildTemporalJoinQuery` now:
1. Detects the simple `<col> = <col>` pattern in the user's `on` clause
   (`sameNameJoinKey` helper) and qualifies it with the resolved side-refs:
   `<left_ref>.<col> = <right_ref>.<col>`. Compound or already-qualified
   conditions pass through unchanged.
2. When a same-name key was detected, replaces `SELECT *` with
   `SELECT <left>.*, <right>.col1, <right>.col2, ...`, omitting the
   right-side join key (the duplicate). The right's columns are read
   from its `Source` node's `props.schema` — when introspection isn't
   possible (right side is a transform without a directly-readable schema),
   the fallback is `SELECT *` and the user is on their own.

## Companion Template Fixes

`src/cli/templates/banking.ts` and `src/cli/templates/grocery-delivery.ts`:
added `watermark: { column: 'updateTime', expression: ... }` to
`AccountSchema` and `StoreInventorySchema`. Flink event-time temporal joins
require both a primary key (already declared) *and* a rowtime attribute on
the temporal table. Without a watermark, the planner errors with:
```
Event-Time Temporal Table Join requires both primary key and row time
attribute in versioned table, but no row time attribute can be found.
```

## Pipelines Still Blocked (separate bugs)

- `bank-fraud-detection` — sink schema mismatch on `firstTxn`/`lastTxn`:
  query produces `TIMESTAMP(3)` (from `MatchRecognize` `MEASURES FIRST(txnTime)`),
  but the auto-derived sink declares `STRING`. Tracked as
  [BUG-027](./027-match-recognize-measures-type-inference.md).
- `grocery-order-fulfillment` — temporal join condition uses only
  `storeId`, but `StoreInventorySchema` PK is `[storeId, productId]`. Flink
  requires *all* PK columns in the join. The orders schema doesn't carry
  `productId`, so this is a template-redesign issue (not codegen). Tracked
  as [BUG-028](./028-temporal-join-partial-pk-coverage.md).

## Verification
```bash
flink-reactor cluster up
REQUIRE_SQL_GATEWAY=1 pnpm vitest run src/cli/templates/__tests__/template-explain.test.ts
```
- The codegen fix is reachable: both pipelines now move past the ON-clause
  ambiguity error to the next layer of validation (BUG-027 / BUG-028).
- Existing `16-temporal-join` example (which used a fully-qualified `on`
  clause) continues to pass unchanged — the regex-driven qualification
  is opt-in by syntax shape.

## Snapshot Impact
None. The example pipelines use already-qualified ON clauses
(`\`forex_orders\`.currency_pair = \`currency_rates\`.currency_pair`), so
my detection regex doesn't match and emission is identical.
