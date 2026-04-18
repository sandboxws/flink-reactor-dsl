# BUG-020: LookupJoin references an undeclared dimension table [FIXED]

## Affected Examples
- `ecom-customer-360` (ecommerce template)

## Flink Error
```
Object 'customers' not found
```
(or variant: `Column 'ecom_orders.proc_time' not found`, depending on
which check Flink performs first)

## Generated SQL (before fix)
```sql
CREATE TABLE `ecom_orders` (...) WITH ('connector' = 'kafka', ...);
CREATE TABLE `customer_sessions` (...) WITH ('connector' = 'jdbc', ...);
-- No CREATE TABLE for `customers`

INSERT INTO `customer_sessions`
SELECT ... FROM (
  WITH `_windowed_input` AS (
    SELECT * FROM `ecom_orders`
      LEFT JOIN `customers`
      FOR SYSTEM_TIME AS OF `ecom_orders`.proc_time
      ON customerId = customerId
  )
  SELECT * FROM TABLE(SESSION(TABLE `_windowed_input`, ...))
) GROUP BY ...;
```

Three gaps:
- No `CREATE TABLE customers` DDL — Flink has no idea what columns or
  connector the dimension table has.
- `ecom_orders.proc_time` is referenced but the source DDL has no
  `proc_time AS PROCTIME()` computed column.
- `ON customerId = customerId` is ambiguous — both `ecom_orders` and
  `customers` carry a `customerId`.

## Root Cause
The `LookupJoin` component takes `table: string` + `url: string` but no
schema. At codegen time the join emitter has no way to emit a valid
`CREATE TABLE` for the dimension table, so the `LEFT JOIN customers`
references something Flink has never seen.

Processing-time is also implicit: the emitter writes
`FOR SYSTEM_TIME AS OF <input>.proc_time`, assuming the input source has
a `proc_time` column — but no source declares one.

And unlike TemporalJoin (BUG-017), LookupJoin doesn't invoke
`sameNameJoinKey` for ON-clause disambiguation.

## Where Fixed

**Template** — `src/cli/templates/ecommerce.ts` (`ecom-customer-360`):
- Declared the `customers` dimension as a `JdbcSource` with
  `CustomerSchema` + `lookupCache`, and rendered it in the JSX so the
  codegen emits a `CREATE TABLE customers` DDL. The `table: "customers"`
  string in `LookupJoin` matches the registered source id.
- Swapped legacy `SESSION_START` / `SESSION_END` in the downstream
  Aggregate for TVF-style `window_start` / `window_end`, matching
  BUG-015's convention for TVF-based window functions.

**Codegen** — `src/codegen/sql-generator.ts`, `buildLookupJoinQuery`:
- Wrap the driving input in a CTE (`_lookup_<joinId>`) that adds
  `PROCTIME() AS proc_time`, keeping the processing-time attribute
  local to the join instead of adding a computed column to every source.
- Run `sameNameJoinKey` on the ON clause; when it matches, qualify the
  ON with side refs and project `left.*, right.col1, right.col2, ...`
  via `buildJoinProjectionSkippingRightCol` to drop the duplicate
  right-side join key. The right side's schema is resolved by looking
  up its `table` string as a node id — the dimension source the
  template now declares.

## Verification
```bash
REQUIRE_SQL_GATEWAY=1 pnpm vitest run \
  src/cli/templates/__tests__/template-explain.test.ts -t ecom-customer-360
```
Passes. `ecom-customer-360` removed from `SKIP`.

## Snapshots Rebaselined
The proc-time CTE wrapping shows up as a predictable diff in:
- `src/codegen/__tests__/__snapshots__/sql-generator.test.ts.snap`
  (`6.9: LookupJoin with async config`)
- `src/codegen/__tests__/__snapshots__/example-snapshots.test.ts.snap`
  (`07-lookup-join`, `20-realtime-dashboard`, `22-enrichment-archive`)
