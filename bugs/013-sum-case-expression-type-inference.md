# BUG-013: SUM(CASE ...) expression inferred as STRING instead of BIGINT [FIXED]

## Affected Examples
- `25-batch-reporting` — `fraud_count: "SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END)"`

## Flink Error
```
Column types of query result and sink for 'category_summary' do not match.
```

## Root Cause
The `inferExpressionType()` function in `src/codegen/schema-introspect.ts` has a SUM regex that only matches simple column references:
```typescript
const sumMatch = trimmed.match(/^SUM\s*\(\s*`?(\w+)`?\s*\)/i)
```

This matches `SUM(amount)` but NOT `SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END)` because the inner expression isn't a simple `\w+` identifier. The expression falls through all patterns and returns the fallback type `"STRING"`.

The sink DDL declares `fraud_count STRING`, but the actual Flink query produces `BIGINT` (SUM of integers). This causes a column type mismatch at EXPLAIN time.

## Generated DDL
```sql
CREATE TABLE `category_summary` (
  `merchant_category` STRING,
  `transaction_date` DATE,
  `total_volume` DECIMAL(10, 2),
  `txn_count` BIGINT,
  `fraud_count` STRING              -- ← should be BIGINT
)
```

## Where to Fix
- `src/codegen/schema-introspect.ts` — `inferExpressionType()` SUM handling
- After the existing simple-column SUM match fails, add a fallback for `SUM(...)` with complex inner expressions:
  ```typescript
  if (/^SUM\s*\(/i.test(trimmed)) {
    return "BIGINT"  // SUM always returns numeric; BIGINT is a safe default
  }
  ```
- This should go after the existing `sumMatch` check so simple columns still get precise type inference

## Verification
```bash
pnpm test:explain  # example 25 should pass after fix (along with other fixes)
```
