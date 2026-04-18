# BUG-027: MatchRecognize MEASURES type inference returns STRING for FIRST/LAST(timestamp_col) [FIXED]

## Affected Templates/Pipelines
- `banking/bank-fraud-detection`

Surfaced after BUG-017 cleared the temporal-join ambiguity for this
pipeline. Now Flink reaches sink-schema validation and rejects:

## Flink Error
```
Column types of query result and sink for 'default_catalog.default_database.bank_fraud_alerts' do not match.

Query schema: [accountId STRING, txnCount BIGINT, totalAmount DOUBLE,
               firstTxn TIMESTAMP(3), lastTxn TIMESTAMP(3),
               name STRING, tier STRING, status STRING, balance DOUBLE,
               updateTime TIMESTAMP(3)]
Sink  schema: [accountId STRING, txnCount BIGINT, totalAmount DOUBLE,
               firstTxn STRING,  lastTxn STRING,
               name STRING, tier STRING, status STRING, balance DOUBLE,
               updateTime TIMESTAMP(3)]
```

## Root Cause (hypothesis)
The `bank-fraud-detection` pipeline defines a `MatchRecognize` with:
```ts
measures: {
  ...,
  firstTxn: 'FIRST(txnTime)',
  lastTxn:  'LAST(txnTime)',
}
```
`txnTime` is `TIMESTAMP(3)` on the upstream `TransactionSchema`. The
`MatchRecognize` codegen / sink-DDL inference appears to default
unrecognized expression results to `STRING` rather than walking back to
the input column's declared type.

## Where Fixed (type-inference portion)

Fixed in `src/codegen/schema-introspect.ts` under BUG-024 (C.1 —
MATCH_RECOGNIZE for `rides-trip-tracking`). The fix adds a
`MATCH_RECOGNIZE FIRST/LAST(col [, N])` case to `inferExpressionType`
that preserves the input column's type (parallel to the existing
MIN/MAX/FIRST_VALUE/LAST_VALUE case). It also broadens the
pattern-variable prefix-strip regex in the MatchRecognize schema
resolver from `\b[A-Z]\.` to `\b[A-Za-z_]\w*\.` so that multi-character
lowercase pattern variables (`pickup.`, `dropoff.`) are handled, not
just single-uppercase-letter ones (`A.`, `B.`).

See [`024-match-recognize-explain.md`](./024-match-recognize-explain.md)
for the full investigation.

## Template Fixes (remaining errors after codegen fix)

After the codegen type-inference fix shipped under BUG-024, two more
Flink validation errors surfaced on `bank-fraud-detection` — both
pure template bugs:

1. **Missing `ORDER BY`.** Flink's `StreamExecMatch.checkOrderKeys`
   rejects `MATCH_RECOGNIZE` over a streaming input without an
   explicit `ORDER BY <rowtime_col>`. The template's `MatchRecognize`
   declaration didn't pass an `orderBy` prop. Added
   `orderBy: "txnTime"` (the `TransactionSchema`'s watermarked rowtime).

2. **Greedy trailing quantifier.** The pattern `"high{3,}"` with a
   greedy open-ended quantifier as its final element is rejected by
   Flink:
   ```
   Greedy quantifiers are not allowed as the last element of a Pattern
   yet. Finish your pattern with either a simple variable or reluctant
   quantifier.
   ```
   Changed to `"high{3,}?"` — the reluctant form matches the minimum
   required (3) then commits, which is also better fraud-detection
   semantics (fire as soon as the threshold is crossed rather than
   waiting for the longest possible run).

Both fixes are in `src/cli/templates/banking.ts`.

## Verification
```bash
REQUIRE_SQL_GATEWAY=1 pnpm vitest run \
  src/cli/templates/__tests__/template-explain.test.ts -t bank-fraud-detection
```
`banking/bank-fraud-detection` moves from `↓` → `✓`. Removed from the
EXPLAIN `SKIP` set.
