# BUG-027: MatchRecognize MEASURES type inference returns STRING for FIRST/LAST(timestamp_col)

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

## Where to Investigate
- `src/codegen/sql-generator.ts` — search for the function that builds
  the auto-derived sink schema downstream of `MatchRecognize`. Likely
  pattern: when a `MEASURES` expression is `FIRST(col)` or `LAST(col)`,
  inherit the type of `col` from the upstream schema instead of defaulting
  to STRING.
- Cross-check against `Aggregate` codegen — `SUM`/`COUNT`/`AVG` likely
  have similar inference paths and may already do this correctly.

## Verification
```bash
REQUIRE_SQL_GATEWAY=1 pnpm vitest run src/cli/templates/__tests__/template-explain.test.ts
```
`bank-fraud-detection` should pass EXPLAIN; the sink table's `firstTxn`
/ `lastTxn` columns should be declared as `TIMESTAMP(3)`.
