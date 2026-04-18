# BUG-027: MatchRecognize MEASURES type inference returns STRING for FIRST/LAST(timestamp_col) [TYPE-INFERENCE FIXED]

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

## Residual Issue — Pipeline Still Skipped

After the type-inference fix, `bank-fraud-detection` still fails
EXPLAIN with a different error:

```
You must specify either rowtime or proctime for order by.
```

at `StreamExecMatch.checkOrderKeys`. Flink requires an explicit
`ORDER BY <rowtime-col>` inside `MATCH_RECOGNIZE` when the input is a
streaming table.

The `bank-fraud-detection` template's `MatchRecognize` declaration
doesn't pass an `orderBy` prop, so the codegen (correctly) doesn't
emit the `ORDER BY` clause. Adding `orderBy: "txnTime"` to the
template should complete the fix, but that's a template authoring
change tracked separately from the codegen work. Pipeline remains in
`SKIP` under BUG-027 until that template change lands.

## Verification
```bash
REQUIRE_SQL_GATEWAY=1 pnpm vitest run \
  src/cli/templates/__tests__/template-explain.test.ts -t bank-fraud-detection
```
The `firstTxn` / `lastTxn` sink-schema error no longer appears. The
failure mode has advanced to the missing `ORDER BY` (to be resolved
by a template fix).
