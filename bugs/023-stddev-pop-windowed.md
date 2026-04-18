# BUG-023: `STDDEV_POP` in windowed Aggregate produces wrong sink column type; `value` collides with reserved word [FIXED]

## Affected Templates/Pipelines
- `iot-factory/iot-predictive-maintenance` — SlideWindow → Aggregate with
  `STDDEV_POP(value)` / `AVG(value)` / `MAX(value)`, feeding a TemporalJoin
  and a Route into Kafka and JDBC sinks.

Surfaced by
`src/cli/templates/__tests__/template-explain.test.ts` when run against a
local Flink SQL Gateway (`REQUIRE_SQL_GATEWAY=1`).

## Flink Errors

Two distinct failures chained back-to-back.

**(1) Parse error from the unquoted column name `value`:**
```
SQL parse failed. Encountered ")" at line 4, column 43.
```
at position `AVG(value)` — Flink/Calcite treats bare `VALUE` as the start
of a `VALUE (row...)` row-constructor expression (a `TableConstructor`),
so inside `AVG(...)` the parser tries to consume row contents and fails
on the immediate `)`.

**(2) After (1) is fixed, sink DDL type mismatch:**
```
Column types of query result and sink for
'default_catalog.default_database.iot_maintenance_alerts' do not match.
Cause: Incompatible types for sink column 'stddevValue' at position 3.

Query schema:  [..., stddevValue: DOUBLE, ...]
Sink schema:   [..., stddevValue: STRING, ...]
```
The query produces `DOUBLE` (Flink types `STDDEV_POP` correctly), but the
auto-inferred sink DDL declares `STRING`.

## Root Causes

### (1) Template authoring bug
`src/cli/templates/iot-factory.ts` passed raw SQL expressions
`AVG(value)`, `STDDEV_POP(value)`, `MAX(value)` into `Aggregate select={{ … }}`.
Raw SQL expressions are opaque to the codegen — it doesn't rewrite them —
so identifiers that collide with reserved/non-reserved keywords must be
backtick-quoted by the template author.

### (2) Codegen gap in type inference
`src/codegen/schema-introspect.ts::inferExpressionType` pattern-matches
common aggregates (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `FIRST_VALUE`,
`LAST_VALUE`) and falls through to `STRING` for anything else. The
variance-family aggregates (`STDDEV_POP`, `STDDEV_SAMP`, `VAR_POP`,
`VAR_SAMP`) had no case, so sink DDL auto-inference produced `STRING`
for columns that the runtime actually computes as `DOUBLE`.

## Fix

- `src/cli/templates/iot-factory.ts`: backtick-quote `value` inside the
  three aggregate expressions (`` AVG(`value`) `` etc.).
- `src/codegen/schema-introspect.ts`: add a regex case that treats
  `STDDEV_POP`/`STDDEV_SAMP`/`VAR_POP`/`VAR_SAMP` like `AVG` —
  output is `DOUBLE`, except `DECIMAL` input is preserved.

The two fixes are independent: either alone leaves the pipeline broken.
Shipping both as one PR under BUG-023 because they're both preconditions
to exercising the same code path.

## Why Group the Variance Family Together
`STDDEV_SAMP`, `VAR_POP`, `VAR_SAMP` share *identical* type-inference
rules with `STDDEV_POP` per the Flink SQL reference. One regex matches
the existing precedent for grouping identity-preserving aggregates
(`MIN/MAX/FIRST_VALUE/LAST_VALUE`) — the marginal cost is zero, and it
prevents the same bug recurring for the three siblings.

## Residual Issue (Not Covered Here)

After both fixes, `iot-predictive-maintenance` still fails EXPLAIN with
a third, unrelated error:

```
Temporal table join currently only supports 'FOR SYSTEM_TIME AS OF'
left table's time attribute field
```

The windowed-Aggregate output's `window_end` column loses its rowtime-
attribute metadata through the `window_end AS windowEnd` alias and the
CTE wrapper, so the downstream TemporalJoin's `FOR SYSTEM_TIME AS OF
Aggregate_99.windowEnd` fails validation.

Tracked separately as [BUG-030](./030-temporal-join-on-windowed-agg-output.md).
The pipeline remains in the EXPLAIN `SKIP` set under BUG-030 until that
is resolved.

## Verification

```bash
flink-reactor cluster up
REQUIRE_SQL_GATEWAY=1 pnpm vitest run \
  src/cli/templates/__tests__/template-explain.test.ts --reporter=verbose
```

The `value` parse error and the `stddevValue: STRING` sink-schema error
no longer appear in the failure trace. The pipeline advances to the
temporal-join validation (BUG-030).

Additionally:
```bash
pnpm typecheck && pnpm vitest run && pnpm lint
```
