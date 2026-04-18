# BUG-022: Route branch after a passthrough window emits extra `window_time` column [FIXED]

## Affected Examples
- `bank-compliance-agg` — Route.Branch without an `Aggregate`, downstream of a
  sibling `TumbleWindow`, fans out into a `KafkaSink`. Surfaced by
  `src/cli/templates/__tests__/template-explain.test.ts`.

## Flink Error
```
Column types of query result and sink for 'default_catalog.default_database.bank_compliance_reports' do not match.
Cause: Different number of columns.

Query schema:   [..., txnTime: TIMESTAMP(3), window_start: TIMESTAMP(3), window_end: TIMESTAMP(3), window_time: TIMESTAMP(3)]
Sink schema:    [..., txnTime: TIMESTAMP(3), window_start: TIMESTAMP(3), window_end: TIMESTAMP(3)]
```

## Root Cause
Flink's `TUMBLE` / `HOP` / `SESSION` table-valued functions append **three**
columns to their input: `window_start`, `window_end`, **and** `window_time`.

- `src/codegen/schema-introspect.ts` (`resolveNodeSchema` / `resolveTransformSchema`)
  models the passthrough window as "upstream columns + `window_start` +
  `window_end`". The inferred sink DDL therefore has N+2 columns.
- `src/codegen/sql-generator.ts` `buildWindowQuery` emits
  `SELECT * FROM TABLE(TUMBLE(...))` when there's no `Aggregate` child. That
  `*` materialises N+3 columns, including `window_time`.

When this passthrough feeds a sink (directly, or — as in
`bank-compliance-agg` — through a `Route.Branch` whose only operation is a
filter), the INSERT's projection has one more column than the sink DDL and
EXPLAIN fails.

The windowed-aggregation path is unaffected because `buildWindowQuery`'s
`aggChild` branch already emits an explicit column list (`groupBy…,
select…, window_start, window_end`) that naturally excludes `window_time`.

## Where to Fix
- `src/codegen/sql-generator.ts` — in `collectRouteDml`, when a branch has
  no `Aggregate` transform and the sink's resolved schema contains
  `window_start`/`window_end` but not `window_time`, wrap the branch query
  with an explicit projection of the sink's column list. This drops
  `window_time` (the extra ROWTIME column emitted by the TVF) while
  leaving untouched the cases where an `Aggregate` already projects
  explicitly. The `sinkMeta` map from `resolveSinkMetadata` is threaded
  through `generateDml` → `collectSinkDml` → `collectRouteDml` to
  provide the schema at INSERT-building time.

## Why Not Touch `buildWindowQuery`
A narrower fix — projecting explicitly inside `buildWindowQuery`'s
passthrough branch — looked simpler, but it misprojects when the window
is downstream of a `LookupJoin` (or any other join). `resolveTransformSchema`
defaults to `inputSchema` for joins, so a `VirtualRef._schema` injected
into the Window wouldn't carry the joined-in columns. Doing the wrap at
the sink boundary uses the already-correct `resolveSinkMetadata` schema
and avoids re-deriving join schemas here.

## Why Not Add `window_time` to the Inferred Schema
Including `window_time` in the sink DDL matches the TVF's column count
but causes Flink to reject the INSERT: both the source's watermarked
column (e.g. `txnTime`) and the inherited `window_time` are ROWTIME
attributes, and Flink forbids more than one ROWTIME column in a sink.
Stripping `window_time` at the INSERT boundary avoids the conflict.

## Verification
```bash
flink-reactor cluster up
REQUIRE_SQL_GATEWAY=1 pnpm vitest run \
  src/cli/templates/__tests__/template-explain.test.ts --reporter=verbose
# banking/bank-compliance-agg should move from ↓ to ✓
```

Also:
```bash
pnpm typecheck && pnpm vitest run && pnpm lint
```
