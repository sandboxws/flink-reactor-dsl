# BUG-018: Multi-pair StatementSet emits first source for every sink [FIXED]

## Affected Examples
- `pump-ecom` (4 DataGenSource → KafkaSink pairs)
- `pump-iot` (2 DataGenSource → KafkaSink pairs)
- `pump-lakehouse` (3 DataGenSource → KafkaSink pairs)
- `lakehouse-ingest` (3 KafkaSource → IcebergSink pairs)

All four templates wrap alternating `<Source/><Sink/>` pairs inside a
`<StatementSet>`. Only the first pair passes EXPLAIN; later pairs fail
column-type validation.

## Flink Error
```
Column types of query result and sink for '<later_sink>' do not match.
```
(Exact message varies per sink — later IcebergSinks complain that the
query produces event columns while the sink expects clickstream /
transaction columns.)

## Root Cause
For a sink written in forward-reading JSX, `collectSinkDml` calls
`buildSiblingChainQuery(parent, sinkIndex, …)` to reconstruct the
upstream query from preceding siblings. The helper collects *every*
preceding sibling whose `kind` is `Source`/`Transform`/`Window`/`Join`/
`CEP` into a single chain, treating `chain[0]` as the root source and
`chain[1..]` as transforms applied on top of it.

Under `<StatementSet>`, the children interleave sources and sinks:

```
<StatementSet>
  <Source A/> <Sink A/>    ← pair 1
  <Source B/> <Sink B/>    ← pair 2
  <Source C/> <Sink C/>    ← pair 3
</StatementSet>
```

When `collectSinkDml` runs for Sink B, `buildSiblingChainQuery` sees
`[Source A, Source B]` as the chain. It treats Source A as the root
and Source B as a "transform." The transform-application path injects
a `VirtualRef` into Source B's `children`, then calls `buildQuery`.
The `DataGenSource`/`KafkaSource` case at `sql-generator.ts:2329-2349`
sees `children.length > 0`, enters the wrapper loop, and returns the
child's SQL — which is Source A's SQL. Net effect: every later sink's
`INSERT INTO` ends up selecting from Source A.

Observed output for `lakehouse-ingest`:

```sql
EXECUTE STATEMENT SET BEGIN
INSERT INTO `lakehouse`.`raw`.`events`       SELECT * FROM `lake_events`;
INSERT INTO `lakehouse`.`raw`.`clickstream`  SELECT * FROM `lake_events`;  -- wrong
INSERT INTO `lakehouse`.`raw`.`transactions` SELECT * FROM `lake_events`;  -- wrong
END;
```

The second and third `INSERT`s should read `lake_clickstream` and
`lake_transactions`. Because they all read `lake_events`, their column
types don't match their respective sinks, and Flink rejects the plan.

## Where to Fix
`src/codegen/sql-generator.ts` — `buildSiblingChainQuery` (≈L1791).
When building the sibling chain, start from the **most recent**
preceding `kind === "Source"` instead of `i = 0`. That way each sink
pairs with its immediately preceding source, and any transforms
between them are still included.

```ts
// Find the most recent preceding Source; if none, start at 0.
let startIdx = 0
for (let i = sinkIndex - 1; i >= 0; i--) {
  if (parent.children[i].kind === "Source") { startIdx = i; break }
}
for (let i = startIdx; i < sinkIndex; i++) { /* existing collection */ }
```

The single-pair case is unchanged (only one preceding Source, so
`startIdx` lands at it). The StatementSet case now correctly isolates
each `(Source, …, Sink)` pair.

## Verification
```bash
flink-reactor cluster up
REQUIRE_SQL_GATEWAY=1 pnpm vitest run \
  src/cli/templates/__tests__/template-explain.test.ts
```
Expect `pump-ecom`, `pump-iot`, `pump-lakehouse`, and `lakehouse-ingest`
to transition from `↓` (skipped) to `✓`. Remove them from the `SKIP`
set in `template-explain.test.ts`.
