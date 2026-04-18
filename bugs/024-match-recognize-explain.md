# BUG-024: Multiple issues prevent MATCH_RECOGNIZE pipeline from passing EXPLAIN [FIXED]

## Affected Templates/Pipelines
- `ride-sharing/rides-trip-tracking` — IntervalJoin → MatchRecognize →
  Route → JDBC/Kafka sinks.

Also partially unblocks `banking/bank-fraud-detection` (see BUG-027).

## Original Hypothesis (plan) vs. Actual Findings

The `bugs/PLAN.md` Sprint C.1 entry anticipated a Flink limitation —
historically the SQL Gateway hasn't supported `EXPLAIN` over
`MATCH_RECOGNIZE`, so the plan proposed adding a separate describe
block that validates these pipelines via `executeStatement` instead.

Investigation showed this hypothesis was **wrong for this pipeline** on
our Flink version. The statement never reached Flink's planner at all
— it was rejected at **parse time** by the SQL grammar, for reasons
unrelated to MATCH_RECOGNIZE's EXPLAIN support. Fixing the concrete
parse / validation / type errors unblocks the pipeline.

## Errors Encountered (chained)

Each fix surfaced the next error in the chain:

1. **Parse error on MEASURES** — `Encountered "pickup" at line 12,
   column 22` at `FIRST(eventTime, pickup)`. Flink's
   `FIRST(expr [, N])` expects the second argument to be an integer
   offset; `pickup` is a pattern variable name. Valid syntax is
   `FIRST(pickup.eventTime)` — qualified reference.

2. **Calcite validator assertion** — `Cycle detected during
   type-checking` at `MatchRecognizeNamespace.validateImpl`. Caused by
   the upstream `IntervalJoin` emitting malformed references:
   `ON requests.rideId = events.rideId` against tables actually named
   `rides_requests_2` / `rides_trip_events`, plus a triple-qualified
   `rides_requests_2.requests.requestTime` in the BETWEEN bounds. Same
   shape as BUG-019 but in a different template.

3. **Sink schema mismatch** — `Incompatible types for sink column
   'pickupTime' at position 3 … Query: TIMESTAMP(3) *ROWTIME*, Sink:
   STRING`. The auto-inferred sink DDL used `STRING` for columns the
   runtime actually computes as `TIMESTAMP(3)`, because
   `inferExpressionType` had no case for MATCH_RECOGNIZE's `FIRST`/
   `LAST` navigation functions (which are syntactically distinct from
   the `FIRST_VALUE`/`LAST_VALUE` analytical functions it does handle).
   The MatchRecognize schema resolver also had a too-narrow regex for
   stripping pattern-variable prefixes (only matched single-uppercase
   names like `A.`, `B.`), so `pickup.eventTime` never collapsed to
   `eventTime` for the type-inference lookup.

4. **Missing ORDER BY** — `You must specify either rowtime or proctime
   for order by` at `StreamExecMatch.checkOrderKeys`. `MATCH_RECOGNIZE`
   over a streaming input requires an explicit `ORDER BY <time_attr>`
   inside the MR clause. The template omitted the `orderBy` prop, so
   the (correct) codegen didn't emit the clause.

## Fixes

### Template — `src/cli/templates/ride-sharing.ts` (rides-trip-tracking)

1. **MEASURES syntax.** `FIRST(eventTime, pickup)` / `LAST(eventTime,
   dropoff)` → `FIRST(pickup.eventTime)` / `LAST(dropoff.eventTime)`.
2. **IntervalJoin source aliases.** Added `name: "requests"` and
   `name: "events"` to the two `KafkaSource` calls so the auto-derived
   table ids match the aliases used in `on`; dropped the `requests.`
   qualifier from `interval.from` / `interval.to` (same pattern as
   BUG-019).
3. **MATCH_RECOGNIZE ordering.** Added `orderBy: "eventTime"` to the
   `MatchRecognize` call — the events side's rowtime attribute is the
   natural ordering column for this pattern.

### Codegen — `src/codegen/schema-introspect.ts`

1. **Pattern-variable prefix-strip regex.** In the `MatchRecognize`
   branch of `resolveNodeSchema`, broadened `\b[A-Z]\.` to
   `\b[A-Za-z_]\w*\.` so that multi-character lowercase pattern
   variables (`pickup.`, `dropoff.`) are recognized, not only
   single-uppercase ones (`A.`, `B.`). Safe because
   `MATCH_RECOGNIZE` operates on a single input relation — any dotted
   prefix inside MEASURES is a pattern variable.
2. **`FIRST`/`LAST` type inference.** Added a dedicated block in
   `inferExpressionType` that matches `FIRST|LAST\s*\(\s*col\s*(?:,\s*
   N)?\s*\)` and returns the source column's type (defaults to
   `STRING` only when the column isn't in the upstream schema).
   Kept as a separate block — not merged into the existing
   MIN/MAX/FIRST_VALUE/LAST_VALUE regex — so future readers can
   immediately see that FIRST/LAST here refers to the MATCH_RECOGNIZE
   navigation functions (which are *not* the same as the
   `FIRST_VALUE`/`LAST_VALUE` analytical functions, despite similar
   type semantics). Mirrors the grouping precedent set by the
   STDDEV_POP variance-family block added under BUG-023.

## Why Not the Plan's Proposed Approach

The plan suggested adding a describe block that validates
MATCH_RECOGNIZE via `executeStatement` instead of EXPLAIN. That would
be the right move if the current Flink version actually refused to
EXPLAIN MATCH_RECOGNIZE — but once the parse/validation errors above
were fixed, EXPLAIN worked. Keeping the single EXPLAIN-based
validation path is simpler (one code path, one SKIP set, one reporter)
and catches real regressions earlier than a separate describe block
would.

## Side Effect on BUG-027

The codegen type-inference fix also addresses the *reported* type-
inference side of BUG-027 (`bank-fraud-detection`): that pipeline's
`FIRST(txnTime)` / `LAST(txnTime)` now infer `TIMESTAMP(3)` correctly.
However, the template ALSO misses an `orderBy` prop (same bug #4 as
above), so the pipeline remains skipped under BUG-027 pending a
template-side fix.

## Verification

```bash
flink-reactor cluster up
REQUIRE_SQL_GATEWAY=1 pnpm vitest run \
  src/cli/templates/__tests__/template-explain.test.ts \
  -t rides-trip-tracking --reporter=verbose
# ride-sharing/rides-trip-tracking passes (moved ↓ → ✓)
```

Also:
```bash
pnpm typecheck && pnpm vitest run && pnpm lint
```

## Result — SKIP set shrinks by 1

- `rides-trip-tracking` removed from SKIP.
- `bank-fraud-detection` still in SKIP, now under an updated BUG-027
  comment noting the type-inference half is done.
- SKIP set `21 → 20`? No — SKIP was already 4 entries going into this
  fix (6 were "skipped" due to other dependencies). Count: `6 → 5`
  skipped tests after C.1 lands.
