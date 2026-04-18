# BUG-015: TUMBLE/HOP window columns emitted uppercase, Flink expects lowercase [FIXED]

## Affected Templates/Pipelines
- `realtime-analytics/page-view-analytics` — TumbleWindow aggregation
- `grocery-delivery/grocery-store-rankings` — TumbleWindow + Deduplicate + Top-N
- `ecommerce/ecom-revenue-analytics` — HopWindow aggregation inside a Route

Surfaced by `src/cli/templates/__tests__/template-explain.test.ts` when run
against a local Flink SQL Gateway via scaffold-driven EXPLAIN validation.
The pre-existing hand-written EXPLAIN fixtures masked this because they
didn't exercise a scaffolded project with `flink-reactor.config.ts`.

## Flink Error
```
Column 'WINDOW_START' not found in any table; did you mean 'window_start'?
Column 'WINDOW_END' not found in any table; did you mean 'window_end'?
```

## Root Cause (hypothesis)
The codegen for windowed aggregations emits `WINDOW_START AS windowStart`
and `WINDOW_END AS windowEnd` in the SELECT list (and possibly `GROUP BY
WINDOW_START, WINDOW_END`). Flink's `TUMBLE` / `HOP` / `CUMULATE` TVFs
produce output columns named `window_start`, `window_end`, `window_time`
(lowercase), so the uppercase references fail to resolve.

Likely relevant: the similar `crd-generator.ts` path correctly produces
normalized enum values (see BUG-015 fix sibling for `checkpoint.mode` in
`sql-generator.ts`), suggesting a pattern of SQL vs. CRD divergence.

## Where to Fix
The actual bug location was **not** in the codegen. The codegen
(`src/codegen/sql-generator.ts:3079, 3088`) already emits `window_start`
/ `window_end` correctly in both the `GROUP BY` and the auto-projection.

The uppercase strings lived in the **templates themselves** as raw SQL
expressions passed via `Aggregate select={{ windowEnd: 'WINDOW_END' }}`.
Fixed by lowercasing the values in:
- `src/cli/templates/realtime-analytics.ts`
- `src/cli/templates/grocery-delivery.ts`
- `src/cli/templates/ecommerce.ts`
- `src/cli/templates/iot-factory.ts`
- `src/cli/templates/banking.ts`
- `src/cli/templates/ride-sharing.ts`
- `src/cli/templates/lakehouse-analytics.ts`
- `src/components/__tests__/windows.test.ts` (component test fixture)

## Verification
```bash
flink-reactor cluster up
REQUIRE_SQL_GATEWAY=1 pnpm vitest run src/cli/templates/__tests__/template-explain.test.ts
```

**Result:**
- ✓ `page-view-analytics` — passes EXPLAIN
- ✓ `grocery-store-rankings` — passes EXPLAIN
- ⚠ `ecom-revenue-analytics` — case-mismatch fixed, but a *separate*
  pre-existing bug (schema missing `category` field) was previously
  masked by this one and now surfaces. Tracked separately as
  [BUG-025](./025-ecom-revenue-schema-missing-category.md).

A.1 unblocks 2 of 3 advertised pipelines; the third moves from a
window-case bug to a schema-authoring bug.
