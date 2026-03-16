# BUG-007: MatchRecognize excluded from sibling chain — generates SELECT * FROM unknown

## Affected Examples
- `15-cep-fraud-detection`

## Flink Error
```
Encountered "unknown" at line 3, column 15.
```

## Root Cause
`MatchRecognize` has `kind: "CEP"` which isn't recognized by `buildSiblingChainQuery` in `src/codegen/sql-generator.ts`. The sibling chain filter only includes `Source | Transform | Window | Join`, so the MatchRecognize node is skipped entirely. The downstream Map transform sees no upstream and falls back to `SELECT * FROM unknown`.

## Generated SQL
```sql
INSERT INTO `fraud_alerts`
SELECT card_id AS `card_id`, ... FROM (
SELECT * FROM unknown   -- ← MatchRecognize was skipped
);
```

## Where to Fix
- `src/codegen/sql-generator.ts` — `buildSiblingChainQuery()` line ~1566: add `sibling.kind === "CEP"` to the kind filter
- Same pattern as the BUG-002 fix that added `"Join"` to the filter

## Verification
```bash
pnpm test:explain  # example 15 should pass after fix
```
