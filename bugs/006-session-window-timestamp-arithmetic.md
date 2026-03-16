# BUG-006: Session window — TIMESTAMP subtraction used for duration computation [FIXED]

## Affected Examples
- `12-session-window`

## Flink Error
```
Cannot apply '-' to arguments of type '<TIMESTAMP(3) *ROWTIME*> - <TIMESTAMP(3) *ROWTIME*>'.
Supported form(s): '<NUMERIC> - <NUMERIC>'
```

## Root Cause
This is a combination of BUG-001 (type mismatch) and a Flink SQL semantics issue.

The example's Map transform computes `session_end - session_start AS session_duration`. In Flink SQL, the `-` operator cannot be applied directly to TIMESTAMP values. You need `TIMESTAMPDIFF(SECOND, session_start, session_end)` instead.

However, the immediate issue is that the sink schema resolves `session_start` and `session_end` as STRING (BUG-001), which makes the `-` operator fail even earlier. Even with correct types (TIMESTAMP(3)), the arithmetic would still fail because Flink doesn't support `TIMESTAMP - TIMESTAMP` directly.

## Generated SQL
```sql
INSERT INTO `user_sessions`
SELECT user_id AS `user_id`, ...,
       session_end - session_start AS `session_duration`,  -- ← invalid
       ...
FROM (
  SELECT `user_id`, ...,
         MIN(activity_time) AS `session_start`,
         MAX(activity_time) AS `session_end`,
         ...
  FROM TABLE(SESSION(...)) GROUP BY ...
);
```

## Two Fixes Required
1. **BUG-001 fix**: Schema introspection must resolve `MIN(TIMESTAMP)` → TIMESTAMP, not STRING
2. **Codegen or example fix**: The Map expression `session_end - session_start` needs to be rewritten as `TIMESTAMPDIFF(SECOND, session_start, session_end)` or the example should use a different computation

## Where to Fix
- `src/codegen/schema-introspect.ts` — fix type inference (see BUG-001)
- `src/examples/12-session-window/after.tsx` — the Map select expression for `session_duration` should use TIMESTAMPDIFF

## Verification
```bash
pnpm test:explain  # example 12 should pass after both fixes
```
