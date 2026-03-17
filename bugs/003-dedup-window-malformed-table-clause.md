# BUG-003: Deduplicate inside Window generates malformed TABLE() clause [FIXED]

## Affected Examples
- `11-dedup-window` — Deduplicate → TumbleWindow → Aggregate
- `38-dedup-aggregate` — identical pattern (sandbox-derived copy)
- `19-union-aggregate` — Union → TumbleWindow (similar TABLE nesting issue)

## Flink Error
```
Encountered "(" at line 3, column 16.
```
(Parser encounters unexpected `(` in the TABLE clause of the window function)

## Root Cause
When a Deduplicate precedes a Window function, the generated SQL wraps the dedup subquery inside `TABLE(...)` with incorrect parenthesization. The dedup subquery (using `ROW_NUMBER() OVER ... QUALIFY`) is wrapped as a nested `TABLE (SELECT ...)` inside the `TUMBLE(TABLE ...)` call, which Flink's parser rejects.

## Generated SQL (11-dedup-window)
```sql
INSERT INTO `hourly_user_events`
SELECT `user_id`, `event_type`, COUNT(*) AS `event_count`, window_start, window_end FROM TABLE(
  TUMBLE(TABLE (                                           -- ← malformed: extra TABLE(
SELECT *, ROW_NUMBER() OVER (PARTITION BY `event_id` ORDER BY `event_time` ASC) AS rownum
FROM `raw_events`
QUALIFY rownum = 1
), DESCRIPTOR(`event_time`), INTERVAL '1' HOUR)
) GROUP BY `user_id`, `event_type`, window_start, window_end;
```

### Expected SQL
The dedup subquery should be wrapped as a subquery in a CTE or inline view, not nested directly in TABLE():
```sql
INSERT INTO `hourly_user_events`
SELECT `user_id`, `event_type`, COUNT(*) AS `event_count`, window_start, window_end FROM TABLE(
  TUMBLE(TABLE `dedup_raw_events`, DESCRIPTOR(`event_time`), INTERVAL '1' HOUR)
) GROUP BY `user_id`, `event_type`, window_start, window_end;
```
Where `dedup_raw_events` is either a CTE or a CREATE VIEW.

## Where to Fix
- `src/codegen/sql-generator.ts` — the code path that generates Window function SQL when the input is a Deduplicate node
- The Deduplicate subquery needs to be materialized as a CTE/VIEW before being passed to the TUMBLE/HOP/SESSION TABLE function, since Flink's TVF syntax only accepts `TABLE <table_name>`, not `TABLE (<subquery>)`

## Verification
```bash
pnpm test:explain  # examples 11, 19, 38 should pass after fix
```
