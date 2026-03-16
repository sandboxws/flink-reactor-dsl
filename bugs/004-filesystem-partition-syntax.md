# BUG-004: Filesystem sink PARTITIONED BY — missing columns and invalid function syntax [FIXED]

## Affected Examples
- `23-batch-etl` — `PARTITIONED BY (sale_date)` but `sale_date` not in column list
- `24-lambda-architecture` — `PARTITIONED BY (DATE(event_time), HOUR(event_time))` — function calls not supported
- `28-branching-iot` — same DATE/HOUR partition function issue

## Flink Errors
```
Partition column 'sale_date' not defined in the table schema. Available columns: ['']
```
```
Incorrect syntax near the keyword 'DATE' at line 10, column 19.
```

## Root Cause

Two related issues in `FileSystemSink` DDL generation:

### Issue A: Missing partition columns in schema (23-batch-etl)
The sink DDL is generated with `PARTITIONED BY (sale_date)` but the column definitions are empty or don't include `sale_date`. The sink schema resolver doesn't include the partition columns in the CREATE TABLE column list.

```sql
CREATE TABLE `daily_category_sales` PARTITIONED BY (sale_date) WITH (
  'connector' = 'filesystem',
  ...
);
-- No column definitions! Partition column 'sale_date' is not defined.
```

### Issue B: Function calls in PARTITIONED BY (24, 28)
The codegen emits `PARTITIONED BY (DATE(event_time), HOUR(event_time))` which is invalid Flink SQL. Flink's `PARTITIONED BY` only accepts column references, not expressions.

```sql
CREATE TABLE `raw` (
  ...
) PARTITIONED BY (DATE(event_time), HOUR(event_time)) WITH (
  'connector' = 'filesystem',
  ...
);
```

The correct approach is to define computed columns and partition by those:
```sql
CREATE TABLE `raw` (
  ...
  `dt` DATE AS CAST(event_time AS DATE),
  `hr` INT AS HOUR(CAST(event_time AS TIMESTAMP))
) PARTITIONED BY (`dt`, `hr`) WITH (...);
```

## Where to Fix
- `src/codegen/sql-generator.ts` — `generateSinkDdl()` for FileSystemSink
- Issue A: Ensure partition columns are included in the column definitions
- Issue B: When partition expressions contain function calls, generate computed columns and reference those in PARTITIONED BY
- `src/components/sinks.ts` — the `FileSystemSink` component props may need to support partition expressions as computed columns

## Verification
```bash
pnpm test:explain  # examples 23, 24, 28 should pass after fix
```
