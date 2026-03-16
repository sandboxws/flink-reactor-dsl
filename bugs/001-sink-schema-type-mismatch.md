# BUG-001: Sink schema type mismatch — STRING instead of correct aggregate types [FIXED]

## Affected Examples
- `05-hop-window` — `max_temp` and `min_temp` resolved as STRING instead of DOUBLE
- `14-ohlcv-window` — similar type mismatch in OHLCV aggregate columns
- `12-session-window` — `session_start` and `session_end` resolved as STRING instead of TIMESTAMP(3)

## Flink Error
```
Column types of query result and sink for 'default_catalog.default_database.sensor_metrics' do not match.
```
```
Cannot apply '-' to arguments of type '<TIMESTAMP(3) *ROWTIME*> - <TIMESTAMP(3) *ROWTIME*>'.
Supported form(s): '<NUMERIC> - <NUMERIC>'
```

## Root Cause
The sink schema resolution in `src/codegen/schema-introspect.ts` is not correctly inferring return types for aggregate functions applied inside windowed queries. When a `Map` transform references aggregate output columns (e.g. `max_temp` from `MAX(temperature)`), the schema introspector falls back to STRING instead of propagating the correct type.

For `05-hop-window`, the pipeline is:
```
KafkaSource(temperature: DOUBLE) → HopWindow → Aggregate(MAX(temperature) AS max_temp) → Map(max_temp - min_temp AS temp_variance) → JdbcSink
```

The sink DDL generates:
```sql
`max_temp` STRING,   -- should be DOUBLE (MAX of DOUBLE → DOUBLE)
`min_temp` STRING,   -- should be DOUBLE (MIN of DOUBLE → DOUBLE)
`temp_variance` BIGINT  -- computed as max_temp - min_temp, should be DOUBLE
```

For `12-session-window`, `MIN(activity_time)` and `MAX(activity_time)` should produce TIMESTAMP(3) but get resolved as STRING.

## Generated SQL (05-hop-window sink)
```sql
CREATE TABLE `sensor_metrics` (
  `sensor_id` STRING,
  `avg_temp` DOUBLE,
  `max_temp` STRING,     -- ← WRONG: should be DOUBLE
  `min_temp` STRING,     -- ← WRONG: should be DOUBLE
  `avg_humidity` DOUBLE,
  `window_start` TIMESTAMP(3),
  `window_end` TIMESTAMP(3),
  `temp_variance` BIGINT  -- ← WRONG: should be DOUBLE (DOUBLE - DOUBLE)
) WITH (...)
```

## Where to Fix
- `src/codegen/schema-introspect.ts` — the type inference for aggregate function return types (MAX, MIN, SUM, AVG, COUNT) when consumed by downstream Map transforms
- Aggregate functions should preserve input types: MAX(DOUBLE) → DOUBLE, MIN(TIMESTAMP) → TIMESTAMP, etc.
- Computed expressions in Map (e.g. `max_temp - min_temp`) need type inference based on operand types

## Verification
```bash
pnpm test:explain  # examples 05, 12, 14 should pass after fix
```
