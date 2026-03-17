# BUG-010: KafkaSource with PRIMARY KEY generates invalid DDL (needs upsert-kafka connector) [FIXED]

## Affected Examples
- `16-temporal-join` — CurrencyRateSchema has `primaryKey: { columns: ["currency_pair"] }`

## Flink Error
```
The Kafka table 'default_catalog.default_database.currency_rates' with 'json' format
doesn't support defining PRIMARY KEY constraint on the table, because it can't
guarantee the semantic of primary key.
```

## Root Cause
The source DDL generator in `generateSourceDdl()` always emits `PRIMARY KEY (...) NOT ENFORCED` when the schema defines a primary key, and always uses `'connector' = 'kafka'` for KafkaSource. But Flink's `kafka` connector with `json` format does not support PRIMARY KEY constraints — it requires `upsert-kafka` connector instead.

The sink DDL generator already handles this correctly: `generateSinkWithClause()` checks `changelogMode === "retract"` and switches to `upsert-kafka`. But the source DDL generator has no equivalent logic.

## Generated DDL
```sql
CREATE TABLE `currency_rates` (
  `currency_pair` STRING,
  `exchange_rate` DECIMAL(12, 6),
  `rate_time` TIMESTAMP(3),
  PRIMARY KEY (`currency_pair`) NOT ENFORCED,     -- ← incompatible with kafka+json
  WATERMARK FOR `rate_time` AS `rate_time` - INTERVAL '1' SECOND
) WITH (
  'connector' = 'kafka',                          -- ← should be 'upsert-kafka'
  'format' = 'json',
  ...
);
```

## Where to Fix
- `src/codegen/sql-generator.ts` — source DDL generation (look for KafkaSource WITH clause generation)
- When a KafkaSource schema has a primary key, switch connector to `'upsert-kafka'` and use `'value.format'` instead of `'format'`
- Alternative: this could be a TSX example bug — change the temporal table to use a JDBC source or `debezium-json` format

## Verification
```bash
pnpm test:explain  # example 16 should pass after fix
```
