# BUG-011: Anti-join produces retract but KafkaSink has no PK for upsert mode

## Affected Examples
- `18-broadcast-join`

## Flink Error
```
Table sink 'default_catalog.default_database.valid_events' doesn't support consuming
update and delete changes which is produced by node Join(joinType=[LeftAntiJoin], ...)
```

## Root Cause
The anti-join between a streaming KafkaSource and a JDBC blacklist table produces a retract changelog (records can be retracted when the blacklist changes). The codegen correctly detects this (`propagateChangelogMode` marks anti/semi joins as retract). However, the KafkaSink auto-upsert logic requires BOTH conditions:

1. `changelogMode === "retract"` ✓
2. `primaryKey.length > 0` ✗ — the left source (EventSchema) has no PK

Without a PK, the sink uses regular `'connector' = 'kafka'` which is append-only and cannot consume retract streams.

Adding a PK to EventSchema would trigger BUG-010 (kafka+json doesn't support PK on sources).

## Options to Fix

### Option A: Change the example pattern
Replace the anti-join with a LookupJoin + Filter pattern:
```tsx
<LookupJoin input={events} table="blacklist" url="..." on="..." />
<Filter condition="blacklist_user_id IS NULL" />
```
LookupJoin treats the right side as a dimension table (point-in-time lookup), producing append-only output.

### Option B: Fix codegen for upsert-kafka sources
Fix BUG-010 first (upsert-kafka for sources with PK), then add PK to EventSchema. The source would use upsert-kafka, and the sink would auto-detect retract + PK → upsert-kafka.

### Option C: Support changelog-json sink format
Add codegen support for `'connector' = 'kafka', 'format' = 'debezium-json'` which can handle retract streams without requiring a PK.

## Where to Fix
- Depends on chosen option — see above
- The fundamental issue is the pipeline design: anti-join against a mutable dimension table inherently produces retractions

## Verification
```bash
pnpm test:explain  # example 18 should pass after fix
```
