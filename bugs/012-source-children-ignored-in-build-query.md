# BUG-012: Source nodes with child transforms — children ignored in buildQuery [FIXED]

## Affected Examples
- `19-union-aggregate` — Union children are `<KafkaSource><Map/></KafkaSource>`

## Flink Error
```
Column 'platform' not found in any table
```

## Root Cause
Each Union child is a KafkaSource that wraps a Map transform as a child:
```tsx
const mobile = (
  <KafkaSource topic="mobile_events" ...>
    <Map select={{ ..., platform: "'mobile'" }} />
  </KafkaSource>
)
```

But `buildQuery()` treats source nodes as terminal:
```typescript
case "KafkaSource":
case "JdbcSource":
case "GenericSource":
  return `SELECT * FROM ${q(node.id)}`
```

The Map child that adds the `platform` column is completely ignored. The Union query generates `SELECT * FROM mobile_events UNION ALL SELECT * FROM web_events ...` without the Map projections. The downstream Aggregate then fails because `platform` doesn't exist.

## Generated SQL
```sql
SELECT * FROM `mobile_events`    -- ← missing Map projection that adds 'platform'
UNION ALL
SELECT * FROM `web_events`
UNION ALL
SELECT * FROM `iot_events`
```

### Expected SQL
```sql
SELECT `event_id`, `user_id`, `event_type`, `event_time`, 'mobile' AS `platform`
FROM `mobile_events`
UNION ALL
SELECT `event_id`, `user_id`, `event_type`, `event_time`, 'web' AS `platform`
FROM `web_events`
UNION ALL
SELECT `event_id`, `device_id` AS `user_id`, `event_type`, `event_time`, 'iot' AS `platform`
FROM `iot_events`
```

## Options to Fix

### Option A: Fix codegen — handle source children in buildQuery
When a Source node has children, don't treat it as terminal. Instead, build the source reference and then apply child transforms (like reverse-nesting does for sinks):
```typescript
case "KafkaSource":
  if (node.children.length > 0) {
    // Process children as transforms on top of this source
    return buildQuery(node.children[0], nodeIndex, pluginSql)
  }
  return `SELECT * FROM ${q(node.id)}`
```

### Option B: Fix TSX example — use reverse-nesting
Restructure to `<Map><KafkaSource/></Map>` instead of `<KafkaSource><Map/></KafkaSource>`:
```tsx
const mobile = (
  <Map select={{ ..., platform: "'mobile'" }}>
    <KafkaSource topic="mobile_events" ... />
  </Map>
)
```

## Where to Fix
- `src/codegen/sql-generator.ts` — `buildQuery()` source cases (line ~2071)
- Or the TSX example itself

## Verification
```bash
pnpm test:explain  # example 19 should pass after fix
```
