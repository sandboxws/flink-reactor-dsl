# BUG-021: buildJoinQuery doesn't prune duplicate shared-key columns [FIXED]

## Affected Examples
- `rides-surge-pricing` (ride-sharing template)

## Flink Error
```
Column types of query result and sink for
'default_catalog.default_database.rides_surge_prices' do not match.
```
The sink has `zoneId STRING` once; the query emits `zoneId` twice —
once from the broadcast-join's left side, once from the right.

## Generated SQL (before fix)
```sql
INSERT INTO `rides_surge_prices`
WITH `Aggregate_3` AS (
  SELECT `zoneId`, COUNT(*) AS `demandCount`, window_end AS `windowEnd`
  FROM TABLE(TUMBLE(TABLE `rides_requests`, DESCRIPTOR(`requestTime`),
                     INTERVAL '1' MINUTE))
  GROUP BY `zoneId`, window_end
)
SELECT /*+ BROADCAST(`rides_surge_zones`) */ *
FROM `Aggregate_3` INNER JOIN `rides_surge_zones`
  ON demand.zoneId = surgeZones.zoneId;
```

`SELECT *` pulls `Aggregate_3.zoneId` + `rides_surge_zones.zoneId`; the
sink declares one.

## Root Cause
`BroadcastJoin` compiles to a regular `Join` node, which `buildJoinQuery`
turns into a `SELECT * FROM left JOIN right ON ...`. That emitter never
acquired the shared-key projection pruning that `buildTemporalJoinQuery`
(BUG-017), `buildIntervalJoinQuery` (BUG-019), and `buildLookupJoinQuery`
(BUG-020) all use to drop the duplicate right-side join column.

The template's ON clause — `demand.zoneId = surgeZones.zoneId` — is the
qualified shared-key form that the extended `sameNameJoinKey`
(BUG-019) already recognises. It just isn't being called from
`buildJoinQuery`.

PLAN.md described this as "BroadcastJoin references intermediate
`demand` table not registered" (BUG-009 pattern). That's actually not
the problem — `resolveJoinOperand` already wraps non-source operands in
CTEs, so `Aggregate_3` is emitted and referenced correctly. The
diagnosis was stale; the real blocker is the duplicate column.

## Where Fixed

**Codegen** — `src/codegen/sql-generator.ts`:

- `buildJoinQuery`: when `sameNameJoinKey(onCondition)` matches,
  qualify the ON with side refs and replace `SELECT *` with
  `buildJoinProjectionSkippingRightCol(...)`. Anti/semi joins are
  untouched — they already project `left.*` only.

- `resolveSinkMetadata` (Join branch): inner/left/right/full joins now
  become retract when *either* input source carries a retract
  changelog (previously only anti/semi joins did). Primary-key
  resolution also prefers a same-name join key as the natural PK of
  the result, falling back to the left source's PK.

- `resolveSourceChangelogMode`: a KafkaSource with a schema primary
  key is emitted as `upsert-kafka` and therefore carries upsert
  (retract-equivalent) changelog semantics. Previously it stayed
  "append-only" because `inferChangelogMode` only looks at the format
  string (json/avro aren't CDC formats). With this fix, any downstream
  sink on a PK'd KafkaSource correctly propagates retract and emits
  the right `upsert-kafka` DDL.

**Template** — `src/cli/templates/ride-sharing.ts`:

- Added the missing `zoneId` column to `RideRequestSchema` (the
  template aggregated by `zoneId` but the schema never declared it).

## Verification
```bash
REQUIRE_SQL_GATEWAY=1 pnpm vitest run \
  src/cli/templates/__tests__/template-explain.test.ts -t rides-surge-pricing
```
Passes. `rides-surge-pricing` removed from `SKIP`.

## Snapshots Rebaselined
The changelog-mode fix surfaces as upsert-kafka + `PRIMARY KEY …`
clauses on sinks downstream of PK'd KafkaSources. 10 snapshots in
`src/codegen/__tests__/__snapshots__/*.snap` were refreshed. The
diffs encode the correct behaviour — those sinks were previously
emitted as append-only `kafka` connectors, which Flink would have
rejected at runtime for retract-producing pipelines.
