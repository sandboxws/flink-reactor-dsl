# BUG-016: Iceberg REST catalog pipelines fail end-to-end EXPLAIN [FIXED]

## Affected Templates/Pipelines
- `cdc-lakehouse/cdc-to-lakehouse`
- `lakehouse-analytics/medallion-bronze`
- `lakehouse-analytics/medallion-silver`
- `lakehouse-analytics/medallion-gold`
- (`lakehouse-ingestion/lakehouse-ingest` — surfaces a different bug, see Note)

Surfaced by `src/cli/templates/__tests__/template-explain.test.ts` running
the synthesized SQL for these pipelines against the local Flink SQL Gateway.

## Flink Errors (three layers, fixed in order)

### Layer 1 — JAR version incompatibility
```
Could not find any factory for identifier 'iceberg' that implements
'org.apache.flink.table.factories.CatalogFactory' in the classpath.
Available factory identifiers are: generic_in_memory, jdbc
```
Root cause: `Dockerfile.flink` downloaded `iceberg-flink-runtime-1.20-1.8.1.jar`
(Flink 1.20 ABI), but the cluster runs `flink:2.0.1`. The factory class
silently fails to register because its target API surface no longer matches.

### Layer 2 — Hadoop classpath missing
```
java.lang.NoClassDefFoundError: org/apache/hadoop/conf/Configuration
  at org.apache.iceberg.flink.FlinkCatalogFactory.clusterHadoopConf(FlinkCatalogFactory.java:214)
```
Root cause: `iceberg-flink-runtime` requires Hadoop classes for
`Configuration` even when using a REST catalog with S3FileIO (no HDFS in
the path). The `iceberg-aws-bundle` does not bring Hadoop along.

### Layer 3 — IcebergSink emits a comment instead of CREATE TABLE
```
Cannot find table '`lakehouse`.`bronze`.`orders_raw`' in any of the catalogs
[lakehouse, default_catalog], nor as a temporary table.
```
Root cause: `src/codegen/sql-generator.ts:1156-1158` short-circuited to a
`-- (catalog-managed)` comment for any sink with `props.catalogName`. A fresh
REST catalog has no databases or tables, so the subsequent `INSERT` had
nowhere to write.

## Where Fixed

### Layer 1 + 2 — `src/cli/cluster/Dockerfile.flink`
- Replaced `iceberg-flink-runtime-1.20:1.8.1` with
  `iceberg-flink-runtime-2.0:1.10.1` (Flink-2.x-compatible artifact)
- Bumped `iceberg-aws-bundle` to `1.10.1` to match
- Added `hadoop-client-api:3.3.6` + `hadoop-client-runtime:3.3.6`
  (~35 MB combined; lighter than installing the full Hadoop tarball)

### Layer 3 — `src/codegen/sql-generator.ts`
- New `generateCatalogManagedSinkDdl` function emits
  `CREATE DATABASE IF NOT EXISTS <cat>.<db>` and
  `CREATE TABLE IF NOT EXISTS <cat>.<db>.<table> (cols [, PK]) [WITH (...)]`
  for `IcebergSink`s.
- `IF NOT EXISTS` keeps the emission idempotent across cluster restarts and
  across pipelines that share a database (e.g., `medallion-bronze` and
  `medallion-silver` both writing into the same catalog).
- Iceberg table properties (`format-version`, `write.upsert.enabled`,
  `equality-field-columns`, `commit-interval-ms`,
  `write.distribution-mode`, `write.target-file-size-bytes`,
  `write.parquet.compression-codec`) are emitted from the corresponding
  `IcebergSinkProps` fields when set.

## Verification
```bash
flink-reactor cluster down
# (rebuild image to pick up the new JARs)
docker compose -f src/cli/cluster/docker-compose.yml build jobmanager taskmanager-1 taskmanager-2 sql-gateway
flink-reactor cluster up
REQUIRE_SQL_GATEWAY=1 pnpm vitest run src/cli/templates/__tests__/template-explain.test.ts
```

**Result:** 4 of 5 advertised pipelines move from `↓` to `✓`:
- ✓ `cdc-to-lakehouse`
- ✓ `medallion-bronze`
- ✓ `medallion-silver`
- ✓ `medallion-gold`
- ⚠ `lakehouse-ingest` — see Note below

## Note on `lakehouse-ingest`
After A.2's fixes, `lakehouse-ingest` reaches a *different* error:
```
Column types of query result and sink for 'lakehouse.raw.clickstream' do not match.
Query schema: [eventId, userId, eventType, payload, eventTime]   ← EventSchema
Sink schema:  [sessionId, userId, pageUrl, ..., clickTime]       ← ClickstreamSchema
```
This is the same bug as **BUG-018 (multi-pair StatementSet type mismatch,
A.4)**: the second `(KafkaSource, IcebergSink)` pair in the StatementSet
reads the *first* pair's source schema. `lakehouse-ingest` is now grouped
with the `pump-*` pipelines in the SKIP set under that bucket; A.4 will
unblock it.

## Snapshot Rebaseline
6 snapshots in `src/__tests__/__snapshots__/benchmark-pipelines.test.ts.snap`
(the `pg-cdc-iceberg-f1` matrix: `json|avro|protobuf` × `throughput|latency`)
were rebaselined. The diff in each is exactly the comment → CREATE TABLE
emission for `lakekeeper.tpch.orders`.
