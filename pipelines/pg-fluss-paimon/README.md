# pg-fluss-paimon — Streaming OLAP via shared Fluss storage

Postgres → Apache Fluss → Flink SQL → Apache Paimon.

This is a **reference pipeline** — copy this directory when standing up the
"shared streaming storage" topology in your own project. Unlike the
`pg-cdc-iceberg-{f1,f2}` templates, this directory ships **two coupled entry
points** rather than alternative tracks:

- [`ingest.tsx`](ingest.tsx) — Postgres CDC ingest into a Fluss PrimaryKey table.
- [`serve.tsx`](serve.tsx) — Fluss → Flink SQL → Paimon serving job.

The two halves are tied by a shared Fluss table — one ingest job materializes
the upsert state once and any number of serve-side jobs can fan out from it
without re-running CDC against the source database.

> See also: the top-level [README → Reference Pipelines](../../README.md#reference-pipelines)
> section, the alternative [`pg-cdc-iceberg-{f1,f2}`](../) templates for a
> warehouse-style topology, and the [project CLAUDE.md](../../CLAUDE.md) for
> DSL-wide conventions.

## Topology

```
┌──────────┐   ┌──────────────────┐   ┌──────────┐   ┌────────────┐   ┌─────────┐
│ Postgres │ → │ Flink CDC 3.6    │ → │  Fluss   │ → │ Flink SQL  │ → │ Paimon  │
│ (WAL)    │   │ Pipeline Conn.   │   │ PK table │   │ serve job  │   │ (LSM)   │
└──────────┘   └──────────────────┘   └─────┬────┘   └────────────┘   └─────────┘
                                            │              ▲
                                            └──────────────┘
                                       any number of fan-out
                                       readers (other Flink
                                       SQL jobs, ML feature
                                       pipelines, ...)
```

Each Fluss PrimaryKey table is a bucketed LSM with sub-second freshness.
Reading from a PrimaryKey table emits a retract changelog, so serving-tier
sinks that want upsert semantics (Paimon `mergeEngine`, Iceberg `upsertEnabled`,
ClickHouse, etc.) plug in cleanly.

## When to use this vs `pg-cdc-iceberg`

| Use this when…                                                | Prefer `pg-cdc-iceberg` when…                       |
|---------------------------------------------------------------|------------------------------------------------------|
| You need sub-second OLAP fan-out from one CDC stream          | You're driving warehouse-style nightly batch reads   |
| Multiple downstream jobs read the same upsert state           | A single Iceberg table is the only consumer          |
| You want to bridge into ClickHouse / ML feature pipelines     | You're standardising on Iceberg as the lake format   |
| Bucketed LSM commits fit your latency budget                  | Compaction-leaning batch commits fit your budget     |

The two templates are not strict alternatives — running both against the same
Postgres source is a perfectly reasonable architecture (Iceberg for warehouse,
Fluss-as-bus for serving and ML).

## Parameters

`ingest.tsx` exports a parameterised function:

```ts
type IngestParams = {
  snapshotMode: "initial" | "never"          // → Flink CDC scan.snapshot.enabled
  commitMode:   "throughput" | "latency"     // → execution.checkpointing.interval (10s / 2s)
  parallelism?: number                       // default 4
  flinkVersion?: FlinkMajorVersion           // default derived from app config
  hostname?:           string                // default pg-primary
  database?:           string                // default tpch
  username?:           string                // default flink_cdc
  passwordSecretName?: string                // default pg-primary-password
  schemaList?:         readonly string[]     // default ['public']
  tableList?:          readonly string[]     // default ['public.orders', 'public.lineitem', 'public.customer']
  bootstrapServers?:   string                // default fluss-coordinator:9123
  chunkSize?:          number                // default 100_000
}
```

`serve.tsx` exports a parameterised function:

```ts
type ServeParams = {
  paimonMergeEngine: "deduplicate" | "partial-update"  // → Paimon WITH 'merge-engine' = ...
  commitMode:        "throughput" | "latency"          // → execution.checkpointing.interval (10s / 2s)
  parallelism?:      number                            // default 4
  flinkVersion?:     FlinkMajorVersion                 // default derived from app config
  bootstrapServers?: string                            // default fluss-coordinator:9123
  warehouse?:        string                            // default s3a://benchmark/paimon
}
```

## Sidecar requirements

- **Postgres** — replication slot configured, `wal_level=logical`. The
  `flink_cdc` user needs `REPLICATION` and read on the published tables. The
  `pg-primary-password` Kubernetes Secret must hold the password under key
  `password` (override via the `passwordSecretName` parameter).
- **Apache Fluss cluster** — coordinator + tablet servers reachable at
  `bootstrapServers`. Use the simulator infra that ships with
  `sink-fluss-phase-2` (port 9123 by default).
- **MinIO (or any S3-compatible object store)** — Paimon's warehouse backend.
  Bucket `benchmark` must exist; credentials are forwarded to the Paimon
  catalog by the platform-level config (not threaded via the DSL).

## Cross-pipeline contract

`ingest.tsx` and `serve.tsx` share a single Fluss cluster — the contract is
the `bootstrap.servers` value on the `FlussCatalog` (the same `fluss-coordinator:9123`
default in both files). Running them against different Fluss clusters
produces independent unrelated jobs.

The Flink CDC 3.6 Fluss Pipeline Connector auto-derives the Fluss table
identity from the upstream Postgres `tableList` — the `database` / `table`
props on `FlussSink` are recorded on the construct node but not surfaced into
the emitted Pipeline-YAML sink stanza. The serve pipeline reads from the
auto-derived table by name.

## Synthesize and apply

```bash
# From the flink-reactor-dsl repo root:

# Ingest:
pnpm flinkreactor synth \
  --pipeline pipelines/pg-fluss-paimon/ingest.tsx \
  --params '{"snapshotMode":"initial","commitMode":"throughput"}'

# Serve:
pnpm flinkreactor synth \
  --pipeline pipelines/pg-fluss-paimon/serve.tsx \
  --params '{"paimonMergeEngine":"deduplicate","commitMode":"throughput"}'

# Emitted artifacts (default location `dist/<pipeline-name>/`):
#   - sql/<pipeline-name>.sql            # serve.tsx only — Flink SQL DDL
#   - yaml/<pipeline-name>.yaml          # ingest.tsx only — Flink CDC pipeline YAML
#   - crd/<pipeline-name>.yaml           # FlinkDeployment CRD (both)
#   - jars.txt                           # resolved Maven coordinates (both)

kubectl apply -f dist/pg-fluss-paimon-ingest/crd/
kubectl apply -f dist/pg-fluss-paimon-serve/crd/
```

## Merge engine selection

`serve.tsx` parameterises on `paimonMergeEngine`. Pick the engine that matches
the workload:

| Merge engine     | When to choose                                                                |
|------------------|-------------------------------------------------------------------------------|
| `deduplicate`    | Standard upsert: latest record per primary key wins. Default for CDC mirrors. |
| `partial-update` | Sparse-column updates: incoming rows merge column-by-column into existing PK rows. Useful when joining multiple CDC streams with disjoint columns. |
| `first-row`      | Keep the first record per primary key (audit-log dedup). Available on `PaimonSink` directly; not exposed via this template's `ServeMergeEngine` parameter. |
| `aggregation`    | Pre-aggregated rollups via per-column aggregation functions. Configured on the table definition. Available on `PaimonSink` directly. |

The template's parameter type is restricted to `'deduplicate' | 'partial-update'`
because those are the two CDC-mirror engines. Add `'first-row'` /
`'aggregation'` to the union in `serve.tsx` if you need them.

## TPC-H schemas

The `schemas/` directory ships the three TPC-H tables used by the benchmark:

- `orders.ts`    — `OrdersSchema`   (PK `o_orderkey`)
- `lineitem.ts`  — `LineitemSchema` (PK `l_orderkey + l_linenumber`)
- `customer.ts`  — `CustomerSchema` (PK `c_custkey`)

The shipped `serve.tsx` wires `orders` only; copy the file and swap imports
to materialize the other two tables.
