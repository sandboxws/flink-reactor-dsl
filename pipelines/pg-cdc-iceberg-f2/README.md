# pg-cdc-iceberg-f2 — Pipeline Connector reference pipeline

Postgres → Flink CDC 3.6 Pipeline Connector → Iceberg.

This is a **reference pipeline** — copy it when standing up the same topology
in your own project. It is also the F2 track of the Postgres → Iceberg CDC
benchmark plan: the benchmark harness imports this file directly so that the
numbers measure exactly what users would run in production.

> See also: the top-level [README → Reference Pipelines](../../README.md#reference-pipelines)
> section, the companion [F1 Kafka-hop pipeline](../pg-cdc-iceberg-f1/), and
> the [project CLAUDE.md](../../CLAUDE.md) for DSL-wide conventions.

## Topology

```
┌──────────┐   ┌──────────────────────────────────┐   ┌─────────┐
│ Postgres │ → │ Flink CDC 3.6 Pipeline Connector │ → │ Iceberg │
│ (WAL)    │   │  (pipeline.yaml inside job)      │   │ (MoR v2)│
└──────────┘   └──────────────────────────────────┘   └─────────┘
```

No external Debezium cluster, no Kafka hop. The Pipeline Connector runs
replication-slot / publication-based CDC directly inside the Flink job and
emits to the Iceberg sink via the CDC runtime. The DSL synthesizes a
`pipeline.yaml` (mounted as a ConfigMap) and a `FlinkDeployment` that launches
`flink-cdc-cli.jar` with the YAML on its classpath.

## Parameters

```ts
type F2Params = {
  snapshotMode: "initial" | "never" | "initial_only"
  commitMode:   "throughput" | "latency"
  parallelism?: number                    // default 4
  flinkVersion?: FlinkMajorVersion
  hostname?: string                       // default pg-primary
  database?: string                       // default tpch
  username?: string                       // default flink_cdc
  passwordSecretName?: string             // default pg-primary-password
  schemaList?: readonly string[]          // default ["public"]
  tableList?: readonly string[]           // default ["public.orders", "public.lineitem", "public.customer"]
  catalogUri?: string                     // default http://lakekeeper:8181/catalog
  chunkSize?: number                      // default 100_000
}
```

`commitMode` maps to Iceberg's `commit-interval-ms`:

| commitMode   | commit interval |
|--------------|-----------------|
| `throughput` | 10 s            |
| `latency`    | 2 s             |

## Replication slot & publication lifecycle

The Pipeline Connector needs a **replication slot** and a **publication** on
the Postgres primary. Both are **user-managed** by default — the DSL never
runs `CREATE PUBLICATION` or `CREATE_REPLICATION_SLOT` for you. Before
deploying this pipeline, your DBA should:

```sql
-- 1. Create the publication covering every table in tableList
CREATE PUBLICATION fr_pg_cdc_iceberg_f2_pub
  FOR TABLE public.orders, public.lineitem, public.customer;

-- 2. Create the logical replication slot using pgoutput
SELECT pg_create_logical_replication_slot(
  'fr_pg_cdc_iceberg_f2_slot',
  'pgoutput'
);
```

If you omit `replicationSlotName` / `publicationName` the DSL derives
deterministic names from the pipeline name (`fr_pg_cdc_iceberg_f2_slot` and
`fr_pg_cdc_iceberg_f2_pub`) — use those exact names in the DDL above.

### `slotDropOnStop`

| snapshotMode    | slotDropOnStop | Rationale |
|-----------------|----------------|-----------|
| `initial`       | `false`        | Long-running streaming job — keeping the slot lets the next run rejoin the WAL at the low-water-mark. |
| `never`         | `false`        | Same as above. |
| `initial_only`  | `true`         | One-shot backfill — terminates after snapshot, so the slot would otherwise leak and accumulate WAL forever. |

The pipeline wires `slotDropOnStop: true` automatically when
`snapshotMode === 'initial_only'`.

## Emitted artifacts

Unlike a Flink-SQL pipeline, this one emits **three** artifacts per
synthesis:

1. **`pipeline.yaml`** — the Flink CDC pipeline document with
   `source:` (postgres), `sink:` (iceberg, MoR), and `pipeline:` stanzas.
2. **ConfigMap** — wraps `pipeline.yaml` so the FlinkDeployment can mount it.
3. **FlinkDeployment CRD** — launches `flink-cdc-cli.jar` with the ConfigMap
   mounted; the `spec.job.jarURI` is the CDC CLI, not the SQL runner.

The resolved jar list pins:
- `flink-cdc-pipeline-connector-postgres:3.6.0`
- `iceberg-flink-runtime-<flink-major>:1.6.0`

## Sidecar requirements

- **Postgres** — WAL-based logical replication enabled
  (`wal_level = logical`), slot and publication created as above.
- **Lakekeeper** — Iceberg REST catalog endpoint at `catalogUri`.
- **MinIO (or any S3-compatible object store)** — Lakekeeper's warehouse
  backend. Bucket credentials are configured inside Lakekeeper, not here.

## Synthesize and apply

```bash
pnpm flinkreactor synth \
  --pipeline pipelines/pg-cdc-iceberg-f2 \
  --params '{"snapshotMode":"initial","commitMode":"throughput"}'

# Emitted artifacts (default location `dist/pg-cdc-iceberg-f2/`):
#   - pipeline.yaml                     # CDC pipeline document
#   - crd/pg-cdc-iceberg-f2.yaml        # FlinkDeployment CRD (mounts ConfigMap)
#   - crd/configmap.yaml                # holds pipeline.yaml
#   - jars.txt                          # resolved Maven coordinates

kubectl apply -f dist/pg-cdc-iceberg-f2/crd/
```

## One-shot backfills (`snapshotMode: 'initial_only'`)

`initial_only` turns the pipeline into a **bounded** job: the CRD's
`job.mode` is `batch`, the Flink job terminates once the snapshot completes,
and `slotDropOnStop: true` cleans up the replication slot automatically. Use
this mode for initial loads that don't need to transition into steady-state
streaming afterwards.
