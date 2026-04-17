# pg-cdc-iceberg-f1 — Kafka-hop reference pipeline

Postgres → Debezium → Kafka → Flink SQL → Iceberg.

This is a **reference pipeline** — copy it when standing up the same topology in
your own project. It is also the F1 track of the Postgres → Iceberg CDC
benchmark plan: the benchmark harness imports this file directly so that the
numbers measure exactly what users would run in production.

> See also: the top-level [README → Reference Pipelines](../../README.md#reference-pipelines)
> section, the companion [F2 Pipeline-Connector pipeline](../pg-cdc-iceberg-f2/),
> and the [project CLAUDE.md](../../CLAUDE.md) for DSL-wide conventions.

## Topology

```
┌──────────┐   ┌──────────────┐   ┌───────┐   ┌──────────┐   ┌─────────┐
│ Postgres │ → │ Debezium     │ → │ Kafka │ → │ Flink SQL│ → │ Iceberg │
│ (WAL)    │   │ (Connect)    │   │ topic │   │  DSL job │   │ (MoR v2)│
└──────────┘   └──────────────┘   └───────┘   └──────────┘   └─────────┘
```

Debezium runs outside the Flink job (a separate Kafka Connect cluster). The DSL
job only reads the changelog topic. This decouples source-side capture from
stream processing — useful when the same CDC stream fans out to more than one
downstream job.

## Parameters

```ts
type F1Params = {
  wireFormat: "json" | "avro" | "protobuf"  // → debezium-json / -avro / -protobuf
  commitMode: "throughput" | "latency"       // → Iceberg commit-interval-ms
  parallelism?: number                       // default 4
  flinkVersion?: FlinkMajorVersion           // default derived from app config
  bootstrapServers?: string                  // default kafka:9092
  schemaRegistryUrl?: string                 // required for avro / protobuf
  catalogUri?: string                        // default http://lakekeeper:8181/catalog
  topic?: string                             // default tpch.public.orders
}
```

`commitMode` maps to Iceberg's `commit-interval-ms`:

| commitMode   | commit interval |
|--------------|-----------------|
| `throughput` | 10 s            |
| `latency`    | 2 s             |

## Sidecar requirements

- **Kafka** — brokers reachable at `bootstrapServers`. The topic is expected to
  be populated by an external Debezium Postgres connector (outside the scope of
  this pipeline).
- **Confluent Schema Registry** — required when `wireFormat` is `avro` or
  `protobuf`. Pass the endpoint via `schemaRegistryUrl`, or set
  `SCHEMA_REGISTRY_URL` in the caller environment and thread it through. The
  Flink deserializer fetches the writer schema at runtime.
- **Lakekeeper** — Iceberg REST catalog endpoint at `catalogUri`.
- **MinIO (or any S3-compatible object store)** — Lakekeeper's warehouse
  backend. Bucket credentials are configured inside Lakekeeper, not here.

## Synthesize and apply

```bash
# From the flink-reactor-dsl repo root:
pnpm flinkreactor synth \
  --pipeline pipelines/pg-cdc-iceberg-f1 \
  --params '{"wireFormat":"json","commitMode":"throughput"}'

# Emitted artifacts (default location `dist/pg-cdc-iceberg-f1/`):
#   - sql/pg-cdc-iceberg-f1.sql         # CREATE TABLE DDL + INSERT INTO
#   - crd/pg-cdc-iceberg-f1.yaml        # FlinkDeployment CRD
#   - jars.txt                          # resolved Maven coordinates

kubectl apply -f dist/pg-cdc-iceberg-f1/crd/
```

## TPC-H schemas

The `schemas/` directory ships the three TPC-H tables used by the benchmark:

- `orders.ts`    — `OrdersSchema`   (PK `o_orderkey`)
- `lineitem.ts`  — `LineitemSchema` (PK `l_orderkey + l_linenumber`)
- `customer.ts`  — `CustomerSchema` (PK `c_custkey`)

The default F1 pipeline wires `orders`. To run against `lineitem` or
`customer`, copy `index.tsx` and swap the schema import and `topic` /
`table` / `primaryKey` props accordingly. The F2 pipeline
(`../pg-cdc-iceberg-f2/`) uses all three via the Pipeline Connector's
`tableList`.
