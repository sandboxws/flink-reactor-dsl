import type { ScaffoldOptions, TemplateFile } from "@/cli/commands/new.js"
import {
  pipelineReadme,
  sharedFiles,
  templatePipelineTestStub,
} from "./shared.js"

export function getPgFlussPaimonTemplates(
  opts: ScaffoldOptions,
): TemplateFile[] {
  return [
    ...sharedFiles(opts),

    // Override the shared `flink-reactor.config.ts` to pin Flink 2.2 and
    // expose a 30s exactly-once checkpoint default (the topology assumes
    // CDC commits land on every checkpoint cycle).
    {
      path: "flink-reactor.config.ts",
      content: `import { defineConfig } from '@flink-reactor/dsl'

export default defineConfig({
  flink: { version: '2.2' },

  // Postgres for the CDC source (TPC-H), Fluss for streaming + Paimon
  // shared storage. No Kafka in this lane.
  services: { postgres: {}, fluss: {} },

  environments: {
    // The Docker (\`pnpm fr cluster up\`) and minikube (\`pnpm fr sim up\`) lanes
    // both ship a Postgres seeded with TPC-H data (\`tpch.public.{orders,
    // lineitem,customer}\`), the \`flink_cdc\` replication role, and the
    // \`flink_cdc_pub\` publication — everything the ingest pipeline needs.
    //
    // Fluss starts empty: the Flink CDC Pipeline Connector creates the
    // \`fluss_catalog.public.<table>\` entries on the first record, mirroring
    // upstream Postgres schemas. Paimon's serve pipeline emits
    // \`CREATE DATABASE/TABLE IF NOT EXISTS\` so its targets are self-bootstrapping.
    test: {
      runtime: 'minikube',
      pipelines: {
        '*': {
          parallelism: 4,
          checkpoint: { interval: '30s', mode: 'exactly-once' },
        },
      },
    },
    development: {
      cluster: { url: 'http://localhost:8081' },
      pipelines: {
        '*': {
          parallelism: 4,
          checkpoint: { interval: '30s', mode: 'exactly-once' },
        },
      },
    },
    production: {
      cluster: { url: 'https://flink-prod:8081' },
      kubernetes: { namespace: 'flink-prod' },
      pipelines: {
        '*': {
          parallelism: 4,
          checkpoint: { interval: '30s', mode: 'exactly-once' },
        },
      },
    },
  },
})
`,
    },

    // ── TPC-H schemas (ported verbatim from the in-tree reference pipeline) ──
    {
      path: "schemas/orders.ts",
      content: `import { Schema, Field } from '@flink-reactor/dsl'

export const OrdersSchema = Schema({
  fields: {
    o_orderkey: Field.BIGINT(),
    o_custkey: Field.BIGINT(),
    o_orderstatus: Field.STRING(),
    o_totalprice: Field.DECIMAL(15, 2),
    o_orderdate: Field.DATE(),
    o_orderpriority: Field.STRING(),
    o_clerk: Field.STRING(),
    o_shippriority: Field.INT(),
    o_comment: Field.STRING(),
    event_time: Field.TIMESTAMP(3),
  },
  primaryKey: { columns: ['o_orderkey'] },
  watermark: {
    column: 'event_time',
    expression: "event_time - INTERVAL '5' SECOND",
  },
})
`,
    },
    {
      path: "schemas/lineitem.ts",
      content: `import { Schema, Field } from '@flink-reactor/dsl'

export const LineitemSchema = Schema({
  fields: {
    l_orderkey: Field.BIGINT(),
    l_partkey: Field.BIGINT(),
    l_suppkey: Field.BIGINT(),
    l_linenumber: Field.INT(),
    l_quantity: Field.DECIMAL(15, 2),
    l_extendedprice: Field.DECIMAL(15, 2),
    l_discount: Field.DECIMAL(15, 2),
    l_tax: Field.DECIMAL(15, 2),
    l_returnflag: Field.STRING(),
    l_linestatus: Field.STRING(),
    l_shipdate: Field.DATE(),
    l_commitdate: Field.DATE(),
    l_receiptdate: Field.DATE(),
    l_shipinstruct: Field.STRING(),
    l_shipmode: Field.STRING(),
    l_comment: Field.STRING(),
    event_time: Field.TIMESTAMP(3),
  },
  primaryKey: { columns: ['l_orderkey', 'l_linenumber'] },
  watermark: {
    column: 'event_time',
    expression: "event_time - INTERVAL '5' SECOND",
  },
})
`,
    },
    {
      path: "schemas/customer.ts",
      content: `import { Schema, Field } from '@flink-reactor/dsl'

export const CustomerSchema = Schema({
  fields: {
    c_custkey: Field.BIGINT(),
    c_name: Field.STRING(),
    c_address: Field.STRING(),
    c_nationkey: Field.BIGINT(),
    c_phone: Field.STRING(),
    c_acctbal: Field.DECIMAL(15, 2),
    c_mktsegment: Field.STRING(),
    c_comment: Field.STRING(),
    event_time: Field.TIMESTAMP(3),
  },
  primaryKey: { columns: ['c_custkey'] },
  watermark: {
    column: 'event_time',
    expression: "event_time - INTERVAL '5' SECOND",
  },
})
`,
    },

    // ── Pipeline 1: Postgres CDC → Fluss PrimaryKey table ──────────────────
    {
      path: "pipelines/ingest/index.tsx",
      content: `import {
  FlussCatalog,
  FlussSink,
  Pipeline,
  PostgresCdcPipelineSource,
  secretRef,
} from '@flink-reactor/dsl'

const catalog = FlussCatalog({
  name: 'fluss_catalog',
  // In-cluster DNS for the Fluss coordinator. Both the Docker-compose
  // lane (service \`fluss-coordinator\`) and the minikube lane (Service
  // of the same name in the cluster's default namespace) resolve to this
  // address. Using \`localhost\` here would point at the Flink JobManager's
  // own loopback and fail with "Connection refused" at deploy time.
  bootstrapServers: 'fluss-coordinator:9123',
})

const source = PostgresCdcPipelineSource({
  // In-cluster DNS for the Postgres service. The compose service is
  // \`postgres\` (\`src/cli/cluster/docker-compose.yml\`) and the minikube
  // Service has the same name (\`src/cli/sim/manifests/03-postgres.yaml\`).
  hostname: 'postgres',
  port: 5432,
  database: 'tpch',
  username: 'flink_cdc',
  password: secretRef('PG_PRIMARY_PASSWORD'),
  schemaList: ['public'],
  tableList: ['public.orders', 'public.lineitem', 'public.customer'],
  snapshotMode: 'initial',
  chunkSize: 100_000,
})

const sink = FlussSink({
  catalog: catalog.handle,
  // Flink CDC 3.6's Fluss pipeline connector mirrors upstream
  // \`<schema>.<table>\` into Fluss \`<database>.<table>\` — Postgres
  // \`public.orders\` lands as \`fluss_catalog.public.orders\`. The
  // \`database\`/\`table\` props here document that contract for the
  // construct tree but are not surfaced into the emitted Pipeline-YAML
  // (the connector auto-derives from the upstream \`tableList\`).
  database: 'public',
  table: 'orders',
  primaryKey: ['o_orderkey'],
  buckets: 8,
  children: [source],
})

export default (
  <Pipeline
    name="ingest"
    parallelism={4}
    checkpoint={{ interval: '10s', mode: 'exactly-once' }}
  >
    {catalog.node}
    {sink}
  </Pipeline>
)
`,
    },

    // ── Pipeline 2: Fluss → filter → Paimon (open-orders serving sink) ────
    {
      path: "pipelines/serve/index.tsx",
      content: `import {
  Filter,
  FlussCatalog,
  FlussSource,
  PaimonCatalog,
  PaimonSink,
  Pipeline,
} from '@flink-reactor/dsl'
import { OrdersSchema } from '@/schemas/orders'

const fluss = FlussCatalog({
  name: 'fluss_catalog',
  // See pipelines/ingest/index.tsx for why this is the in-cluster DNS
  // name and not \`localhost\`.
  bootstrapServers: 'fluss-coordinator:9123',
})

const paimon = PaimonCatalog({
  name: 'paimon_catalog',
  // \`s3://\` (not \`s3a://\`) routes through Paimon's bundled S3FileIO
  // (provided by the \`paimon-s3\` plugin in /opt/flink/lib). \`s3a://\`
  // would delegate to Hadoop's S3AFileSystem, which isn't on the user
  // classpath and fails with ClassNotFoundException at deploy time.
  //
  // The bucket \`flink-state\` is created once by seaweedfs-init in both
  // lanes; warehouses live as sub-prefixes inside it. Matches the default
  // in \`src/cli/cluster/paimon-init.ts\` so cluster-init and pipeline
  // deploys agree on the physical warehouse path.
  warehouse: 's3://flink-state/paimon',
  // SeaweedFS in-cluster endpoint. The compose service is \`seaweedfs\`
  // (\`docker-compose.yml\`) with a network alias of
  // \`seaweedfs.flink-demo.svc\` so the same address works in the
  // minikube lane (where it's the real K8s Service DNS).
  s3Endpoint: 'http://seaweedfs.flink-demo.svc:8333',
  s3AccessKey: 'admin',
  s3SecretKey: 'admin',
  s3PathStyleAccess: true,
})

const source = FlussSource({
  catalog: fluss.handle,
  // Reads the table the ingest pipeline auto-creates by mirroring the
  // upstream Postgres \`public\` schema — see the FlussSink comment in
  // pipelines/ingest/index.tsx.
  database: 'public',
  table: 'orders',
  schema: OrdersSchema,
  primaryKey: ['o_orderkey'],
  scanStartupMode: 'initial',
})

const openOrders = Filter({
  condition: "o_orderstatus = 'O'",
  children: [source],
})

const sink = PaimonSink({
  catalog: paimon.handle,
  // Self-bootstrapping: the serve SQL emits \`CREATE DATABASE/TABLE
  // IF NOT EXISTS\`, so no pre-provisioning is needed. \`public\` mirrors
  // the Fluss source database so the lake-side and stream-side names
  // line up — pick anything that fits your domain.
  database: 'public',
  table: 'orders',
  primaryKey: ['o_orderkey'],
  mergeEngine: 'deduplicate',
  changelogProducer: 'input',
  bucket: 8,
  children: [openOrders],
})

export default (
  <Pipeline
    name="serve"
    parallelism={4}
    checkpoint={{ interval: '10s', mode: 'exactly-once' }}
  >
    {fluss.node}
    {paimon.node}
    {sink}
  </Pipeline>
)
`,
    },

    // ── Per-pipeline READMEs (7-section structure) ─────────────────────────
    pipelineReadme({
      pipelineName: "ingest",
      tagline:
        "Postgres CDC (Flink CDC 3.6 Pipeline Connector) into a Fluss PrimaryKey table — the shared upsert state any number of serve-side jobs can fan out from.",
      demonstrates: [
        "`<PostgresCdcPipelineSource>` — Flink CDC 3.6 Pipeline Connector consuming a Postgres logical replication slot.",
        "`<FlussCatalog>` declaration plus `<FlussSink>` writing to a bucketed Fluss PrimaryKey table.",
        "Secret indirection via `secretRef('PG_PRIMARY_PASSWORD')` — the password is pulled from the runtime environment, never inlined.",
      ],
      topology: `Postgres (WAL)
  └── PostgresCdcPipelineSource (public.orders, lineitem, customer)
        └── FlussCatalog (fluss_catalog @ fluss-coordinator:9123)
              └── FlussSink (public.orders, PK o_orderkey, 8 buckets)`,
      schemas: [
        "`schemas/orders.ts` — TPC-H `orders` (PK `o_orderkey`)",
        "`schemas/lineitem.ts` — TPC-H `lineitem` (PK `l_orderkey + l_linenumber`)",
        "`schemas/customer.ts` — TPC-H `customer` (PK `c_custkey`)",
      ],
      runCommand: `pnpm synth
pnpm test`,
      translationNotes:
        "The Flink CDC Pipeline Connector auto-derives Fluss table identity from the upstream `tableList`, so `database`/`table` props on `<FlussSink>` are recorded on the construct node but not surfaced into the emitted Pipeline-YAML sink stanza.",
    }),
    pipelineReadme({
      pipelineName: "serve",
      tagline:
        "Read the Fluss PrimaryKey table as a retract changelog, filter to open orders, and materialize into Apache Paimon with a `deduplicate` merge engine.",
      demonstrates: [
        "`<FlussSource>` consuming a PrimaryKey table — emits a retract changelog suitable for upsert sinks.",
        "`<Filter>` predicate pushed through to the Flink SQL `WHERE` clause.",
        "`<PaimonSink>` with `mergeEngine: 'deduplicate'` and `changelogProducer: 'input'` — sub-second OLAP fan-out without re-running CDC against Postgres.",
      ],
      topology: `FlussCatalog (fluss_catalog)
  └── FlussSource (public.orders, scanStartupMode='initial')
        └── Filter (o_orderstatus = 'O')
              └── PaimonCatalog (paimon_catalog @ s3://flink-state/paimon)
                    └── PaimonSink (public.orders, deduplicate, 8 buckets)`,
      schemas: [
        "`schemas/orders.ts` — same `OrdersSchema` consumed by the ingest pipeline",
      ],
      runCommand: `pnpm synth
pnpm test`,
      translationNotes:
        "Swap `mergeEngine: 'deduplicate'` for `'partial-update'` if multiple CDC streams with disjoint columns merge into the same Paimon row. `'first-row'` and `'aggregation'` engines are also available on `<PaimonSink>` directly — see the docs.",
    }),

    // ── Per-pipeline snapshot test stubs ───────────────────────────────────
    templatePipelineTestStub({
      pipelineName: "ingest",
      loadBearingPatterns: [/fluss/i, /postgres-cdc/i, /benchmark/i],
    }),
    templatePipelineTestStub({
      pipelineName: "serve",
      loadBearingPatterns: [
        /fluss/i,
        /paimon/i,
        /'merge-engine'\s*=\s*'deduplicate'/i,
      ],
    }),

    // ── Project-root README ─────────────────────────────────────────────────
    //
    // Hand-written rather than \`templateReadme()\` because pg-fluss-paimon
    // documents the lake-side topology (Fluss + Paimon) and the in-cluster
    // service DNS the pipelines depend on — neither of which the generic
    // helper models.
    {
      path: "README.md",
      content: `# Pg Fluss Paimon

Postgres → Apache Fluss → Flink SQL → Apache Paimon. A shared streaming-storage topology: one ingest job materializes the upsert state into a Fluss PrimaryKey table; any number of serve-side jobs fan out from it without re-running CDC against the source database.

## Pipelines

- **ingest** — Postgres CDC (Flink CDC 3.6 Pipeline Connector) → Fluss PrimaryKey table (shared upsert state).
- **serve** — Fluss → Filter (open orders) → Paimon \`deduplicate\` merge engine for sub-second OLAP fan-out.

## Quickstart

Three commands take you from a fresh checkout to a running pipeline:

\`\`\`bash
pnpm install
pnpm fr cluster up --runtime=docker     # or: pnpm fr sim up   (minikube)
pnpm fr deploy ingest && pnpm fr deploy serve
\`\`\`

The Docker and minikube lanes both ship a Postgres seeded with TPC-H data plus the \`flink_cdc\` replication role + publication, a Fluss cluster, a Paimon warehouse on SeaweedFS, and a Flink runtime — no manual setup.

### What the runtime ships

- **Postgres** \`tpch\` database seeded with TPC-H SF=0.1 data (\`public.orders\`, \`public.lineitem\`, \`public.customer\`).
- **Postgres** \`flink_cdc\` user with \`REPLICATION\` + \`LOGIN\` and the \`flink_cdc_pub\` publication on those three tables.
- **Fluss catalog** \`fluss_catalog\`. Empty at boot — the ingest pipeline's CDC connector creates \`fluss_catalog.public.<table>\` entries on the first record by mirroring the upstream Postgres schema names.
- **Paimon warehouse** at \`s3://flink-state/paimon\` (SeaweedFS-backed). The serve pipeline emits \`CREATE DATABASE/TABLE IF NOT EXISTS\`, so its targets are self-bootstrapping.

No manual \`CREATE CATALOG\` / \`CREATE DATABASE\` SQL is required before \`pnpm fr deploy\` succeeds.

> **Note on deploy ordering.** The serve pipeline references \`fluss_catalog.public.orders\` via a SQL \`LIKE\` clause that resolves at submit time. Deploy ingest first and wait until it has emitted at least one record (the CDC initial-snapshot phase) before deploying serve, otherwise serve will fail planning with "Source table … not found".

## Prerequisites

- Postgres with \`wal_level=logical\`, a \`flink_cdc\` user with \`REPLICATION\` and read on the published tables, and a Kubernetes Secret (or \`.env\` entry) supplying \`PG_PRIMARY_PASSWORD\`.
- Apache Fluss cluster reachable at the configured \`bootstrapServers\`. The pipeline TSX files use the in-cluster DNS name \`fluss-coordinator:9123\` (the compose service / k8s Service name), which is what the Flink JobManager uses to dial Fluss. From your host shell you can still reach the same coordinator at \`localhost:9123\` via the published port.
- An S3-compatible store hosting the Paimon warehouse at \`s3://flink-state/paimon\` (the \`flink-state\` bucket is created by \`fr cluster up\` / \`fr sim up\` via SeaweedFS; the \`paimon\` sub-prefix matches the cluster-init default in \`src/cli/cluster/paimon-init.ts\`). The Paimon catalog DDL hardcodes \`admin\`/\`admin\` credentials against the local store; swap to your own warehouse + credentials by editing \`paimon-init.ts\` for the local lane or the Paimon DDL in \`pipelines/serve/index.tsx\` for production.

## Getting Started

\`\`\`bash
cp .env.example .env   # then fill in PG_PRIMARY_PASSWORD
pnpm install
pnpm synth             # Preview generated SQL + Pipeline YAML
pnpm test              # Run the snapshot suite
\`\`\`
`,
    },

    // ── Environment scaffolding (.env.example) ─────────────────────────────
    //
    // The shared `.gitignore` already excludes `.env` and `.env.local`, so the
    // example file ships safely while real secrets stay out of git.
    {
      path: ".env.example",
      content: `# Postgres CDC user password — referenced from pipelines/ingest/index.tsx via secretRef()
PG_PRIMARY_PASSWORD=
`,
    },
  ]
}
