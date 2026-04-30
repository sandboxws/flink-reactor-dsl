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

  environments: {
    // \`pnpm fr sim up\` provisions Fluss + Paimon catalogs and the
    // \`benchmark\` database in each, zero-config. The same \`sim.init\`
    // block also drives the docker-compose lane via
    // \`pnpm fr cluster up --runtime=docker\`.
    test: {
      runtime: 'minikube',
      sim: {
        init: {
          fluss: { databases: ['benchmark'] },
          paimon: { databases: ['benchmark'] },
        },
      },
      pipelines: {
        '*': {
          parallelism: 4,
          checkpoint: { interval: '30s', mode: 'exactly-once' },
        },
      },
    },
    development: {
      cluster: { url: 'http://localhost:8081' },
      sim: {
        init: {
          fluss: { databases: ['benchmark'] },
          paimon: { databases: ['benchmark'] },
        },
      },
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
  bootstrapServers: 'localhost:9123',
})

const source = PostgresCdcPipelineSource({
  hostname: 'pg-primary',
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
  database: 'benchmark',
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
  bootstrapServers: 'localhost:9123',
})

const paimon = PaimonCatalog({
  name: 'paimon_catalog',
  warehouse: 's3a://benchmark/paimon',
})

const source = FlussSource({
  catalog: fluss.handle,
  database: 'benchmark',
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
  database: 'benchmark',
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
        └── FlussCatalog (fluss_catalog @ localhost:9123)
              └── FlussSink (benchmark.orders, PK o_orderkey, 8 buckets)`,
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
  └── FlussSource (benchmark.orders, scanStartupMode='initial')
        └── Filter (o_orderstatus = 'O')
              └── PaimonCatalog (paimon_catalog @ s3a://benchmark/paimon)
                    └── PaimonSink (benchmark.orders, deduplicate, 8 buckets)`,
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
    // Hand-written rather than `templateReadme()` because pg-fluss-paimon is
    // the only template that ships zero-config sim provisioning, so it has
    // a custom Quickstart + "What `fr sim up` provisions" section the
    // generic helper doesn't model.
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
pnpm fr sim up
pnpm fr deploy ingest && pnpm fr deploy serve
\`\`\`

\`pnpm fr sim up\` provisions the entire runtime against minikube — Fluss, Paimon, Postgres, and the catalogs the pipelines write into. Prefer docker-compose? Run \`pnpm fr cluster up --runtime=docker\` instead; the same \`sim.init\` block in \`flink-reactor.config.ts\` provisions the equivalent resources via the Docker-lane SQL gateway.

### What \`fr sim up\` provisions

- **Fluss catalog** \`fluss_catalog\` with the \`benchmark\` database — the upsert-state target for the ingest pipeline.
- **Paimon catalog** \`paimon_catalog\` with the \`benchmark\` database — the OLAP serving sink for the serve pipeline.
- **Postgres** \`tpch\` database seeded with TPC-H SF=0.1 data (orders, lineitem, customer).
- **Postgres** \`flink_cdc\` user with \`REPLICATION\` + \`LOGIN\` and the \`flink_cdc_pub\` publication on the TPC-H tables — everything the CDC pipeline source needs.

No manual \`CREATE CATALOG\` / \`CREATE DATABASE\` SQL is required before \`pnpm fr deploy\` succeeds.

## Prerequisites

- Postgres with \`wal_level=logical\`, a \`flink_cdc\` user with \`REPLICATION\` and read on the published tables, and a Kubernetes Secret (or \`.env\` entry) supplying \`PG_PRIMARY_PASSWORD\`.
- Apache Fluss cluster reachable at the configured \`bootstrapServers\` (default \`localhost:9123\` — override via your environment).
- MinIO (or any S3-compatible store) hosting the Paimon warehouse at \`PAIMON_WAREHOUSE\` (default \`s3a://benchmark/paimon\`). \`MINIO_ACCESS_KEY\` / \`MINIO_SECRET_KEY\` are forwarded to the Paimon catalog by the platform-level config.

## Getting Started

\`\`\`bash
cp .env.example .env   # then fill in PG_PRIMARY_PASSWORD and the MinIO creds
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

# MinIO (or any S3-compatible store) credentials for the Paimon warehouse
MINIO_ACCESS_KEY=
MINIO_SECRET_KEY=

# Fluss coordinator bootstrap servers (default: localhost:9123)
FLUSS_BOOTSTRAP_SERVERS=localhost:9123

# Paimon warehouse URI (default: s3a://benchmark/paimon)
PAIMON_WAREHOUSE=s3a://benchmark/paimon
`,
    },
  ]
}
