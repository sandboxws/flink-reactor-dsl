<h1 align="center">FlinkReactor</h1>

<p align="center">
  <strong>Write streaming pipelines as TypeScript components. Compile to Flink SQL + Kubernetes CRDs.</strong>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/status-early%20alpha-orange" alt="early alpha" />
  <a href="https://www.npmjs.com/package/flink-reactor"><img src="https://img.shields.io/npm/v/flink-reactor?color=d97085&label=npm" alt="npm version" /></a>
  <a href="https://github.com/sandboxws/flink-reactor-dsl/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-BSL%201.1-blue" alt="license" /></a>
  <a href="https://github.com/sandboxws/flink-reactor-dsl"><img src="https://img.shields.io/github/stars/sandboxws/flink-reactor-dsl?style=social" alt="GitHub stars" /></a>
  <a href="https://github.com/sandboxws/flink-reactor-dsl/issues"><img src="https://img.shields.io/github/issues/sandboxws/flink-reactor-dsl" alt="open issues" /></a>
  <img src="https://img.shields.io/badge/TypeScript-strict-3178c6" alt="TypeScript strict" />
  <img src="https://img.shields.io/badge/Flink-1.20%20%7C%202.0%20%7C%202.1%20%7C%202.2-e6526f" alt="Flink versions" />
</p>

<p align="center">
  <a href="https://flink-reactor.dev">Documentation</a> &middot;
  <a href="#-quick-start">Quick Start</a> &middot;
  <a href="#-examples">Examples</a> &middot;
  <a href="https://github.com/sandboxws/flink-reactor-dsl/issues">Issues</a>
</p>

<br />

## <img src="assets/icons/triangle-alert.svg" width="24" height="24" style="vertical-align: middle; margin-bottom: 2px;"> The Problem

Building Apache Flink streaming pipelines means writing raw SQL strings, manually managing connector JARs, hand-crafting Kubernetes YAML, and losing all the type safety and tooling that modern TypeScript provides. There's no component model, no reusability, and no way to compose complex topologies without drowning in boilerplate.

## <img src="assets/icons/lightbulb.svg" width="24" height="24" style="vertical-align: middle; margin-bottom: 2px;"> The Solution

FlinkReactor lets you write streaming pipelines as **TypeScript components** using JSX syntax you already know. Your pipeline is a component tree that **synthesizes** to Flink SQL and Kubernetes FlinkDeployment CRDs — with full type safety, IDE autocomplete, and deterministic output.

**No runtime overhead.** FlinkReactor generates artifacts that Flink executes natively.

```
TSX Components  →  Construct Tree  →  Flink SQL + K8s CRDs  →  Flink Kubernetes Operator
```

<br />

## <img src="assets/icons/play.svg" width="24" height="24" style="vertical-align: middle; margin-bottom: 2px;"> See It in Action

**8 lines of TSX** replace **33 lines of raw SQL.** Here's a basic Kafka passthrough pipeline:

<table>
<tr>
<th>After — TSX (8 lines)</th>
<th>Before — Raw SQL (33 lines)</th>
</tr>
<tr>
<td>

```tsx
export default (
  <Pipeline name="simple-source-sink" parallelism={4}>
    <KafkaSource
      topic="user_events"
      bootstrapServers="kafka:9092"
      schema={UserEventSchema}
    />
    <KafkaSink topic="user_events_processed" />
  </Pipeline>
);
```

</td>
<td>

```sql
CREATE TABLE `user_events` (
  `event_id` STRING,
  `user_id` STRING,
  `event_type` STRING,
  `payload` STRING,
  `event_time` TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'user_events',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json',
  'scan.startup.mode' = 'latest-offset'
);

CREATE TABLE `user_events_processed` (
  `event_id` STRING,
  `user_id` STRING,
  `event_type` STRING,
  `payload` STRING,
  `event_time` TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'user_events_processed',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

INSERT INTO `user_events_processed`
SELECT * FROM `user_events`;
```

</td>
</tr>
</table>

Run `flink-reactor synth` and get production-ready Flink SQL + a Kubernetes `FlinkDeployment` CRD — ready for `kubectl apply`.

<br />

## <img src="assets/icons/rocket.svg" width="24" height="24" style="vertical-align: middle; margin-bottom: 2px;"> Quick Start

```bash
# Create a new project
npx create-fr-app my-pipeline
cd my-pipeline

# Install dependencies
pnpm install

# Generate SQL + CRDs from your pipeline
pnpm flink-reactor synth

# Validate the pipeline topology
pnpm flink-reactor validate

# Visualize the DAG
pnpm flink-reactor graph
```

<br />

## <img src="assets/icons/sparkles.svg" width="24" height="24" style="vertical-align: middle; margin-bottom: 2px;"> Features

### <img src="assets/icons/workflow.svg" width="20" height="20" style="vertical-align: middle; margin-bottom: 2px;"> Pipeline DSL

| Component          | What it does                                                                                              |
| ------------------ | --------------------------------------------------------------------------------------------------------- |
| **Sources**        | `KafkaSource`, `JdbcSource`, `GenericSource`, `CatalogSource` — declarative connectors with typed schemas |
| **Sinks**          | `KafkaSink`, `JdbcSink`, `FileSystemSink`, `PaimonSink`, `IcebergSink`, `GenericSink`                     |
| **Transforms**     | `Filter`, `Map`, `FlatMap`, `Aggregate`, `Union`, `Deduplicate`, `TopN`, `Route`                          |
| **Joins**          | `Join`, `TemporalJoin`, `LookupJoin`, `IntervalJoin` — all Flink join strategies                          |
| **Windows**        | `TumbleWindow`, `SlideWindow`, `SessionWindow` — TVF-based windowing                                      |
| **Catalogs**       | `PaimonCatalog`, `IcebergCatalog`, `HiveCatalog`, `JdbcCatalog` — catalog management as components        |
| **Escape Hatches** | `RawSQL`, `UDF`, `MatchRecognize` — drop to raw SQL when you need to                                      |

### <img src="assets/icons/terminal.svg" width="20" height="20" style="vertical-align: middle; margin-bottom: 2px;"> CLI

| Command                  | Description                                         |
| ------------------------ | --------------------------------------------------- |
| `flink-reactor new`      | Scaffold a new project with interactive prompts     |
| `flink-reactor synth`    | Synthesize pipelines to Flink SQL + CRDs            |
| `flink-reactor validate` | Validate pipeline topology (no cycles, no orphans)  |
| `flink-reactor graph`    | Visualize the pipeline DAG                          |
| `flink-reactor dev`      | Watch mode with hot-reload                          |
| `flink-reactor deploy`   | Apply CRDs to a Kubernetes cluster                  |
| `flink-reactor doctor`   | Diagnose environment (Java, Docker, kubectl, Flink) |
| `flink-reactor cluster`  | Local Flink cluster via Docker Compose              |

<br />

## <img src="assets/icons/code-xml.svg" width="24" height="24" style="vertical-align: middle; margin-bottom: 2px;"> Examples

FlinkReactor ships with 28 examples covering every component. Here are six that showcase the DSL's range — from a simple passthrough to a full lambda architecture.

### Simple — Kafka Passthrough

> [`01-simple-source-sink`](src/examples/01-simple-source-sink/after.tsx) — basic schema, source, and sink

```tsx
const UserEventSchema = Schema({
  fields: {
    event_id: Field.STRING(),
    user_id: Field.STRING(),
    event_type: Field.STRING(),
    payload: Field.STRING(),
    event_time: Field.TIMESTAMP(3),
  },
});

export default (
  <Pipeline name="simple-source-sink" parallelism={4}>
    <KafkaSource
      topic="user_events"
      bootstrapServers="kafka:9092"
      schema={UserEventSchema}
    />
    <KafkaSink topic="user_events_processed" />
  </Pipeline>
);
```

### Intermediate — Windowed Aggregation with Filter

> [`04-tumble-window`](src/examples/04-tumble-window/after.tsx) — tumble window + aggregation + filter

```tsx
export default (
  <Pipeline name="active-users-per-minute" parallelism={12}>
    <KafkaSource
      topic="clickstream"
      bootstrapServers="kafka:9092"
      schema={ClickstreamSchema}
    />
    <TumbleWindow size="1 minute" on="event_time">
      <Aggregate
        groupBy={["user_id"]}
        select={{
          user_id: "user_id",
          page_views: "COUNT(*)",
          unique_pages: "COUNT(DISTINCT page_url)",
        }}
      />
    </TumbleWindow>
    <Filter condition="page_views > 5" />
    <KafkaSink topic="active_users_per_minute" />
  </Pipeline>
);
```

### Intermediate — Interval Join

> [`06-interval-join`](src/examples/06-interval-join/after.tsx) — two-stream join with time interval

```tsx
const orders = (
  <KafkaSource
    topic="orders"
    bootstrapServers="kafka:9092"
    schema={OrderSchema}
  />
);
const shipments = (
  <KafkaSource
    topic="shipments"
    bootstrapServers="kafka:9092"
    schema={ShipmentSchema}
  />
);

export default (
  <Pipeline name="order-fulfillment" parallelism={8}>
    <IntervalJoin
      left={orders}
      right={shipments}
      on="order_id = order_id"
      interval={{ from: "order_time", to: "order_time + INTERVAL '7' DAY" }}
    />
    <Map
      select={{
        order_id: "order_id",
        user_id: "user_id",
        amount: "amount",
        carrier: "carrier",
        fulfillment_time: "ship_time - order_time",
      }}
    />
    <KafkaSink topic="order_fulfillment" />
  </Pipeline>
);
```

### Advanced — CEP Fraud Detection

> [`15-cep-fraud-detection`](src/examples/15-cep-fraud-detection/after.tsx) — `MATCH_RECOGNIZE` pattern matching

```tsx
export default (
  <Pipeline name="fraud-detection" parallelism={16}>
    <MatchRecognize
      input={transactions}
      partitionBy={["card_id"]}
      orderBy="transaction_time"
      pattern="A B+ C"
      after="NEXT_ROW"
      define={{
        A: "A.amount > 1000",
        B: "B.location <> A.location",
        C: "C.amount > 500 AND C.location <> B.location",
      }}
      measures={{
        card_id: "A.card_id",
        first_txn: "A.transaction_id",
        last_txn: "C.transaction_id",
        total_amount: "A.amount + SUM(B.amount) + C.amount",
        txn_count: "COUNT(B.*) + 2",
      }}
    />
    <Map
      select={{
        card_id: "card_id",
        first_txn: "first_txn",
        last_txn: "last_txn",
        total_amount: "total_amount",
        txn_count: "txn_count",
        fraud_type: "'RAPID_GEO_CHANGE'",
      }}
    />
    <KafkaSink topic="fraud_alerts" />
  </Pipeline>
);
```

### Advanced — Conditional Fan-Out

> [`21-branching-multi-sink`](src/examples/21-branching-multi-sink/after.tsx) — `Route` component with conditional sinks

```tsx
export default (
  <Pipeline name="order-routing" parallelism={16}>
    <KafkaSource
      topic="raw_orders"
      bootstrapServers="kafka:9092"
      schema={OrderSchema}
    />
    <Map
      select={{
        order_id: "order_id",
        customer_id: "customer_id",
        product_id: "product_id",
        total_amount: "quantity * unit_price",
        order_time: "order_time",
        region: "region",
        order_status: "order_status",
      }}
    />
    <Route>
      <Route.Branch condition="total_amount >= 1000">
        <KafkaSink topic="high_value_orders" />
      </Route.Branch>
      <Route.Branch condition="order_status = 'FAILED'">
        <KafkaSink topic="failed_orders_alerts" />
      </Route.Branch>
      <Route.Default>
        <TumbleWindow size="1 minute" on="order_time">
          <Aggregate
            groupBy={["region"]}
            select={{
              region: "region",
              revenue: "SUM(total_amount)",
              order_count: "COUNT(*)",
            }}
          />
        </TumbleWindow>
        <JdbcSink
          url="jdbc:postgresql://db:5432/analytics"
          table="regional_metrics_per_minute"
        />
      </Route.Default>
    </Route>
  </Pipeline>
);
```

### Advanced — Lambda Architecture

> [`24-lambda-architecture`](src/examples/24-lambda-architecture/after.tsx) — 4 sinks from 1 source: archive, real-time metrics, upsert, and alerts

```tsx
export default (
  <Pipeline name="clickstream-lambda" parallelism={24}>
    <KafkaSource
      topic="clickstream"
      bootstrapServers="kafka:9092"
      schema={ClickstreamSchema}
    />
    <Route>
      {/* Raw archive to data lake */}
      <Route.Branch condition="true">
        <FileSystemSink
          path="s3://data-lake/clickstream/raw/"
          format="parquet"
          partitionBy={["DATE(event_time)", "HOUR(event_time)"]}
          rollingPolicy={{ size: "256MB", interval: "10min" }}
        />
      </Route.Branch>

      {/* Real-time page metrics */}
      <Route.Branch condition="true">
        <TumbleWindow size="1 minute" on="event_time">
          <Aggregate
            groupBy={["page_url"]}
            select={{
              page_url: "page_url",
              view_count: "COUNT(*)",
              unique_visitors: "COUNT(DISTINCT user_id)",
            }}
          />
        </TumbleWindow>
        <KafkaSink topic="realtime_page_metrics" />
      </Route.Branch>

      {/* User activity upsert */}
      <Route.Branch condition="true">
        <Aggregate
          groupBy={["user_id"]}
          select={{
            user_id: "user_id",
            total_events: "COUNT(*)",
            session_count: "COUNT(DISTINCT session_id)",
            last_seen: "MAX(event_time)",
          }}
        />
        <JdbcSink
          url="jdbc:postgresql://db:5432/analytics"
          table="user_activity_summary"
          upsertMode={true}
          keyFields={["user_id"]}
        />
      </Route.Branch>

      {/* Error alerts */}
      <Route.Branch condition="event_type IN ('error', 'exception')">
        <KafkaSink topic="error_events_alerts" />
      </Route.Branch>
    </Route>
  </Pipeline>
);
```

> **See all 28 examples** in the [`src/examples/`](src/examples/) directory.

<br />

## <img src="assets/icons/code-xml.svg" width="24" height="24" style="vertical-align: middle; margin-bottom: 2px;"> Reference Pipelines

First-class, production-shaped pipeline templates that ship in-tree. Copy the
directory into your own project as a starting point — they are also the tracks
used by the Postgres → Iceberg CDC benchmark, so any number the benchmark
publishes is reproducible from the same source.

- [`pipelines/pg-cdc-iceberg-f1/`](pipelines/pg-cdc-iceberg-f1/) — **F1 (Kafka-hop):**
  Postgres → Debezium → Kafka → Flink SQL → Iceberg. Parameterised on
  `wireFormat` (`json` / `avro` / `protobuf`) and `commitMode`
  (`throughput` / `latency`).
- [`pipelines/pg-cdc-iceberg-f2/`](pipelines/pg-cdc-iceberg-f2/) — **F2 (Pipeline
  Connector):** Postgres → Flink CDC 3.6 Pipeline Connector → Iceberg. No
  Kafka hop. Parameterised on `snapshotMode`
  (`initial` / `never` / `initial_only`) and `commitMode`.

Both write to a Lakekeeper REST Iceberg catalog with Merge-on-Read
(`upsertEnabled`, equality-field columns, `zstd` Parquet, hash distribution)
so downstream Iceberg queries see equivalent tables regardless of which
pipeline is running.

<br />

## <img src="assets/icons/layers.svg" width="24" height="24" style="vertical-align: middle; margin-bottom: 2px;"> Architecture

```
┌──────────────────────────────────────────────────┐
│                  Your Pipeline                   │
│           (TypeScript + JSX components)          │
└──────────────────┬───────────────────────────────┘
                   │ flink-reactor synth
                   ▼
┌──────────────────────────────────────────────────┐
│              Construct Tree (DAG)                │
│    Sources → Transforms → Joins → Sinks          │
│         Topology validation + wiring             │
└──────────┬───────────────────────┬───────────────┘
           │                       │
           ▼                       ▼
┌─────────────────────┐  ┌─────────────────────────┐
│     SQL Generator   │  │     CRD Generator       │
│  CREATE TABLE ...   │  │  FlinkDeployment YAML   │
│  INSERT INTO ...    │  │  Connector JAR manifest │
└─────────────────────┘  └─────────────────────────┘
           │                       │
           ▼                       ▼
┌──────────────────────────────────────────────────┐
│         Flink Kubernetes Operator                │
│    Deploys your pipeline to a Flink cluster      │
└──────────────────────────────────────────────────┘
```

### <img src="assets/icons/shield.svg" width="20" height="20" style="vertical-align: middle; margin-bottom: 2px;"> Design Principles

- **Synthesis only** — no runtime code executes inside Flink. We generate SQL and YAML.
- **Custom JSX, not React** — `createElement()` builds a construct tree, not a virtual DOM.
- **DAG, not tree** — pipelines are directed acyclic graphs. JSX nesting is sugar for the linear case.
- **Deterministic output** — same input always produces the same SQL and YAML.
- **Flink SQL is the target** — all components compile to Flink SQL. No DataStream API (yet).

<br />

## <img src="assets/icons/folder-tree.svg" width="24" height="24" style="vertical-align: middle; margin-bottom: 2px;"> Project Structure

```
flink-reactor-dsl/
├── src/                          # Core DSL engine, components, codegen, CLI
│   ├── core/                     #   JSX runtime, schemas, synth context, DAG
│   ├── components/               #   Sources, sinks, transforms, joins, windows
│   ├── codegen/                  #   SQL generator, CRD generator, JAR resolution
│   ├── cli/                      #   Commander.js commands (new, synth, validate, ...)
│   ├── testing/                  #   synth() and validate() test helpers
│   └── examples/                 #   28 example pipelines (simple → advanced)
├── packages/
│   ├── create-fr-app/            # Project scaffolder (create-fr-app)
│   └── ts-plugin/                # TypeScript language service plugin
└── scripts/                      # Build and publish scripts
```

| Package                     | npm                                                                                                                                           | Description                               |
| --------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------- |
| `flink-reactor`             | [![npm](https://img.shields.io/npm/v/flink-reactor?color=d97085&label=)](https://www.npmjs.com/package/flink-reactor)                         | Core DSL engine, components, codegen, CLI |
| `@flink-reactor/create-app` | [![npm](https://img.shields.io/npm/v/@flink-reactor/create-app?color=d97085&label=)](https://www.npmjs.com/package/@flink-reactor/create-app) | Project scaffolder                        |
| `@flink-reactor/ts-plugin`  | [![npm](https://img.shields.io/npm/v/@flink-reactor/ts-plugin?color=d97085&label=)](https://www.npmjs.com/package/@flink-reactor/ts-plugin)   | TypeScript language service plugin        |

<br />

## <img src="assets/icons/circle-check.svg" width="24" height="24" style="vertical-align: middle; margin-bottom: 2px;"> Flink Version Compatibility

| Feature                       | Flink 1.20 | Flink 2.0 | Flink 2.1 | Flink 2.2 |
| ----------------------------- | :--------: | :-------: | :-------: | :-------: |
| All DSL components            |     ✅     |    ✅     |    ✅     |    ✅     |
| Flink SQL codegen             |     ✅     |    ✅     |    ✅     |    ✅     |
| FlinkDeployment CRDs          |     ✅     |    ✅     |    ✅     |    ✅     |
| Connector JAR resolution      |     ✅     |    ✅     |    ✅     |    ✅     |
| `CREATE MODEL` / `ML_PREDICT` |     —      |     —     |  🔜 v0.2  |  🔜 v0.2  |
| `VECTOR_SEARCH`               |     —      |     —     |     —     |  🔜 v0.2  |

Differences between versions (config key renames, JDBC connector structure) are handled automatically by `FlinkVersionCompat`.

<br />

## <img src="assets/icons/heart-handshake.svg" width="24" height="24" style="vertical-align: middle; margin-bottom: 2px;"> Contributing

We welcome contributions of all kinds — bug reports, feature suggestions, and PRs. See the **[Contributing Guide](CONTRIBUTING.md)** for setup instructions, development workflow, and release process.

- **Report bugs** — [open an issue](https://github.com/sandboxws/flink-reactor-dsl/issues/new)
- **Suggest features** — [start a discussion](https://github.com/sandboxws/flink-reactor-dsl/issues)
- **Submit PRs** — we use conventional commits (`feat:`, `fix:`, `docs:`, `refactor:`)

<br />

## <img src="assets/icons/scale.svg" width="24" height="24" style="vertical-align: middle; margin-bottom: 2px;"> License

This project uses a **split license model**:

| Package | License | npm |
|---------|---------|-----|
| `@flink-reactor/dsl` (core DSL) | [BSL 1.1](./LICENSE) | [![npm](https://img.shields.io/npm/v/@flink-reactor/dsl?color=d97085&label=)](https://www.npmjs.com/package/@flink-reactor/dsl) |
| `@flink-reactor/create-fr-app` | [MIT](./packages/create-fr-app/LICENSE) | [![npm](https://img.shields.io/npm/v/@flink-reactor/create-fr-app?color=d97085&label=)](https://www.npmjs.com/package/@flink-reactor/create-fr-app) |
| `@flink-reactor/ts-plugin` | [MIT](./packages/ts-plugin/LICENSE) | [![npm](https://img.shields.io/npm/v/@flink-reactor/ts-plugin?color=d97085&label=)](https://www.npmjs.com/package/@flink-reactor/ts-plugin) |

**Core DSL (BSL 1.1):**
- **Internal production use is always free** — use FlinkReactor to build and run your own pipelines without restriction.
- **Commercial license required** to offer FlinkReactor (or a derivative) as a managed service, hosted platform, or API to third parties.
- **Converts to [Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0) on 2030-03-10** — after the change date, this version becomes fully open source.

**Tooling packages (MIT):** The scaffolder and TypeScript plugin are MIT-licensed — use them freely in any context.

For commercial licensing inquiries, see [flink-reactor-platform](https://github.com/sandboxws/flink-reactor-platform).
