// `stock-temporal-topn` template factory.
//
// Ports the two more advanced Apache Flink Table API examples that
// demonstrate temporal/versioned joins and continuous Top-N ranking:
//   • temporal-join-fx        ← TemporalJoinSQLExample.java
//   • updating-top-city       ← UpdatingTopCityExample.java
//   • pump-temporal-join-fx   ← internal data-generator helper
//
// Per-pipeline READMEs document each translation choice (debezium-json
// over Kafka instead of in-process changelog rows; DataGen instead of CSV;
// <TopN> lowering vs LATERAL correlated subquery) under their Translation
// Notes.
import type { ScaffoldOptions, TemplateFile } from "@/cli/commands/new.js"
import {
  pipelineReadme,
  sharedFiles,
  templatePipelineTestStub,
} from "./shared.js"
import {
  bundleReadme,
  currencyRateSchemaFile,
  currencyRateSimTable,
  populationUpdatesSchemaFile,
  populationUpdatesSimTable,
  transactionSchemaFile,
  transactionSimTable,
} from "./stock-shared/index.js"

// Pinned ref on apache/flink for source links + LOC counts.
// LOC counts are wc -l on the corresponding `*.java` file at this ref.
const FLINK_REF = "release-2.0.0"
const FLINK_BASICS_BASE = `https://github.com/apache/flink/blob/${FLINK_REF}/flink-examples/flink-examples-table/src/main/java/org/apache/flink/table/examples/java/basics`

export function getStockTemporalTopnTemplates(
  opts: ScaffoldOptions,
): TemplateFile[] {
  return [
    // Drop the default `flink-reactor.config.ts` from sharedFiles so we can
    // emit a richer one with sim.init.kafka.tables wired up via the
    // *SimTable() helpers.
    ...sharedFiles(opts).filter((f) => f.path !== "flink-reactor.config.ts"),
    {
      path: "flink-reactor.config.ts",
      content: makeConfig(opts),
    },

    // ── Schemas ──────────────────────────────────────────────────────
    transactionSchemaFile(),
    currencyRateSchemaFile(),
    populationUpdatesSchemaFile(),

    // ── Pipelines ────────────────────────────────────────────────────
    {
      path: "pipelines/temporal-join-fx/index.tsx",
      content: TEMPORAL_JOIN_FX_PIPELINE,
    },
    {
      path: "pipelines/updating-top-city/index.tsx",
      content: UPDATING_TOP_CITY_PIPELINE,
    },
    {
      path: "pipelines/pump-temporal-join-fx/index.tsx",
      content: PUMP_TEMPORAL_JOIN_FX_PIPELINE,
    },

    // ── Per-pipeline READMEs ─────────────────────────────────────────
    pipelineReadme({
      pipelineName: "temporal-join-fx",
      tagline:
        "Versioned currency-rate join over an append-only transaction stream. Port of `TemporalJoinSQLExample.java` (~196 Java LOC → ~38 TSX LOC).",
      source: `[\`TemporalJoinSQLExample.java\`](${FLINK_BASICS_BASE}/TemporalJoinSQLExample.java)`,
      demonstrates: [
        "Temporal join via `<TemporalJoin>` lowering to `FOR SYSTEM_TIME AS OF`.",
        'Versioned dimension stream via `<KafkaSource format="debezium-json">` (retract changelog).',
        "Computed column (`amount * euroRate`) via `<Map>` after the join.",
      ],
      topology: `KafkaSource (transactions, append-only)        ─┐
                                                  ├─► TemporalJoin ─► Map (totalEuro) ─► KafkaSink (enriched-transactions)
KafkaSource (currency-rates, debezium-json) ─┘`,
      schemas: [
        "`schemas/transaction.ts` — `{ id, currencyCode, amount, trxTime }` with watermark on `trxTime`",
        "`schemas/currency-rate.ts` — `{ currencyCode, euroRate, rateTime }` with primary key on `currencyCode` and watermark on `rateTime`",
      ],
      runCommand: `pnpm synth
pnpm test`,
      translationNotes: TEMPORAL_JOIN_FX_TRANSLATION_NOTE,
    }),
    pipelineReadme({
      pipelineName: "updating-top-city",
      tagline:
        "Per-state Top-2 cities by population, continuously updated. Port of `UpdatingTopCityExample.java` (~194 Java LOC → ~36 TSX LOC).",
      source: `[\`UpdatingTopCityExample.java\`](${FLINK_BASICS_BASE}/UpdatingTopCityExample.java)`,
      demonstrates: [
        "Continuous aggregation with `<Aggregate>` (SUM, MAX) wrapped in a named `<View>`.",
        "Continuous Top-N ranking via `<TopN>` partitioned by state.",
        "`<TopN>` lowering to `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)` + rank filter.",
        'CSV file output via `<FileSystemSink format="csv">`.',
      ],
      topology: `DataGenSource (population-updates)
  └── View (current_population)
        └── Aggregate (GROUP BY city, state — SUM(populationDiff), MAX(updateYear))
              └── TopN (PARTITION BY state ORDER BY population DESC, latestYear DESC, n=2)
                    └── FileSystemSink (csv)`,
      schemas: [
        "`schemas/population-updates.ts` — `{ city, state, updateYear, populationDiff }`",
      ],
      runCommand: `pnpm synth
pnpm test`,
      translationNotes: UPDATING_TOP_CITY_TRANSLATION_NOTE,
    }),
    pipelineReadme({
      pipelineName: "pump-temporal-join-fx",
      tagline:
        "Synthetic currency-rate updates pumped onto the `currency-rates` Kafka topic to feed `temporal-join-fx`.",
      demonstrates: [
        '`<DataGenSource>` driving a `<KafkaSink>` with `format="debezium-json"`.',
        "Bundle-internal pump pattern (no upstream Apache Flink source — exists only to make the bundle runnable end-to-end on the local sim).",
      ],
      topology: `DataGenSource (currency-rate updates)
  └── KafkaSink (currency-rates, debezium-json)`,
      schemas: ["`schemas/currency-rate.ts` — same schema the consumer reads"],
      runCommand: `pnpm synth
pnpm test`,
      translationNotes: PUMP_TRANSLATION_NOTE,
    }),

    // ── Per-pipeline snapshot tests ──────────────────────────────────
    templatePipelineTestStub({
      pipelineName: "temporal-join-fx",
      loadBearingPatterns: [
        /FOR SYSTEM_TIME AS OF/i,
        /debezium-json/i,
        /amount \* `?euroRate`?/i,
      ],
    }),
    templatePipelineTestStub({
      pipelineName: "updating-top-city",
      loadBearingPatterns: [/ROW_NUMBER\(\)/i, /PARTITION BY `state`/i, /<= 2/],
    }),
    templatePipelineTestStub({
      pipelineName: "pump-temporal-join-fx",
      loadBearingPatterns: [/INSERT INTO/i, /debezium-json/i],
    }),

    // ── Project-root README ──────────────────────────────────────────
    bundleReadme({
      templateName: "stock-temporal-topn",
      tagline:
        "FlinkReactor ports of two advanced Apache Flink Table API examples — temporal (versioned-dimension) joins and continuous Top-N ranking — plus a bundled data-generator pipeline that feeds the temporal join.",
      pipelines: [
        {
          name: "temporal-join-fx",
          pitch:
            "Append-only transactions joined to a versioned currency-rates stream via `FOR SYSTEM_TIME AS OF` (~196 Java LOC → ~38 TSX LOC).",
          sourceFile: "TemporalJoinSQLExample.java",
        },
        {
          name: "updating-top-city",
          pitch:
            "Per-state Top-2 cities by population, computed continuously via `ROW_NUMBER()` + rank filter (~194 Java LOC → ~36 TSX LOC).",
          sourceFile: "UpdatingTopCityExample.java",
        },
        {
          name: "pump-temporal-join-fx",
          pitch:
            "Internal helper pipeline that pumps synthetic currency-rate updates onto the `currency-rates` Kafka topic so `temporal-join-fx` has data to consume.",
          sourceFile: "(internal helper)",
        },
      ],
      gettingStarted: ["pnpm install", "pnpm synth", "pnpm test"],
    }),
  ]
}

// ── Pipeline source bodies ──────────────────────────────────────────
//
// Each constant is the verbatim TSX source for `pipelines/<name>/index.tsx`.
// Backticks inside the embedded TSX are escaped as `\\\`` so the outer
// template literal here renders cleanly.

const TEMPORAL_JOIN_FX_PIPELINE = `import {
  Pipeline,
  KafkaSource,
  KafkaSink,
  TemporalJoin,
  Map,
} from '@flink-reactor/dsl';
import TransactionSchema from '@/schemas/transaction';
import CurrencyRateSchema from '@/schemas/currency-rate';

const transactions = (
  <KafkaSource topic="transactions" schema={TransactionSchema} />
);

// debezium-json carries change-log semantics, which the temporal join
// requires for the versioned (right) side. The schema's primaryKey on
// \`currencyCode\` is what makes this stream a versioned table.
const currencyRates = (
  <KafkaSource
    topic="currency-rates"
    schema={CurrencyRateSchema}
    format="debezium-json"
  />
);

export default (
  <Pipeline name="temporal-join-fx">
    <TemporalJoin
      stream={transactions}
      temporal={currencyRates}
      on="currencyCode = currencyCode"
      asOf="trxTime"
    />
    <Map
      select={{
        id: 'id',
        currencyCode: 'currencyCode',
        amount: 'amount',
        trxTime: 'trxTime',
        euroRate: 'euroRate',
        totalEuro: 'amount * \`euroRate\`',
      }}
    />
    <KafkaSink topic="enriched-transactions" />
  </Pipeline>
);
`

const UPDATING_TOP_CITY_PIPELINE = `import {
  Pipeline,
  DataGenSource,
  Aggregate,
  TopN,
  View,
  FileSystemSink,
} from '@flink-reactor/dsl';
import PopulationUpdatesSchema from '@/schemas/population-updates';

// View captures the continuously-updating per-(city,state) aggregate as
// \`current_population\`, mirroring the Java original's
// \`tableEnv.createTemporaryView("CurrentPopulation", ...)\`. Downstream
// the TopN reads from the view by name, producing a separate ROW_NUMBER
// query stage.
const currentPopulation = (
  <View name="current_population">
    <Aggregate
      groupBy={['city', 'state']}
      select={{
        city: 'city',
        state: 'state',
        latestYear: 'MAX(\\\`updateYear\\\`)',
        population: 'SUM(\\\`populationDiff\\\`)',
      }}
    >
      <DataGenSource
        schema={PopulationUpdatesSchema}
        rowsPerSecond={5}
        name="population_updates"
      />
    </Aggregate>
  </View>
);

const ranked = (
  <TopN
    partitionBy={['state']}
    orderBy={{ population: 'DESC', latestYear: 'DESC' }}
    n={2}
  >
    <View name="current_population" />
  </TopN>
);

export default (
  <Pipeline name="updating-top-city">
    {currentPopulation}
    <FileSystemSink path="output/top-cities" format="csv">
      {ranked}
    </FileSystemSink>
  </Pipeline>
);
`

const PUMP_TEMPORAL_JOIN_FX_PIPELINE = `import {
  Pipeline,
  DataGenSource,
  KafkaSink,
} from '@flink-reactor/dsl';
import CurrencyRateSchema from '@/schemas/currency-rate';

// Pumps synthetic currency-rate updates onto the \`currency-rates\` topic
// so \`temporal-join-fx\` has a versioned dimension stream to consume.
// Format is debezium-json to match the consumer's expectation.
export default (
  <Pipeline name="pump-temporal-join-fx">
    <DataGenSource
      schema={CurrencyRateSchema}
      rowsPerSecond={1}
      name="currency_rate_gen"
    />
    <KafkaSink topic="currency-rates" format="debezium-json" />
  </Pipeline>
);
`

// ── Translation notes ───────────────────────────────────────────────

const TEMPORAL_JOIN_FX_TRANSLATION_NOTE = `The Apache Flink original constructs the versioned currency-rates stream **in-process** by emitting \`Row.ofKind(RowKind.INSERT|UPDATE_AFTER, ...)\` rows from a \`DataStreamSource\`:

\`\`\`java
DataStream<Row> ratesStream = env.fromElements(
    Row.ofKind(RowKind.INSERT,  "Euro",     "no_1", LocalDateTime.parse("2020-08-08T23:59:59.999")),
    Row.ofKind(RowKind.UPDATE_AFTER, "Yen",  "no_1", LocalDateTime.parse("2020-08-09T00:00:00")),
    /* ... */);
Table rates = tEnv.fromChangelogStream(ratesStream, ...);
\`\`\`

The DSL has no first-class primitive for in-process change-log generation, and even if one existed it would obscure the canonical production shape. So the FlinkReactor port substitutes a \`<KafkaSource>\` with \`format="debezium-json"\`:

\`\`\`tsx
<KafkaSource
  topic="currency-rates"
  schema={CurrencyRateSchema}
  format="debezium-json"
/>
\`\`\`

Two consequences:

1. The bundle ships a separate **\`pump-temporal-join-fx\`** pipeline that pumps synthetic rate updates onto the \`currency-rates\` topic so this consumer has data to read on the local sim.
2. The consumer's schema must declare \`primaryKey: { columns: ['currencyCode'] }\` (it does — see \`schemas/currency-rate.ts\`) so Flink treats the stream as a versioned table for the temporal join.

The \`<TemporalJoin>\` itself lowers to \`FOR SYSTEM_TIME AS OF \\\`trxTime\\\`\`, identical to the SQL the Java original generates.`

const UPDATING_TOP_CITY_TRANSLATION_NOTE = `The Apache Flink original reads from a CSV file and uses a \`LATERAL\` correlated subquery to produce per-state Top-2 rows:

\`\`\`sql
CREATE TABLE PopulationUpdates (city STRING, state STRING, update_year INT, population_diff BIGINT)
WITH ('connector' = 'filesystem', 'path' = '<update.csv>', 'format' = 'csv');

CREATE TEMPORARY VIEW CurrentPopulation AS
  SELECT city, state, MAX(update_year), SUM(population_diff) AS population
  FROM PopulationUpdates
  GROUP BY city, state;

SELECT state, ranked.city, ranked.population
FROM ( SELECT DISTINCT state FROM CurrentPopulation ) States,
LATERAL (
  SELECT city, population
  FROM CurrentPopulation
  WHERE state = States.state
  ORDER BY population DESC
  LIMIT 2
) AS ranked;
\`\`\`

Two deliberate translation choices in the FlinkReactor port:

**1. \`<DataGenSource>\` instead of CSV.** Avoids shipping a seed CSV file alongside the template (which \`TemplateFile.content\` does support but adds a "where's the file?" friction during first run). DataGen produces a continuous stream of synthetic city/state/year/populationDiff rows that the \`<View>\` + \`<Aggregate>\` + \`<TopN>\` chain processes naturally.

**2. \`<TopN>\` lowering instead of \`LATERAL\`.** The DSL's \`<TopN>\` lowers to:

\`\`\`sql
SELECT *, ROW_NUMBER() OVER (
  PARTITION BY \\\`state\\\`
  ORDER BY \\\`population\\\` DESC, \\\`latestYear\\\` DESC
) AS rownum
FROM \\\`current_population\\\`
QUALIFY rownum <= 2     -- on Flink 1.x this is rendered as: WHERE rownum <= 2
\`\`\`

This is **semantically equivalent** to the original \`LATERAL ( SELECT ... ORDER BY ... LIMIT 2 )\` — both produce the same continuous Top-N output stream. The \`ROW_NUMBER\` form is the canonical pattern in the Flink Table API documentation for continuous Top-N, and it's also what the Flink runtime optimizer would lower the \`LATERAL\` form to internally. If you grep the synthesized SQL for \`LATERAL\` and don't find it, that's why — the same query is expressed in a form Flink prefers.`

const PUMP_TRANSLATION_NOTE = `This pipeline has **no upstream Apache Flink source** — it exists only to make the \`stock-temporal-topn\` bundle runnable end-to-end on the local sim.

\`temporal-join-fx\` reads from a \`currency-rates\` Kafka topic in \`debezium-json\` format. Without a producer, the topic is empty and the consumer pipeline emits nothing. This pump synthesises rate updates via \`<DataGenSource>\` and writes them to the same topic with the same format, closing the loop:

\`\`\`
pump-temporal-join-fx ─► currency-rates topic ─► temporal-join-fx
\`\`\`

The same pattern is used by other FlinkReactor templates that need a data source: \`ecommerce\` ships \`pump-ecom-rides\`, \`iot-factory\` ships \`pump-iot-factory\`. Pumps are not Apache Flink stock examples — they are bundle-internal helpers.`

// ── Config builder ──────────────────────────────────────────────────

function makeConfig(opts: ScaffoldOptions): string {
  return `import { defineConfig } from '@flink-reactor/dsl';

export default defineConfig({
  flink: { version: '${opts.flinkVersion}' },

  // Kafka-only template.
  services: { kafka: { bootstrapServers: 'kafka:9092' } },

  environments: {
    development: {
      cluster: { url: 'http://localhost:8081' },
      dashboard: { mockMode: true },
      pipelines: { '*': { parallelism: 1 } },
    },
    minikube: {
      cluster: { url: 'http://localhost:8081' },
      sim: {
        init: {
          kafka: {
            catalogs: [{
              name: 'temporal_topn',
              tables: [
                ${transactionSimTable()},
                ${currencyRateSimTable()},
                ${populationUpdatesSimTable()},
              ],
            }],
          },
        },
      },
      pipelines: { '*': { parallelism: 1 } },
    },
    production: {
      cluster: { url: 'https://flink-prod:8081' },
      kubernetes: { namespace: 'flink-prod' },
      pipelines: { '*': { parallelism: 2 } },
    },
  },
});
`
}
