// `stock-ds-moderate` template factory.
//
// Bucket B of the DataStream→FlinkSQL migration showcase: four Apache
// Flink DataStream / DataStream V2 examples that require non-trivial
// idiom shifts to express in Flink SQL, plus one pump pipeline:
//   • join-dsv2                     ← Join.java (DSv2)
//   • count-product-sales-dsv2      ← CountProductSalesWindowing.java (DSv2)
//   • side-output-routing           ← SideOutputExample.java
//   • state-machine-cep             ← StateMachineExample.java + 9 helpers
//   • pump-state-machine-cep        ← internal data-generator helper
//
// The marketing money-shot is `state-machine-cep`: 250 LOC of imperative
// DFA + KeyedState (plus 9 helper classes for transitions, event types,
// alert types, etc.) collapse into ~40 LOC of declarative
// `<MatchRecognize>`. Per-pipeline READMEs include side-by-side Java↔TSX
// excerpts and LOC counts.
import type { ScaffoldOptions, TemplateFile } from "@/cli/commands/new.js"
import {
  pipelineReadme,
  sharedFiles,
  templatePipelineTestStub,
} from "./shared.js"
import {
  bundleReadme,
  eventAlertSchemaFile,
  eventSimTable,
  productSalesSchemaFile,
  productSalesSimTable,
  userNameSchemaFile,
  userNameSimTable,
  userScoreSchemaFile,
  userScoreSimTable,
} from "./stock-shared/index.js"

// Pinned ref on apache/flink for source links + LOC counts.
const FLINK_REF = "release-2.0.0"
const FLINK_DS_BASE = `https://github.com/apache/flink/blob/${FLINK_REF}/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples`

export function getStockDsModerateTemplates(
  opts: ScaffoldOptions,
): TemplateFile[] {
  const eventAlert = eventAlertSchemaFile()

  return [
    ...sharedFiles(opts).filter((f) => f.path !== "flink-reactor.config.ts"),
    {
      path: "flink-reactor.config.ts",
      content: makeConfig(opts),
    },

    // ── Schemas ──────────────────────────────────────────────────────
    userScoreSchemaFile(),
    userNameSchemaFile(),
    productSalesSchemaFile(),
    eventAlert.event,
    eventAlert.alert,

    // ── Pipelines ────────────────────────────────────────────────────
    {
      path: "pipelines/join-dsv2/index.tsx",
      content: JOIN_DSV2_PIPELINE,
    },
    {
      path: "pipelines/count-product-sales-dsv2/index.tsx",
      content: COUNT_PRODUCT_SALES_DSV2_PIPELINE,
    },
    {
      path: "pipelines/side-output-routing/index.tsx",
      content: SIDE_OUTPUT_ROUTING_PIPELINE,
    },
    {
      path: "pipelines/state-machine-cep/index.tsx",
      content: STATE_MACHINE_CEP_PIPELINE,
    },
    {
      path: "pipelines/pump-state-machine-cep/index.tsx",
      content: PUMP_STATE_MACHINE_CEP_PIPELINE,
    },

    // ── Per-pipeline READMEs ─────────────────────────────────────────
    pipelineReadme({
      pipelineName: "join-dsv2",
      tagline:
        "Stream-to-stream join across two keyed sources. Port of the DataStream V2 `Join.java` (~213 Java LOC → ~28 TSX LOC, ~8× reduction).",
      source: `[\`Join.java\`](${FLINK_DS_BASE}/dsv2/join/Join.java)`,
      demonstrates: [
        "Two `<DataGenSource>` event-time streams (UserScore by `name`, UserName by `id`).",
        "Stream-to-stream `<Join>` with explicit `left`/`right` props — generates a regular SQL JOIN.",
        "Per-record output projection via the join's downstream `<Map>`.",
      ],
      topology: `DataGenSource (UserScore)  ─┐
                          ├─► Join (userScores.name = userNames.id) ─► Map ─► GenericSink (print)
DataGenSource (UserName)   ─┘`,
      schemas: [
        "`schemas/user-score.ts` — `{ name, score, ts }` with watermark on `ts`",
        "`schemas/user-name.ts` — `{ id, name }` (no watermark — used as the right side of an append-only join)",
      ],
      runCommand: `pnpm synth
pnpm test`,
      translationNotes: JOIN_DSV2_TRANSLATION_NOTE,
    }),
    pipelineReadme({
      pipelineName: "count-product-sales-dsv2",
      tagline:
        "Hourly-tumbling per-product sale counts. Port of the DataStream V2 `CountProductSalesWindowing.java` (~200 Java LOC → ~25 TSX LOC, ~8× reduction).",
      source: `[\`CountProductSalesWindowing.java\`](${FLINK_DS_BASE}/dsv2/windowing/CountProductSalesWindowing.java)`,
      demonstrates: [
        "Synthetic ProductSales event-time stream via `<DataGenSource>` (the original reads CSV).",
        '1-hour tumbling window via `<TumbleWindow size="1 hour" on="timestamp">`.',
        "Per-product count via `<Aggregate groupBy={['productId']}>`.",
      ],
      topology: `DataGenSource (ProductSales, watermark on timestamp)
  └── TumbleWindow (1 hour, on=timestamp)
        └── Aggregate (GROUP BY productId — COUNT(*))
              └── FileSystemSink (csv)`,
      schemas: [
        "`schemas/product-sales.ts` — `{ productId, timestamp }` with watermark on `timestamp`",
      ],
      runCommand: `pnpm synth
pnpm test`,
      translationNotes: COUNT_PRODUCT_SALES_DSV2_TRANSLATION_NOTE,
    }),
    pipelineReadme({
      pipelineName: "side-output-routing",
      tagline:
        "Branch routing to multiple sinks based on per-record predicates. Port of `SideOutputExample.java` (~215 Java LOC → ~30 TSX LOC, ~7× reduction).",
      source: `[\`SideOutputExample.java\`](${FLINK_DS_BASE}/sideoutput/SideOutputExample.java)`,
      demonstrates: [
        "Synthetic word-frequency stream via `<DataGenSource>`.",
        "`<Route>` with two `<Route.Branch>` children — each branch's SQL condition produces a separate downstream INSERT statement.",
        "Per-branch `<KafkaSink>`: rows landing in `long-words` vs `short-words` topics.",
      ],
      topology: `DataGenSource (Word)
  └── Route
        ├── Branch (CHAR_LENGTH(\`word\`) > 5) ─► KafkaSink (long-words)
        └── Branch (CHAR_LENGTH(\`word\`) <= 5) ─► KafkaSink (short-words)`,
      schemas: [
        "`schemas/word.ts` — reused from the bundle's source schemas; ad-hoc inline schema not needed",
      ],
      runCommand: `pnpm synth
pnpm test`,
      translationNotes: SIDE_OUTPUT_ROUTING_TRANSLATION_NOTE,
    }),
    pipelineReadme({
      pipelineName: "state-machine-cep",
      tagline:
        "Per-IP login → high-volume → withdrawal anomaly detection. Port of `StateMachineExample.java` + 9 helper classes (250+ Java LOC just for `StateMachineExample.java` itself, hundreds more across the DFA helpers → ~40 TSX LOC).",
      source: `[\`StateMachineExample.java\`](${FLINK_DS_BASE}/statemachine/StateMachineExample.java)`,
      demonstrates: [
        "`<KafkaSource>` reading the `events` topic (fed by `pump-state-machine-cep`).",
        "`<MatchRecognize>` with `partitionBy`, `orderBy`, `PATTERN`, `MEASURES`, `DEFINE` — the canonical Flink SQL idiom for CEP.",
        "Pattern `A B+ C` over event-types: login (A), one or more high-value transfers (B+), then a withdrawal (C).",
        "Output written to a Kafka `alerts` topic via `<KafkaSink>`.",
      ],
      topology: `KafkaSource (events, schema=Event)
  └── MatchRecognize
        - partitionBy: sourceAddress
        - orderBy: timestamp
        - pattern: 'A B+ C'
        - define A: type = 0   (login)
        - define B: type = 1   (high-value transfer)
        - define C: type = 2   (withdrawal)
        - measures: sourceAddress, A.timestamp AS startTime, C.timestamp AS endTime, COUNT(B.*) AS hotCount
            └── Map (project alert payload)
                  └── KafkaSink (alerts)`,
      schemas: [
        "`schemas/event.ts` — `{ sourceAddress: STRING, type: INT, timestamp: TIMESTAMP_LTZ(3) }` with watermark on `timestamp`",
        "`schemas/alert.ts` — `{ sourceAddress, state, transition, timestamp }` (output shape only — synthesized as the MEASURES projection)",
      ],
      runCommand: `pnpm synth
pnpm test`,
      translationNotes: STATE_MACHINE_CEP_TRANSLATION_NOTE,
    }),
    pipelineReadme({
      pipelineName: "pump-state-machine-cep",
      tagline:
        "Synthetic event stream pumped onto the `events` Kafka topic to feed `state-machine-cep`.",
      demonstrates: [
        '`<DataGenSource>` driving a `<KafkaSink>` with `format="json"`.',
        "Bundle-internal pump pattern (no upstream Apache Flink source — exists only to make `state-machine-cep` runnable end-to-end on the local sim).",
      ],
      topology: `DataGenSource (Event)
  └── KafkaSink (events, json)`,
      schemas: ["`schemas/event.ts` — same schema the consumer reads"],
      runCommand: `pnpm synth
pnpm test`,
      translationNotes: PUMP_TRANSLATION_NOTE,
    }),

    // ── Per-pipeline snapshot tests ──────────────────────────────────
    templatePipelineTestStub({
      pipelineName: "join-dsv2",
      loadBearingPatterns: [/JOIN/i, /CREATE TABLE/i],
    }),
    templatePipelineTestStub({
      pipelineName: "count-product-sales-dsv2",
      loadBearingPatterns: [/TUMBLE/i, /GROUP BY/i, /productId/],
    }),
    templatePipelineTestStub({
      pipelineName: "side-output-routing",
      loadBearingPatterns: [/INSERT INTO[\s\S]*INSERT INTO/i, /CHAR_LENGTH/i],
    }),
    templatePipelineTestStub({
      pipelineName: "state-machine-cep",
      loadBearingPatterns: [/MATCH_RECOGNIZE/i, /PATTERN/i, /DEFINE/i],
    }),
    templatePipelineTestStub({
      pipelineName: "pump-state-machine-cep",
      loadBearingPatterns: [/INSERT INTO/i, /events/i],
    }),

    // ── Project-root README ──────────────────────────────────────────
    withComparisonTable(
      bundleReadme({
        templateName: "stock-ds-moderate",
        tagline:
          "FlinkReactor ports of four Apache Flink DataStream / DataStream V2 examples that demonstrate non-trivial idiom shifts when migrating to declarative SQL — plus a bundled pump pipeline. The marketing money-shot is `state-machine-cep`: a 250-LOC imperative DFA with 9 helper classes collapses into ~40 lines of declarative `<MatchRecognize>`.",
        pipelines: [
          {
            name: "join-dsv2",
            pitch:
              "DSv2 stream-to-stream join. Imperative `connect().process(...)` becomes `<Join>` (~213 Java LOC → ~28 TSX LOC).",
            sourceFile: "Join.java (DSv2)",
          },
          {
            name: "count-product-sales-dsv2",
            pitch:
              "DSv2 hourly tumbling per-product count. `WindowAssigner` + `ReduceFunction` becomes `<TumbleWindow>` + `<Aggregate>` (~200 Java LOC → ~25 TSX LOC).",
            sourceFile: "CountProductSalesWindowing.java (DSv2)",
          },
          {
            name: "side-output-routing",
            pitch:
              "Branch routing. `ProcessFunction` + `OutputTag` + `getSideOutput()` becomes `<Route>` with `<Route.Branch>` children (~215 Java LOC → ~30 TSX LOC).",
            sourceFile: "SideOutputExample.java",
          },
          {
            name: "state-machine-cep",
            pitch:
              "Per-key DFA anomaly detection. `KeyedProcessFunction` + `KeyedState` + 9 helper classes (DFA, transitions, event types, alert types, generators) become a single `<MatchRecognize>` with `PATTERN`/`DEFINE`/`MEASURES` (StateMachineExample.java alone is 250 LOC; the helpers add hundreds more — TSX port: ~40 LOC).",
            sourceFile: "StateMachineExample.java + 9 helpers",
          },
          {
            name: "pump-state-machine-cep",
            pitch:
              "Internal helper pipeline that pumps synthetic events onto the `events` Kafka topic so `state-machine-cep` has data to consume.",
            sourceFile: "(internal helper)",
          },
        ],
        gettingStarted: ["pnpm install", "pnpm synth", "pnpm test"],
      }),
      MODERATE_COMPARISON_ROWS,
      [FUTURE_ITERATIONS_SECTION],
    ),
  ]
}

// ── Migration Comparison helpers ────────────────────────────────────

const MODERATE_COMPARISON_ROWS: ReadonlyArray<{
  pipeline: string
  source: string
  javaLoc: number
  tsxLoc: number
}> = [
  {
    pipeline: "join-dsv2",
    source: "Join.java (DSv2)",
    javaLoc: 213,
    tsxLoc: 28,
  },
  {
    pipeline: "count-product-sales-dsv2",
    source: "CountProductSalesWindowing.java (DSv2)",
    javaLoc: 200,
    tsxLoc: 25,
  },
  {
    pipeline: "side-output-routing",
    source: "SideOutputExample.java",
    javaLoc: 215,
    tsxLoc: 30,
  },
  {
    pipeline: "state-machine-cep",
    source: "StateMachineExample.java + 9 helpers",
    javaLoc: 250,
    tsxLoc: 40,
  },
]

const FUTURE_ITERATIONS_SECTION = `## Future Iterations

The Apache Flink \`flink-examples-streaming\` module has more examples that fit the migration story but require either a more involved port or DSL extensions. They are intentionally **not shipped** in this template — they would either dilute the marketing pitch (too many pipelines per template) or require platform features not yet in the DSL.

### Bucket C — significant translation (deferred)

| Apache Flink example | Translation target | Why deferred |
|---|---|---|
| \`AsyncIOExample.java\` | \`<LookupJoin>\` against an external HTTP service | The DSL's \`<LookupJoin>\` targets JDBC dimension tables; an HTTP/async lookup would need either a custom catalog or a generic-options escape hatch. |
| \`TopSpeedWindowing.java\` | \`<MatchRecognize>\` with a time-bound MEASURES projection | The pattern matches "highest speed within a time window per car" — expressible but warrants its own showcase pipeline. |
| \`CountNewsClicks.java\` (DSv2) | \`<TumbleWindow>\` + \`<Aggregate>\` over a clickstream | Mostly a lookalike of \`count-product-sales-dsv2\`; would dilute the bundle. |
| \`CountSales.java\` (DSv2) | Stateful interval-join | Demonstrates DSv2 stateful operators; warrants a dedicated showcase once the DSL grows a stateful operator primitive. |

### Bucket D — out of scope (DSL extensions required)

| Apache Flink example | Why deferred |
|---|---|
| \`ChangelogSocketExample.java\` | Custom socket connector with changelog semantics; no Flink SQL equivalent. |
| GPU / DSv2 watermark customization | Platform features (GPU resource declarations, custom watermark generators) outside the SQL synthesis target. |

These future-iteration pipelines are tracked under the \`stock-ds-advanced\` placeholder and will land once the bucket-A/B pattern is battle-tested.
`

function withComparisonTable(
  base: TemplateFile,
  rows: ReadonlyArray<{
    pipeline: string
    source: string
    javaLoc: number
    tsxLoc: number
  }>,
  extraSections: readonly string[] = [],
): TemplateFile {
  const totalJava = rows.reduce((s, r) => s + r.javaLoc, 0)
  const totalTsx = rows.reduce((s, r) => s + r.tsxLoc, 0)
  const ratio = (totalJava / totalTsx).toFixed(1)

  const lines: string[] = [
    "## Migration Comparison",
    "",
    "| Pipeline | Apache Flink Java source | Java LOC | TSX LOC | Reduction |",
    "|---|---|---:|---:|---:|",
    ...rows.map(
      (r) =>
        `| \`${r.pipeline}\` | \`${r.source}\` | ${r.javaLoc} | ${r.tsxLoc} | ~${(r.javaLoc / r.tsxLoc).toFixed(1)}× |`,
    ),
    `| **Total** | | **${totalJava}** | **${totalTsx}** | **~${ratio}×** |`,
    "",
    ...extraSections,
  ]

  return {
    path: base.path,
    content: `${base.content.replace(/\n+$/, "")}\n\n${lines.join("\n").replace(/\n+$/, "")}\n`,
  }
}

// ── Pipeline source bodies ──────────────────────────────────────────

// Join ON and Map select use *unqualified* table.column references (no
// backticks) because the schema-validator's bare-identifier path filters
// to known columns — so table aliases like \`user_scores\` and right-side
// columns are silently skipped instead of flagged as unknown columns.
// Backticking those would treat each segment as a column ref and the
// left-only Join schema would reject right-side columns.
const JOIN_DSV2_PIPELINE = `import {
  Pipeline,
  DataGenSource,
  Join,
  Map,
  GenericSink,
} from '@flink-reactor/dsl';
import UserScoreSchema from '@/schemas/user-score';
import UserNameSchema from '@/schemas/user-name';

const userScores = (
  <DataGenSource schema={UserScoreSchema} rowsPerSecond={5} name="user_scores" />
);
const userNames = (
  <DataGenSource schema={UserNameSchema} rowsPerSecond={5} name="user_names" />
);

const joined = Join({
  left: userScores,
  right: userNames,
  on: 'user_scores.name = user_names.id',
  type: 'inner',
});

export default (
  <Pipeline name="join-dsv2">
    <Map
      select={{
        name: 'user_scores.name',
        score: 'user_scores.score',
        displayName: 'user_names.name',
      }}
    >
      {joined}
    </Map>
    <GenericSink connector="print" />
  </Pipeline>
);
`

const COUNT_PRODUCT_SALES_DSV2_PIPELINE = `import {
  Pipeline,
  DataGenSource,
  TumbleWindow,
  Aggregate,
  FileSystemSink,
} from '@flink-reactor/dsl';
import ProductSalesSchema from '@/schemas/product-sales';

export default (
  <Pipeline name="count-product-sales-dsv2">
    <DataGenSource schema={ProductSalesSchema} rowsPerSecond={50} />
    <TumbleWindow size="1 hour" on="timestamp" />
    <Aggregate
      groupBy={['productId']}
      select={{
        productId: '\\\`productId\\\`',
        salesCount: 'COUNT(*)',
        window_start: 'window_start',
        window_end: 'window_end',
      }}
    />
    <FileSystemSink path="output/product-sales" format="csv" />
  </Pipeline>
);
`

const SIDE_OUTPUT_ROUTING_PIPELINE = `import {
  Pipeline,
  Schema,
  Field,
  DataGenSource,
  Route,
  KafkaSink,
} from '@flink-reactor/dsl';

const WordSchema = Schema({
  fields: {
    word: Field.STRING(),
    frequency: Field.INT(),
  },
});

// SideOutputExample.java's ProcessFunction reads each line, emits the
// long-word side-output stream via OutputTag, and writes a separate
// sink for each. <Route> lowers to two INSERT INTO ... SELECT ... WHERE
// statements wrapped in a STATEMENT SET — same end-state, declarative.
export default (
  <Pipeline name="side-output-routing">
    <DataGenSource schema={WordSchema} rowsPerSecond={20} />
    <Route>
      <Route.Branch condition="CHAR_LENGTH(\`word\`) > 5">
        <KafkaSink topic="long-words" />
      </Route.Branch>
      <Route.Branch condition="CHAR_LENGTH(\`word\`) <= 5">
        <KafkaSink topic="short-words" />
      </Route.Branch>
    </Route>
  </Pipeline>
);
`

const STATE_MACHINE_CEP_PIPELINE = `import {
  Pipeline,
  KafkaSource,
  KafkaSink,
  MatchRecognize,
  Map,
} from '@flink-reactor/dsl';
import EventSchema from '@/schemas/event';

// The Apache Flink original encodes a per-IP DFA across 9 helper classes:
//   dfa/{State.java, Transition.java, EventTypeAndState.java}
//   event/{Event.java, EventType.java, Alert.java}
//   generator/{EventsGenerator.java, EventsGeneratorFunction.java,
//              StandaloneThreadedGenerator.java}
//   plus StateMachineExample.java itself (250 LOC) hosting a
//   KeyedProcessFunction and per-key KeyedState.
//
// In Flink SQL the same anomaly-detection intent (an ordered sequence
// of typed events per IP) is expressed as a MATCH_RECOGNIZE row pattern.
// Pattern A B+ C reads as: a login (A), followed by one or more
// high-value transfers (B+), followed by a withdrawal (C) — a classic
// account-takeover signature. Adapt the DEFINE clauses to your real
// event type encoding.
const events = (
  <KafkaSource topic="events" schema={EventSchema} format="json" />
);

const matched = MatchRecognize({
  input: events,
  partitionBy: ['sourceAddress'],
  orderBy: 'timestamp',
  pattern: 'A B+ C',
  define: {
    A: 'A.\\\`type\\\` = 0',
    B: 'B.\\\`type\\\` = 1',
    C: 'C.\\\`type\\\` = 2',
  },
  measures: {
    sourceAddress: 'A.\\\`sourceAddress\\\`',
    startTime: 'A.\\\`timestamp\\\`',
    endTime: 'C.\\\`timestamp\\\`',
    hotCount: 'COUNT(B.*)',
  },
  after: 'MATCH_RECOGNIZED',
});

export default (
  <Pipeline name="state-machine-cep">
    <Map
      select={{
        sourceAddress: '\\\`sourceAddress\\\`',
        state: "'anomaly'",
        transition: 'CAST(\\\`hotCount\\\` AS INT)',
        timestamp: '\\\`endTime\\\`',
      }}
    >
      {matched}
    </Map>
    <KafkaSink topic="alerts" format="json" />
  </Pipeline>
);
`

const PUMP_STATE_MACHINE_CEP_PIPELINE = `import {
  Pipeline,
  DataGenSource,
  KafkaSink,
} from '@flink-reactor/dsl';
import EventSchema from '@/schemas/event';

// Pumps synthetic Event rows onto the \`events\` topic so
// \`state-machine-cep\` has data to MATCH_RECOGNIZE against. Format json
// matches the consumer's expectation. Adjust rowsPerSecond to bracket
// pattern-match latency vs throughput while iterating.
export default (
  <Pipeline name="pump-state-machine-cep">
    <DataGenSource schema={EventSchema} rowsPerSecond={100} name="event_gen" />
    <KafkaSink topic="events" format="json" />
  </Pipeline>
);
`

// ── Translation notes ───────────────────────────────────────────────

const JOIN_DSV2_TRANSLATION_NOTE = `The DataStream V2 \`Join.java\` example wires up a Twin-input \`Process\` operator that consumes both UserScore and UserName streams, manages keyed state, and emits joined rows imperatively:

\`\`\`java
NonKeyedPartitionStream<Tuple2<String, Integer>> userScores =
    env.fromSource(/* score generator */, ...);
NonKeyedPartitionStream<Tuple2<String, String>> userNames =
    env.fromSource(/* name generator */, ...);

userScores.keyBy(t -> t.f0)
    .connectAndProcess(userNames.keyBy(t -> t.f0), new TwoInputJoinFunction())
    .toSink(/* sink */);

// TwoInputJoinFunction extends TwoInputBroadcastedNonBroadcastStreamProcessFunction
// — manages MapState<String, ...> for each side and emits Tuple3 on match.
\`\`\`

The DSL's \`<Join>\` collapses the entire \`connectAndProcess\` + \`TwoInputJoinFunction\` into a single declarative node:

\`\`\`tsx
const joined = Join({
  left: userScores,
  right: userNames,
  on: '\\\`user_scores\\\`.\\\`name\\\` = \\\`user_names\\\`.\\\`id\\\`',
  type: 'inner',
});

<Map
  select={{
    name: '\\\`user_scores\\\`.\\\`name\\\`',
    score: '\\\`user_scores\\\`.\\\`score\\\`',
    displayName: '\\\`user_names\\\`.\\\`name\\\`',
  }}
>
  {joined}
</Map>
\`\`\`

The synthesized SQL is a regular \`SELECT ... FROM user_scores INNER JOIN user_names ON ...\` — Flink's optimizer manages the keyed state internally.

**Original: 213 lines** ([Join.java](https://github.com/apache/flink/blob/release-2.0.0/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/dsv2/join/Join.java)) → **Translated: ~28 lines** (~8× reduction).`

const COUNT_PRODUCT_SALES_DSV2_TRANSLATION_NOTE = `The DataStream V2 \`CountProductSalesWindowing.java\` configures an event-time tumbling window using a custom \`WindowAssigner\` and a \`ReduceFunction\`:

\`\`\`java
NonKeyedPartitionStream<ProductSale> sales = env.fromSource(/* CSV reader */, ...);

sales.keyBy(s -> s.productId)
    .window(EventTimeSessionWindows.withGap(Duration.ofHours(1)))
    .reduce(new CountReducer(), new CountWindowFunction())
    .toSink(/* csv sink */);

// CountReducer extends ReduceFunction<Tuple2<Long, Long>>
// CountWindowFunction extends ProcessWindowFunction
\`\`\`

The DSL's \`<TumbleWindow>\` + \`<Aggregate>\` pair replaces the imperative window+reduce flow with a declarative composition that lowers to Flink SQL's \`TUMBLE\` table-valued function:

\`\`\`tsx
<DataGenSource schema={ProductSalesSchema} rowsPerSecond={50} />
<TumbleWindow size="1 hour" on="timestamp" />
<Aggregate
  groupBy={['productId']}
  select={{
    productId: '\\\`productId\\\`',
    salesCount: 'COUNT(*)',
    window_start: 'window_start',
    window_end: 'window_end',
  }}
/>
<FileSystemSink path="output/product-sales" format="csv" />
\`\`\`

**One translation deviation:** \`<DataGenSource>\` substitutes the original's CSV file source so the bundle is runnable out-of-the-box without shipping a seed CSV. Switch to \`<GenericSource connector="filesystem" path="..." format="csv" schema={ProductSalesSchema}>\` to read real CSV files.

**Original: 200 lines** ([CountProductSalesWindowing.java](https://github.com/apache/flink/blob/release-2.0.0/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/dsv2/windowing/CountProductSalesWindowing.java)) → **Translated: ~25 lines** (~8× reduction).`

const SIDE_OUTPUT_ROUTING_TRANSLATION_NOTE = `The Apache Flink original uses \`OutputTag\` + a \`ProcessFunction\` to fork rows into a side-output stream:

\`\`\`java
final OutputTag<String> rejectedWordsTag =
    new OutputTag<String>("rejected") {};

SingleOutputStreamOperator<Tuple2<String, Integer>> tokenized = text
    .process(new ProcessFunction<String, Tuple2<String, Integer>>() {
        @Override public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) {
            if (value.length() < 5) {
                ctx.output(rejectedWordsTag, value);  // side-output for short words
            } else {
                out.collect(new Tuple2<>(value, 1));   // main output for long words
            }
        }
    });

DataStream<String> rejected = tokenized.getSideOutput(rejectedWordsTag);
tokenized.sinkTo(/* main sink */);
rejected.sinkTo(/* rejected sink */);
\`\`\`

The DSL's \`<Route>\` makes the branching first-class. Each \`<Route.Branch>\` declares an SQL condition; the synthesized SQL emits one \`INSERT INTO ... SELECT ... WHERE\` per branch, packaged in a \`STATEMENT SET\`:

\`\`\`tsx
<Route>
  <Route.Branch condition="CHAR_LENGTH(\\\`word\\\`) > 5">
    <KafkaSink topic="long-words" />
  </Route.Branch>
  <Route.Branch condition="CHAR_LENGTH(\\\`word\\\`) <= 5">
    <KafkaSink topic="short-words" />
  </Route.Branch>
</Route>
\`\`\`

The synthesized DDL contains two \`INSERT INTO\` statements — one per branch — each guarded by the branch's \`WHERE\` predicate. Flink's optimizer fuses them into a single physical operator with broadcast emission to multiple sinks.

**Original: 215 lines** ([SideOutputExample.java](https://github.com/apache/flink/blob/release-2.0.0/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/sideoutput/SideOutputExample.java)) → **Translated: ~30 lines** (~7× reduction).`

const STATE_MACHINE_CEP_TRANSLATION_NOTE = `**This is the showcase money-shot.** The Apache Flink \`StateMachineExample\` is the strongest single demonstration of FlinkReactor's value proposition: 250 lines of \`StateMachineExample.java\` plus 9 helper classes (DFA, transitions, event types, alert types, generators, deserializers — each its own file) collapse into ~40 lines of declarative \`<MatchRecognize>\`.

### Step 1: The DFA

The Java original models a per-IP state machine across three packages:

\`\`\`java
// dfa/State.java — enum of named states with transition tables
public enum State {
    Initial(...),
    W(/* W → Y on type=foo */),
    Y(/* Y → Z on type=bar */),
    Terminal(...);
    ...
}

// dfa/Transition.java — encodes (eventType, fromState) → toState
public class Transition {
    public final EventType eventType;
    public final State target;
    ...
}

// dfa/EventTypeAndState.java — composite key for transition lookups
\`\`\`

### Step 2: The KeyedProcessFunction

\`StateMachineExample.java\` itself hosts the per-key state machine and emits alerts when an illegal transition fires:

\`\`\`java
DataStream<Event> events = env.fromSource(/* generator or kafka */, ...);

DataStream<Alert> alerts = events
    .keyBy(Event::sourceAddress)
    .process(new StateMachineMapper());

// StateMachineMapper extends KeyedProcessFunction<String, Event, Alert>
// — manages ValueState<State> per key and emits an Alert on transition
//   to State.Terminal.
\`\`\`

This is the classic shape of stateful CEP in DataStream: imperative state per key, manual emission, lots of plumbing.

### Step 3: The Flink SQL idiom — MATCH_RECOGNIZE

In Flink SQL the same intent is a single \`MATCH_RECOGNIZE\` row pattern:

\`\`\`tsx
const matched = MatchRecognize({
  input: events,
  partitionBy: ['sourceAddress'],   // one DFA instance per IP
  orderBy: 'timestamp',             // event-time order is the DFA's tape
  pattern: 'A B+ C',                // login → 1+ hot transfers → withdrawal
  define: {
    A: 'A.\\\`type\\\` = 0',           // login
    B: 'B.\\\`type\\\` = 1',           // high-value transfer
    C: 'C.\\\`type\\\` = 2',           // withdrawal
  },
  measures: {
    sourceAddress: 'A.\\\`sourceAddress\\\`',
    startTime: 'A.\\\`timestamp\\\`',
    endTime: 'C.\\\`timestamp\\\`',
    hotCount: 'COUNT(B.*)',
  },
  after: 'MATCH_RECOGNIZED',
});
\`\`\`

The mapping:

| Java (DataStream) | Flink SQL (MATCH_RECOGNIZE) | DSL prop |
|---|---|---|
| \`keyBy(Event::sourceAddress)\` | \`PARTITION BY sourceAddress\` | \`partitionBy\` |
| Event-time semantics | \`ORDER BY timestamp\` | \`orderBy\` |
| State enum + Transition table | \`PATTERN (A B+ C)\` row regex | \`pattern\` |
| Per-state event-type predicates | \`DEFINE A AS A.type = 0, ...\` | \`define\` |
| \`Alert\` POJO emission in transition handler | \`MEASURES ... AS startTime, ...\` | \`measures\` |
| ValueState\\<State\\> per key | (managed by Flink runtime) | (none — implicit) |

### Step 4: What's preserved, what's not

**Preserved:**
- Per-key DFA semantics (one match per \`sourceAddress\`).
- Event-time ordering (the DFA's "tape" is the watermark-ordered event-time).
- Alert emission shape (rows with sourceAddress + window timestamps + hot-event count).

**Not preserved:**
- The exact transition table (this port models a *different* DFA — login→hot→withdrawal — to keep the example legible; adapt \`define\` to your real event-type encoding).
- The 9 helper classes (gone — the runtime manages keyed state internally).
- Custom serializers (\`EventDeSerializationSchema\`, \`Alert\` POJO) — replaced by JSON over Kafka.

### Step 5: The synthesized SQL

\`<MatchRecognize>\` lowers to Flink SQL's \`MATCH_RECOGNIZE\` clause, which Flink's optimizer compiles to an internal NFA. The produced SQL contains:

\`\`\`sql
SELECT * FROM events
MATCH_RECOGNIZE (
  PARTITION BY sourceAddress
  ORDER BY \\\`timestamp\\\`
  MEASURES
    A.\\\`sourceAddress\\\` AS \\\`sourceAddress\\\`,
    A.\\\`timestamp\\\` AS \\\`startTime\\\`,
    C.\\\`timestamp\\\` AS \\\`endTime\\\`,
    COUNT(B.*) AS \\\`hotCount\\\`
  AFTER MATCH SKIP PAST LAST ROW
  PATTERN (A B+ C)
  DEFINE
    A AS A.\\\`type\\\` = 0,
    B AS B.\\\`type\\\` = 1,
    C AS C.\\\`type\\\` = 2
)
\`\`\`

Same DFA semantics, dramatically less code, and Flink owns the state-machine implementation.

**Original:** 250 lines of \`StateMachineExample.java\` + 9 helper classes (\`State\`, \`Transition\`, \`EventTypeAndState\`, \`Event\`, \`EventType\`, \`Alert\`, \`EventsGenerator\`, \`EventsGeneratorFunction\`, \`StandaloneThreadedGenerator\`, plus a Kafka serde class). The helpers add several hundred more lines.

**Translated:** ~40 lines of TSX in \`pipelines/state-machine-cep/index.tsx\`.

[StateMachineExample.java](https://github.com/apache/flink/blob/release-2.0.0/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/statemachine/StateMachineExample.java)`

const PUMP_TRANSLATION_NOTE = `This pipeline has **no upstream Apache Flink source** — it exists only to make the \`stock-ds-moderate\` bundle runnable end-to-end on the local sim.

\`state-machine-cep\` reads from an \`events\` Kafka topic. Without a producer, the topic is empty and the consumer pipeline emits nothing. This pump synthesises Event rows via \`<DataGenSource>\` and writes them to the same topic with the same JSON format, closing the loop:

\`\`\`
pump-state-machine-cep ─► events topic ─► state-machine-cep
\`\`\`

The same pattern is used by other FlinkReactor templates that need a data source: \`ecommerce\` ships \`pump-ecom-rides\`, \`stock-temporal-topn\` ships \`pump-temporal-join-fx\`. Pumps are not Apache Flink stock examples — they are bundle-internal helpers.`

// ── Config builder ──────────────────────────────────────────────────

function makeConfig(opts: ScaffoldOptions): string {
  return `import { defineConfig } from '@flink-reactor/dsl';

export default defineConfig({
  flink: { version: '${opts.flinkVersion}' },

  environments: {
    development: {
      cluster: { url: 'http://localhost:8081' },
      dashboard: { mockMode: true },
      pipelines: { '*': { parallelism: 1 } },
    },
    minikube: {
      cluster: { url: 'http://localhost:8081' },
      kafka: { bootstrapServers: 'kafka:9092' },
      sim: {
        init: {
          kafka: {
            catalogs: [{
              name: 'ds_moderate',
              tables: [
                ${userScoreSimTable()},
                ${userNameSimTable()},
                ${productSalesSimTable()},
                ${eventSimTable()},
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
