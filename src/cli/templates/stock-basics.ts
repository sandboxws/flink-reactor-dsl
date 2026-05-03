// `stock-basics` template factory.
//
// Ports the four introductory examples from
// `flink-examples-table/.../basics/` (Apache Flink) to the FlinkReactor DSL:
//   • wordcount-sql        ← WordCountSQLExample.java
//   • getting-started      ← GettingStartedExample.java
//   • stream-sql-union     ← StreamSQLExample.java
//   • stream-window-sql    ← StreamWindowSQLExample.java
//
// Per-pipeline READMEs document each translation choice (RawSQL/VALUES,
// inline SQL UDF, DataGen instead of CSV) under their Translation Notes.
import type { ScaffoldOptions, TemplateFile } from "@/cli/commands/new.js"
import {
  pipelineReadme,
  sharedFiles,
  templatePipelineTestStub,
} from "./shared.js"
import {
  bundleReadme,
  customerSchemaFile,
  customerSimTable,
  orderSchemaFile,
  orderSimTable,
  wordSchemaFile,
  wordSimTable,
} from "./stock-shared/index.js"

// Pinned ref on apache/flink for source links + LOC counts.
// LOC counts are wc -l on the corresponding `*.java` file at this ref.
const FLINK_REF = "release-2.0.0"
const FLINK_BASICS_BASE = `https://github.com/apache/flink/blob/${FLINK_REF}/flink-examples/flink-examples-table/src/main/java/org/apache/flink/table/examples/java/basics`

export function getStockBasicsTemplates(opts: ScaffoldOptions): TemplateFile[] {
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
    wordSchemaFile(),
    orderSchemaFile(),
    customerSchemaFile(),

    // ── Pipelines ────────────────────────────────────────────────────
    {
      path: "pipelines/wordcount-sql/index.tsx",
      content: WORDCOUNT_SQL_PIPELINE,
    },
    {
      path: "pipelines/getting-started/index.tsx",
      content: GETTING_STARTED_PIPELINE,
    },
    {
      path: "pipelines/stream-sql-union/index.tsx",
      content: STREAM_SQL_UNION_PIPELINE,
    },
    {
      path: "pipelines/stream-window-sql/index.tsx",
      content: STREAM_WINDOW_SQL_PIPELINE,
    },

    // ── Per-pipeline READMEs ─────────────────────────────────────────
    pipelineReadme({
      pipelineName: "wordcount-sql",
      tagline:
        "Static `VALUES` literal → GROUP BY word, SUM frequency. Port of `WordCountSQLExample.java` (~48 Java LOC → ~20 TSX LOC).",
      source: `[\`WordCountSQLExample.java\`](${FLINK_BASICS_BASE}/WordCountSQLExample.java)`,
      demonstrates: [
        "Inline static input via `<RawSQL>` (the DSL has no `<Values>` source primitive).",
        "Keyed aggregation: GROUP BY + SUM.",
        '`<GenericSink connector="print">` for stdout output during local runs.',
      ],
      topology: `RawSQL (VALUES literal)
  └── Aggregate (GROUP BY word, SUM(frequency))
        └── GenericSink (connector="print")`,
      schemas: ["`schemas/word.ts` — `{ word: STRING, frequency: INT }`"],
      runCommand: `pnpm synth
pnpm test`,
      expectedOutput: `+I[Hello, 9]
+I[World, 6]`,
      translationNotes: WORDCOUNT_TRANSLATION_NOTE,
    }),
    pipelineReadme({
      pipelineName: "getting-started",
      tagline:
        "Filter + project on a Customer stream. Port of `GettingStartedExample.java` (~225 Java LOC → ~30 TSX LOC).",
      source: `[\`GettingStartedExample.java\`](${FLINK_BASICS_BASE}/GettingStartedExample.java)`,
      demonstrates: [
        "Synthetic event source via `<DataGenSource>`.",
        "SQL `WHERE` predicate via `<Filter>`.",
        "Inline SQL `UPPER(REGEXP_REPLACE(...))` substitution for a Java `ScalarFunction` UDF.",
      ],
      topology: `DataGenSource (Customer)
  └── Filter (adult AND has_newsletter)
        └── Map (UPPER(REGEXP_REPLACE(street, ' +', ' ')))
              └── GenericSink (connector="print")`,
      schemas: [
        "`schemas/customer.ts` — `{ name, date_of_birth, street, zip_code, city, gender, has_newsletter }`",
      ],
      runCommand: `pnpm synth
pnpm test`,
      translationNotes: GETTING_STARTED_TRANSLATION_NOTE,
    }),
    pipelineReadme({
      pipelineName: "stream-sql-union",
      tagline:
        "Two Order streams, filtered then UNION ALL merged. Port of `StreamSQLExample.java` (~128 Java LOC → ~35 TSX LOC).",
      source: `[\`StreamSQLExample.java\`](${FLINK_BASICS_BASE}/StreamSQLExample.java)`,
      demonstrates: [
        "Multi-source DAG with two `<DataGenSource>` Order branches.",
        "Per-branch `<Filter>` predicates.",
        "Stream merge via `<Union>` (UNION ALL semantics).",
      ],
      topology: `DataGenSource (orders_a) ──► Filter (amount > 2)              ─┐
                                                              ├─► Union ─► GenericSink
DataGenSource (orders_b) ──► Filter (product LIKE '%Rubber%') ─┘`,
      schemas: [
        "`schemas/order.ts` — `{ user: BIGINT, product: STRING, amount: INT, ts: TIMESTAMP(3) }` with watermark",
      ],
      runCommand: `pnpm synth
pnpm test`,
    }),
    pipelineReadme({
      pipelineName: "stream-window-sql",
      tagline:
        "Event-time tumbling window over an Order stream. Port of `StreamWindowSQLExample.java` (~99 Java LOC → ~30 TSX LOC).",
      source: `[\`StreamWindowSQLExample.java\`](${FLINK_BASICS_BASE}/StreamWindowSQLExample.java)`,
      demonstrates: [
        "Event-time `<DataGenSource>` with watermark from the schema declaration.",
        '1-hour tumbling window via `<TumbleWindow size="1 HOUR">`.',
        "Windowed aggregations: `COUNT(*)`, `SUM(amount)`, `COUNT(DISTINCT product)`.",
      ],
      topology: `DataGenSource (Order, watermark on ts)
  └── TumbleWindow (1 HOUR on ts)
        └── Aggregate (GROUP BY user)
              └── GenericSink (connector="print")`,
      schemas: [
        "`schemas/order.ts` — same as `stream-sql-union`; the watermark declaration is what makes the tumble window event-time-correct.",
      ],
      runCommand: `pnpm synth
pnpm test`,
      translationNotes: STREAM_WINDOW_TRANSLATION_NOTE,
    }),

    // ── Per-pipeline snapshot tests ──────────────────────────────────
    templatePipelineTestStub({
      pipelineName: "wordcount-sql",
      loadBearingPatterns: [/GROUP BY/i, /SUM\(/i, /VALUES/i],
    }),
    templatePipelineTestStub({
      pipelineName: "getting-started",
      loadBearingPatterns: [/REGEXP_REPLACE/i, /UPPER\(/i],
    }),
    templatePipelineTestStub({
      pipelineName: "stream-sql-union",
      loadBearingPatterns: [/UNION ALL/i, /Rubber/i],
    }),
    templatePipelineTestStub({
      pipelineName: "stream-window-sql",
      loadBearingPatterns: [/TUMBLE\(/i, /COUNT\(DISTINCT /i, /WATERMARK FOR/i],
    }),

    // ── Project-root README ──────────────────────────────────────────
    bundleReadme({
      templateName: "stock-basics",
      tagline:
        "FlinkReactor ports of the four introductory `basics/` Apache Flink Table API examples — wordcount, getting-started, stream-sql-union, stream-window-sql.",
      pipelines: [
        {
          name: "wordcount-sql",
          pitch:
            "Static VALUES → GROUP BY word, SUM frequency (~48 Java LOC → ~25 TSX LOC).",
          sourceFile: "WordCountSQLExample.java",
        },
        {
          name: "getting-started",
          pitch:
            "Filter + project on Customer stream; inline-SQL UDF substitution (~225 Java LOC → ~30 TSX LOC).",
          sourceFile: "GettingStartedExample.java",
        },
        {
          name: "stream-sql-union",
          pitch:
            "Two Order streams, filtered and UNION ALL merged (~128 Java LOC → ~35 TSX LOC).",
          sourceFile: "StreamSQLExample.java",
        },
        {
          name: "stream-window-sql",
          pitch:
            "1-hour tumbling window over an Order stream with watermark (~99 Java LOC → ~30 TSX LOC).",
          sourceFile: "StreamWindowSQLExample.java",
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

const WORDCOUNT_SQL_PIPELINE = `import {
  Pipeline,
  RawSQL,
  Aggregate,
  GenericSink,
} from '@flink-reactor/dsl';
import WordSchema from '@/schemas/word';

export default (
  <Pipeline name="wordcount-sql">
    <RawSQL
      sql={\`SELECT \\\`word\\\`, \\\`frequency\\\` FROM (VALUES
        ('Hello', 1),
        ('World', 2),
        ('Hello', 3),
        ('World', 4),
        ('Hello', 5)
      ) AS T(\\\`word\\\`, \\\`frequency\\\`)\`}
      outputSchema={WordSchema}
    />
    <Aggregate
      groupBy={['word']}
      select={{
        word: '\\\`word\\\`',
        frequency: 'SUM(\\\`frequency\\\`)',
      }}
    />
    <GenericSink connector="print" />
  </Pipeline>
);
`

const GETTING_STARTED_PIPELINE = `import {
  Pipeline,
  DataGenSource,
  Filter,
  Map,
  GenericSink,
} from '@flink-reactor/dsl';
import CustomerSchema from '@/schemas/customer';

export default (
  <Pipeline name="getting-started">
    <DataGenSource schema={CustomerSchema} rowsPerSecond={5} />
    {/* JSX attributes are not JS strings, so column references in
        condition= are written without backticks; date_of_birth and
        has_newsletter are not SQL-reserved identifiers. */}
    <Filter
      condition="TIMESTAMPDIFF(YEAR, date_of_birth, CURRENT_DATE) >= 18 AND has_newsletter = TRUE"
    />
    <Map
      select={{
        name: 'name',
        normalized_address: "UPPER(REGEXP_REPLACE(street, ' +', ' '))",
        city: 'city',
      }}
    />
    <GenericSink connector="print" />
  </Pipeline>
);
`

const STREAM_SQL_UNION_PIPELINE = `import {
  Pipeline,
  DataGenSource,
  Filter,
  Union,
  GenericSink,
} from '@flink-reactor/dsl';
import OrderSchema from '@/schemas/order';

const ordersA = DataGenSource({
  schema: OrderSchema,
  name: 'orders_a',
  rowsPerSecond: 5,
});

const ordersB = DataGenSource({
  schema: OrderSchema,
  name: 'orders_b',
  rowsPerSecond: 5,
});

const filteredA = Filter({
  condition: '\\\`amount\\\` > 2',
  children: ordersA,
});

const filteredB = Filter({
  condition: "\\\`product\\\` LIKE '%Rubber%'",
  children: ordersB,
});

export default (
  <Pipeline name="stream-sql-union">
    <Union>
      {filteredA}
      {filteredB}
    </Union>
    <GenericSink connector="print" />
  </Pipeline>
);
`

const STREAM_WINDOW_SQL_PIPELINE = `import {
  Pipeline,
  DataGenSource,
  TumbleWindow,
  Aggregate,
  GenericSink,
} from '@flink-reactor/dsl';
import OrderSchema from '@/schemas/order';

export default (
  <Pipeline name="stream-window-sql">
    <DataGenSource schema={OrderSchema} rowsPerSecond={10} />
    <TumbleWindow size="1 HOUR" on="ts" />
    <Aggregate
      groupBy={['user']}
      select={{
        user: '\\\`user\\\`',
        order_count: 'COUNT(*)',
        total_amount: 'SUM(\\\`amount\\\`)',
        distinct_products: 'COUNT(DISTINCT \\\`product\\\`)',
        window_start: 'window_start',
        window_end: 'window_end',
      }}
    />
    <GenericSink connector="print" />
  </Pipeline>
);
`

// ── Translation notes ───────────────────────────────────────────────

const WORDCOUNT_TRANSLATION_NOTE = `The Apache Flink original constructs an input table via \`tEnv.fromValues(...)\`:

\`\`\`java
final Table inputTable = tEnv.fromValues(
    DataTypes.ROW(
        DataTypes.FIELD("word", DataTypes.STRING()),
        DataTypes.FIELD("frequency", DataTypes.INT())),
    Row.of("Hello", 1),
    Row.of("World", 2),
    Row.of("Hello", 3),
    Row.of("World", 4),
    Row.of("Hello", 5));
\`\`\`

The DSL has no \`<Values>\` source primitive (per the design — adding one is a separate concern), so the \`VALUES\` literal is inlined via the \`<RawSQL>\` escape hatch with no \`inputs\` (the SQL body is self-contained):

\`\`\`tsx
<RawSQL
  sql={\`SELECT \\\`word\\\`, \\\`frequency\\\` FROM (VALUES
    ('Hello', 1), ('World', 2), ('Hello', 3), ('World', 4), ('Hello', 5)
  ) AS T(\\\`word\\\`, \\\`frequency\\\`)\`}
  outputSchema={WordSchema}
/>
\`\`\`

\`<RawSQL>\` participates in sibling-chain JSX, so it sits as a direct child of \`<Pipeline>\` and the downstream \`<Aggregate>\`/\`<GenericSink>\` wire automatically.`

const GETTING_STARTED_TRANSLATION_NOTE = `The Apache Flink original defines a Java \`ScalarFunction\` UDF to normalize the address string:

\`\`\`java
public static class AddressNormalizer extends ScalarFunction {
    public String eval(String s) {
        return s.replaceAll(" +", " ").trim().toUpperCase();
    }
}
// ... registered via tableEnv.createTemporarySystemFunction("AddressNormalizer", ...)
//     and called as: \`AddressNormalizer(street)\`
\`\`\`

The DSL's \`<UDF>\` component requires a JAR path (no JS-defined UDFs). For a getting-started pipeline that's a hostile onboarding step, so the same normalization is expressed inline in SQL:

\`\`\`tsx
<Map select={{
  normalized_address: "UPPER(REGEXP_REPLACE(\\\`street\\\`, ' +', ' '))",
}} />
\`\`\`

This captures the same intent — replace runs of spaces with a single space, then upper-case — entirely in Flink SQL, with no extra build step.`

const STREAM_WINDOW_TRANSLATION_NOTE = `The Apache Flink original reads from a CSV file via the filesystem connector:

\`\`\`sql
CREATE TABLE orders (
  user BIGINT, product STRING, amount INT, ts TIMESTAMP(3),
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
  'connector' = 'filesystem',
  'path' = '/path/to/orders.csv',
  'format' = 'csv'
);
\`\`\`

This adaptation swaps the filesystem source for a \`<DataGenSource>\` so the pipeline runs out-of-the-box with no companion CSV file:

\`\`\`tsx
<DataGenSource schema={OrderSchema} rowsPerSecond={10} />
\`\`\`

\`OrderSchema\` declares the watermark via the schema's \`watermark\` clause, so the synthesized \`CREATE TABLE\` still includes the \`WATERMARK FOR \\\`ts\\\` AS \\\`ts\\\` - INTERVAL '5' SECOND\` declaration the tumbling window depends on.`

// ── Config builder ──────────────────────────────────────────────────

function makeConfig(opts: ScaffoldOptions): string {
  return `import { defineConfig } from '@flink-reactor/dsl';

export default defineConfig({
  flink: { version: '${opts.flinkVersion}' },

  // Kafka-only template.
  services: { kafka: {} },

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
              name: 'basics',
              tables: [
                ${wordSimTable()},
                ${orderSimTable()},
                ${customerSimTable()},
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
