// `stock-ds-easy` template factory.
//
// Bucket A of the DataStream→FlinkSQL migration showcase: five Apache
// Flink DataStream examples that map directly to existing DSL primitives
// with no idiom shift:
//   • wordcount-streaming      ← WordCount.java
//   • session-windowing        ← SessionWindowing.java
//   • window-wordcount         ← WindowWordCount.java
//   • socket-window-wordcount  ← SocketWindowWordCount.java
//   • window-join              ← WindowJoin.java + WindowJoinSampleData.java
//
// Each per-pipeline README includes a Translation Notes section with a
// Java↔TSX side-by-side excerpt and a LOC comparison line. The bundle
// README rolls those numbers up into a Migration Comparison table.
import type { ScaffoldOptions, TemplateFile } from "@/cli/commands/new.js"
import {
  pipelineReadme,
  sharedFiles,
  templatePipelineTestStub,
} from "./shared.js"
import {
  bundleReadme,
  keyedEventSchemaFile,
  keyedEventSimTable,
  nameGradeSchemaFile,
  nameGradeSimTable,
  nameSalarySchemaFile,
  nameSalarySimTable,
  wordSchemaFile,
  wordSimTable,
} from "./stock-shared/index.js"

// Pinned ref on apache/flink for source links + LOC counts.
// LOC counts are wc -l on the corresponding `*.java` file at this ref.
const FLINK_REF = "release-2.0.0"
const FLINK_DS_BASE = `https://github.com/apache/flink/blob/${FLINK_REF}/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples`

export function getStockDsEasyTemplates(opts: ScaffoldOptions): TemplateFile[] {
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
    keyedEventSchemaFile(),
    nameGradeSchemaFile(),
    nameSalarySchemaFile(),

    // ── Pipelines ────────────────────────────────────────────────────
    {
      path: "pipelines/wordcount-streaming/index.tsx",
      content: WORDCOUNT_STREAMING_PIPELINE,
    },
    {
      path: "pipelines/session-windowing/index.tsx",
      content: SESSION_WINDOWING_PIPELINE,
    },
    {
      path: "pipelines/window-wordcount/index.tsx",
      content: WINDOW_WORDCOUNT_PIPELINE,
    },
    {
      path: "pipelines/socket-window-wordcount/index.tsx",
      content: SOCKET_WINDOW_WORDCOUNT_PIPELINE,
    },
    {
      path: "pipelines/window-join/index.tsx",
      content: WINDOW_JOIN_PIPELINE,
    },

    // ── Per-pipeline READMEs ─────────────────────────────────────────
    pipelineReadme({
      pipelineName: "wordcount-streaming",
      tagline:
        "Continuous keyed aggregation over a streaming word source. Port of `WordCount.java` (~207 Java LOC → ~22 TSX LOC, ~9× reduction).",
      source: `[\`WordCount.java\`](${FLINK_DS_BASE}/wordcount/WordCount.java)`,
      demonstrates: [
        "Synthetic word stream via `<DataGenSource>` (substituted for the original's filesystem reader; see Translation Notes).",
        "Continuous keyed aggregation via `<Aggregate groupBy={['word']}>` — the equivalent of `KeyedStream.sum(1)`.",
        '`<FileSystemSink format="csv">` writing rolling CSV output.',
      ],
      topology: `DataGenSource (Word)
  └── Aggregate (GROUP BY word, SUM(frequency))
        └── FileSystemSink (csv)`,
      schemas: ["`schemas/word.ts` — `{ word: STRING, frequency: INT }`"],
      runCommand: `pnpm synth
pnpm test`,
      translationNotes: WORDCOUNT_STREAMING_TRANSLATION_NOTE,
    }),
    pipelineReadme({
      pipelineName: "session-windowing",
      tagline:
        "Session-window count per key over an event-time stream. Port of `SessionWindowing.java` (~116 Java LOC → ~25 TSX LOC, ~5× reduction).",
      source: `[\`SessionWindowing.java\`](${FLINK_DS_BASE}/sessionwindowing/SessionWindowing.java)`,
      demonstrates: [
        "Event-time `<DataGenSource>` whose schema declares the watermark inline.",
        '3-second inactivity gap via `<SessionWindow gap="3 seconds" on="ts">` — the SQL equivalent of `EventTimeSessionWindows.withGap(...)`.',
        "Per-session count via `<Aggregate groupBy={['id']}>` projecting `window_start`/`window_end`.",
      ],
      topology: `DataGenSource (KeyedEvent, watermark on ts)
  └── SessionWindow (gap=3 seconds, on=ts)
        └── Aggregate (GROUP BY id — COUNT(*), window_start, window_end)
              └── GenericSink (connector="print")`,
      schemas: [
        "`schemas/keyed-event.ts` — `{ id: STRING, ts: TIMESTAMP_LTZ(3), value: INT }` with watermark on `ts`",
      ],
      runCommand: `pnpm synth
pnpm test`,
      translationNotes: SESSION_WINDOWING_TRANSLATION_NOTE,
    }),
    pipelineReadme({
      pipelineName: "window-wordcount",
      tagline:
        "Tumbling-window word count over an event-time stream. Port of `WindowWordCount.java` (~173 Java LOC → ~24 TSX LOC, ~7× reduction).",
      source: `[\`WindowWordCount.java\`](${FLINK_DS_BASE}/windowing/WindowWordCount.java)`,
      demonstrates: [
        "Synthetic word stream via `<DataGenSource>` (the original reads a text file).",
        '5-second tumbling window via `<TumbleWindow size="5 seconds" on="ts">`.',
        "Per-window keyed count via `<Aggregate>`.",
      ],
      topology: `DataGenSource (Word, watermark)
  └── TumbleWindow (5 seconds, on=ts)
        └── Aggregate (GROUP BY word — COUNT(*), window_start, window_end)
              └── GenericSink (connector="print")`,
      schemas: [
        "`schemas/word.ts` — `{ word: STRING, frequency: INT, ts: TIMESTAMP_LTZ(3) }` (an event-time variant declared inline so the watermark feeds the tumble window)",
      ],
      runCommand: `pnpm synth
pnpm test`,
      translationNotes: WINDOW_WORDCOUNT_TRANSLATION_NOTE,
    }),
    pipelineReadme({
      pipelineName: "socket-window-wordcount",
      tagline:
        "Tumbling-window word count over a synthetic socket-style stream. Port of `SocketWindowWordCount.java` (~120 Java LOC → ~24 TSX LOC, ~5× reduction).",
      source: `[\`SocketWindowWordCount.java\`](${FLINK_DS_BASE}/socket/SocketWindowWordCount.java)`,
      demonstrates: [
        "`<DataGenSource>` substituted for the original's `socketTextStream(host, port)` (see Translation Notes).",
        "5-second tumbling window via `<TumbleWindow>`.",
        "Per-window keyed count via `<Aggregate>`.",
      ],
      topology: `DataGenSource (Word, substitutes socketTextStream)
  └── TumbleWindow (5 seconds, on=ts)
        └── Aggregate (GROUP BY word — COUNT(*))
              └── GenericSink (connector="print")`,
      schemas: [
        "`schemas/word.ts` — `{ word: STRING, frequency: INT, ts: TIMESTAMP_LTZ(3) }` (event-time variant for the tumble window)",
      ],
      runCommand: `pnpm synth
pnpm test`,
      translationNotes: SOCKET_WINDOW_WORDCOUNT_TRANSLATION_NOTE,
    }),
    pipelineReadme({
      pipelineName: "window-join",
      tagline:
        "Time-bounded interval join of two keyed streams. Port of `WindowJoin.java` + `WindowJoinSampleData.java` (~234 Java LOC → ~34 TSX LOC, ~7× reduction).",
      source: `[\`WindowJoin.java\`](${FLINK_DS_BASE}/join/WindowJoin.java) + [\`WindowJoinSampleData.java\`](${FLINK_DS_BASE}/join/WindowJoinSampleData.java)`,
      demonstrates: [
        "Two `<DataGenSource>` event-time streams (NameGrade and NameSalary).",
        "Time-bounded join via `<IntervalJoin>` lowering to a `BETWEEN ... AND ...` predicate on the time attributes.",
        '`<FileSystemSink format="csv">` for the joined output.',
      ],
      topology: `DataGenSource (NameGrade, watermark on time)  ─┐
                                              ├─► IntervalJoin (within 2 seconds) ─► FileSystemSink (csv)
DataGenSource (NameSalary, watermark on time) ─┘`,
      schemas: [
        "`schemas/name-grade.ts` — `{ name, grade, time }` with watermark on `time`",
        "`schemas/name-salary.ts` — `{ name, salary, time }` with watermark on `time`",
      ],
      runCommand: `pnpm synth
pnpm test`,
      translationNotes: WINDOW_JOIN_TRANSLATION_NOTE,
    }),

    // ── Per-pipeline snapshot tests ──────────────────────────────────
    templatePipelineTestStub({
      pipelineName: "wordcount-streaming",
      loadBearingPatterns: [/GROUP BY/i, /SUM\(/i, /CREATE TABLE/i],
    }),
    templatePipelineTestStub({
      pipelineName: "session-windowing",
      loadBearingPatterns: [/SESSION/i, /GROUP BY/i],
    }),
    templatePipelineTestStub({
      pipelineName: "window-wordcount",
      loadBearingPatterns: [/TUMBLE/i, /GROUP BY/i],
    }),
    templatePipelineTestStub({
      pipelineName: "socket-window-wordcount",
      loadBearingPatterns: [/TUMBLE/i, /GROUP BY/i],
    }),
    templatePipelineTestStub({
      pipelineName: "window-join",
      loadBearingPatterns: [/BETWEEN/i, /JOIN/i],
    }),

    // ── Project-root README ──────────────────────────────────────────
    withComparisonTable(
      bundleReadme({
        templateName: "stock-ds-easy",
        tagline:
          "FlinkReactor ports of five Apache Flink DataStream API examples that map directly onto existing DSL primitives — wordcount, session-windowing, window-wordcount, socket-window-wordcount, window-join. The marketing pitch: imperative `KeyedStream` / `WindowAssigner` / `ProcessFunction` code becomes declarative SQL synthesis with a 5–9× LOC reduction.",
        pipelines: [
          {
            name: "wordcount-streaming",
            pitch:
              "Continuous keyed aggregation. `KeyedStream.sum(1)` becomes `<Aggregate>` (~207 Java LOC → ~22 TSX LOC).",
            sourceFile: "WordCount.java",
          },
          {
            name: "session-windowing",
            pitch:
              "3-second inactivity-gap session windows. `EventTimeSessionWindows.withGap(...)` becomes `<SessionWindow>` (~116 Java LOC → ~25 TSX LOC).",
            sourceFile: "SessionWindowing.java",
          },
          {
            name: "window-wordcount",
            pitch:
              "Event-time tumbling-window word count. `TumblingEventTimeWindows.of(...)` becomes `<TumbleWindow>` (~173 Java LOC → ~24 TSX LOC).",
            sourceFile: "WindowWordCount.java",
          },
          {
            name: "socket-window-wordcount",
            pitch:
              "Tumbling window over a substituted DataGen source (the SQL Gateway has no `socketTextStream` connector — see Translation Notes) (~120 Java LOC → ~24 TSX LOC).",
            sourceFile: "SocketWindowWordCount.java",
          },
          {
            name: "window-join",
            pitch:
              "Time-bounded interval join. `JoinedStreams.where().equalTo().window(...)` becomes `<IntervalJoin>` (~234 Java LOC across two files → ~34 TSX LOC).",
            sourceFile: "WindowJoin.java + WindowJoinSampleData.java",
          },
        ],
        gettingStarted: ["pnpm install", "pnpm synth", "pnpm test"],
      }),
      EASY_COMPARISON_ROWS,
    ),
  ]
}

// ── Migration Comparison helpers ────────────────────────────────────

const EASY_COMPARISON_ROWS: ReadonlyArray<{
  pipeline: string
  source: string
  javaLoc: number
  tsxLoc: number
}> = [
  {
    pipeline: "wordcount-streaming",
    source: "WordCount.java",
    javaLoc: 207,
    tsxLoc: 22,
  },
  {
    pipeline: "session-windowing",
    source: "SessionWindowing.java",
    javaLoc: 116,
    tsxLoc: 25,
  },
  {
    pipeline: "window-wordcount",
    source: "WindowWordCount.java",
    javaLoc: 173,
    tsxLoc: 24,
  },
  {
    pipeline: "socket-window-wordcount",
    source: "SocketWindowWordCount.java",
    javaLoc: 120,
    tsxLoc: 24,
  },
  {
    pipeline: "window-join",
    source: "WindowJoin.java + WindowJoinSampleData.java",
    javaLoc: 234,
    tsxLoc: 34,
  },
]

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
//
// Each constant is the verbatim TSX source for `pipelines/<name>/index.tsx`.
// Backticks inside the embedded TSX are escaped as `\\\`` so the outer
// template literal here renders cleanly.

const WORDCOUNT_STREAMING_PIPELINE = `import {
  Pipeline,
  DataGenSource,
  Aggregate,
  FileSystemSink,
} from '@flink-reactor/dsl';
import WordSchema from '@/schemas/word';

export default (
  <Pipeline name="wordcount-streaming">
    <DataGenSource schema={WordSchema} rowsPerSecond={20} />
    <Aggregate
      groupBy={['word']}
      select={{
        word: '\\\`word\\\`',
        count: 'SUM(\\\`frequency\\\`)',
      }}
    />
    <FileSystemSink path="output/wordcount" format="csv" />
  </Pipeline>
);
`

const SESSION_WINDOWING_PIPELINE = `import {
  Pipeline,
  DataGenSource,
  SessionWindow,
  Aggregate,
  GenericSink,
} from '@flink-reactor/dsl';
import KeyedEventSchema from '@/schemas/keyed-event';

export default (
  <Pipeline name="session-windowing">
    <DataGenSource schema={KeyedEventSchema} rowsPerSecond={20} />
    <SessionWindow gap="3 seconds" on="ts" />
    <Aggregate
      groupBy={['id']}
      select={{
        id: '\\\`id\\\`',
        events: 'COUNT(*)',
        window_start: 'window_start',
        window_end: 'window_end',
      }}
    />
    <GenericSink connector="print" />
  </Pipeline>
);
`

// WindowWordCount and SocketWindowWordCount both need a TIMESTAMP-typed
// time attribute on the word stream so their tumble windows work. The
// shared \`WordSchema\` is intentionally schema-only ({ word, frequency })
// — these pipelines extend it inline with a \`ts\` event-time column.
const WINDOW_WORDCOUNT_PIPELINE = `import {
  Pipeline,
  Schema,
  Field,
  DataGenSource,
  TumbleWindow,
  Aggregate,
  GenericSink,
} from '@flink-reactor/dsl';

const WordTsSchema = Schema({
  fields: {
    word: Field.STRING(),
    frequency: Field.INT(),
    ts: Field.TIMESTAMP_LTZ(3),
  },
  watermark: { column: 'ts', expression: "ts - INTERVAL '1' SECOND" },
});

export default (
  <Pipeline name="window-wordcount">
    <DataGenSource schema={WordTsSchema} rowsPerSecond={20} />
    <TumbleWindow size="5 seconds" on="ts" />
    <Aggregate
      groupBy={['word']}
      select={{
        word: '\\\`word\\\`',
        count: 'COUNT(*)',
        window_start: 'window_start',
        window_end: 'window_end',
      }}
    />
    <GenericSink connector="print" />
  </Pipeline>
);
`

const SOCKET_WINDOW_WORDCOUNT_PIPELINE = `import {
  Pipeline,
  Schema,
  Field,
  DataGenSource,
  TumbleWindow,
  Aggregate,
  GenericSink,
} from '@flink-reactor/dsl';

// The Apache Flink original reads from a TCP socket via
// \`env.socketTextStream(host, port)\`. The Flink SQL Gateway has no
// registered "socket" connector — see this pipeline's README Translation
// Notes. The substitution preserves the windowing+aggregate shape, which
// is the marketing point.
const WordTsSchema = Schema({
  fields: {
    word: Field.STRING(),
    frequency: Field.INT(),
    ts: Field.TIMESTAMP_LTZ(3),
  },
  watermark: { column: 'ts', expression: "ts - INTERVAL '1' SECOND" },
});

export default (
  <Pipeline name="socket-window-wordcount">
    <DataGenSource schema={WordTsSchema} rowsPerSecond={20} />
    <TumbleWindow size="5 seconds" on="ts" />
    <Aggregate
      groupBy={['word']}
      select={{
        word: '\\\`word\\\`',
        count: 'COUNT(*)',
        window_start: 'window_start',
        window_end: 'window_end',
      }}
    />
    <GenericSink connector="print" />
  </Pipeline>
);
`

const WINDOW_JOIN_PIPELINE = `import {
  Pipeline,
  DataGenSource,
  IntervalJoin,
  FileSystemSink,
} from '@flink-reactor/dsl';
import NameGradeSchema from '@/schemas/name-grade';
import NameSalarySchema from '@/schemas/name-salary';

const grades = (
  <DataGenSource schema={NameGradeSchema} rowsPerSecond={5} name="grades" />
);
const salaries = (
  <DataGenSource schema={NameSalarySchema} rowsPerSecond={5} name="salaries" />
);

// 2-second time-bounded join: a salary row joins with grade rows whose
// time attribute falls in [grades.time, grades.time + INTERVAL '2'
// SECOND]. The lowering produces a BETWEEN ... AND ... predicate.
const joined = IntervalJoin({
  left: grades,
  right: salaries,
  on: '\\\`grades\\\`.\\\`name\\\` = \\\`salaries\\\`.\\\`name\\\`',
  interval: {
    from: '\\\`grades\\\`.\\\`time\\\`',
    to: "\\\`grades\\\`.\\\`time\\\` + INTERVAL '2' SECOND",
  },
  type: 'inner',
});

export default (
  <Pipeline name="window-join">
    <FileSystemSink path="output/window-join" format="csv">
      {joined}
    </FileSystemSink>
  </Pipeline>
);
`

// ── Translation notes ───────────────────────────────────────────────

const WORDCOUNT_STREAMING_TRANSLATION_NOTE = `The Apache Flink original tokenizes text from a file source and runs an imperative \`KeyedStream\` aggregation:

\`\`\`java
DataStream<String> text = env.fromSource(/* file reader */, ...);
DataStream<Tuple2<String, Integer>> counts =
    text.flatMap(new Tokenizer())
        .keyBy(value -> value.f0)
        .sum(1);
counts.sinkTo(/* CSV sink */);
\`\`\`

The DSL ports the \`keyBy(...).sum(1)\` step directly to \`<Aggregate groupBy={['word']}>\` — that is the marketing point. The tokenization step (\`flatMap\`) is bypassed by sourcing already-tokenized \`{word, frequency}\` rows from \`<DataGenSource>\`:

\`\`\`tsx
<DataGenSource schema={WordSchema} rowsPerSecond={20} />
<Aggregate
  groupBy={['word']}
  select={{ word: '\\\`word\\\`', count: 'SUM(\\\`frequency\\\`)' }}
/>
<FileSystemSink path="output/wordcount" format="csv" />
\`\`\`

**Why substitute the source:** the Java original's tokenization is essentially \`SPLIT(line, ' ')\` followed by a count-each-token. In SQL that's \`CROSS JOIN UNNEST(SPLIT(line, ' '))\` — possible via \`<RawSQL>\`, but it shifts focus away from the headline transformation (continuous keyed aggregation) and forces every reader to grok an UNNEST for what is, at the end, still a \`GROUP BY word, SUM(...)\`. Substituting a synthetic word source preserves the marketing point cleanly.

**Original: 207 lines** ([WordCount.java](https://github.com/apache/flink/blob/release-2.0.0/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/wordcount/WordCount.java)) → **Translated: ~22 lines** (~9× reduction).`

const SESSION_WINDOWING_TRANSLATION_NOTE = `The Apache Flink original assigns timestamps to a \`Tuple3<String, Long, Integer>\` source, keys by id, opens an event-time session window with a 3-millisecond gap, and counts:

\`\`\`java
DataStream<Tuple3<String, Long, Integer>> source =
    env.fromData(/* hard-coded events */)
       .assignTimestampsAndWatermarks(WatermarkStrategy
           .<Tuple3<String, Long, Integer>>forMonotonousTimestamps()
           .withTimestampAssigner((event, ts) -> event.f1));

DataStream<Tuple3<String, Long, Integer>> aggregated = source
    .keyBy(value -> value.f0)
    .window(EventTimeSessionWindows.withGap(Duration.ofMillis(3L)))
    .sum(2);
\`\`\`

The DSL ports this 1:1 — \`EventTimeSessionWindows.withGap\` becomes \`<SessionWindow gap=...>\`, the \`keyBy(...).sum(...)\` becomes \`<Aggregate groupBy={['id']}>\`, and the watermark strategy folds into the schema declaration:

\`\`\`tsx
<DataGenSource schema={KeyedEventSchema} rowsPerSecond={20} />
<SessionWindow gap="3 seconds" on="ts" />
<Aggregate
  groupBy={['id']}
  select={{
    id: '\\\`id\\\`',
    events: 'COUNT(*)',
    window_start: 'window_start',
    window_end: 'window_end',
  }}
/>
<GenericSink connector="print" />
\`\`\`

The synthesized SQL contains a \`SESSION\` table function over the source — Flink's canonical session-window TVF.

**Original: 116 lines** ([SessionWindowing.java](https://github.com/apache/flink/blob/release-2.0.0/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/sessionwindowing/SessionWindowing.java)) → **Translated: ~25 lines** (~5× reduction).

> **Gap unit note:** the Java original uses \`Duration.ofMillis(3L)\` (3 milliseconds) — likely contrived for the in-memory test data. The TSX port uses \`gap="3 seconds"\` because Flink SQL's SESSION TVF requires INTERVAL units of seconds or larger; sub-second session gaps are rarely meaningful in practice anyway.`

const WINDOW_WORDCOUNT_TRANSLATION_NOTE = `The Apache Flink original reads text, tokenizes, keys by word, opens an event-time tumbling window of N records, and sums:

\`\`\`java
DataStream<Tuple2<String, Integer>> windowCounts =
    text.flatMap(new Tokenizer())
        .keyBy(value -> value.f0)
        .countWindow(windowSize, slideSize)   // count window!
        .sum(1);
\`\`\`

Two deliberate translation choices:

**1. Time-window in place of count-window.** The Apache Flink original uses \`countWindow(windowSize, slideSize)\` — a *count-based* sliding window. Flink SQL has no count-window TVF; only \`TUMBLE\`, \`HOP\`, \`SESSION\`, and \`CUMULATE\` (all time-based). The port substitutes a 5-second tumbling window via \`<TumbleWindow size="5 seconds" on="ts">\`, which preserves the windowed-aggregate shape but switches the trigger axis from "every N records per key" to "every 5s of event-time". Honest documentation: this is the canonical SQL idiom for periodic windowed counts.

**2. DataGen source in place of file source.** Same reasoning as \`wordcount-streaming\`: tokenization isn't the marketing point.

\`\`\`tsx
<DataGenSource schema={WordTsSchema} rowsPerSecond={20} />
<TumbleWindow size="5 seconds" on="ts" />
<Aggregate
  groupBy={['word']}
  select={{ word: '\\\`word\\\`', count: 'COUNT(*)' }}
/>
<GenericSink connector="print" />
\`\`\`

**Original: 173 lines** ([WindowWordCount.java](https://github.com/apache/flink/blob/release-2.0.0/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/windowing/WindowWordCount.java)) → **Translated: ~24 lines** (~7× reduction).`

const SOCKET_WINDOW_WORDCOUNT_TRANSLATION_NOTE = `The Apache Flink original reads from a TCP socket and runs a 5-second tumbling window:

\`\`\`java
DataStream<String> text = env.socketTextStream(hostname, port, "\\\\n");
DataStream<WordWithCount> windowCounts = text
    .flatMap((line, out) -> {
        for (String word : line.split("\\\\s")) out.collect(new WordWithCount(word, 1L));
    })
    .keyBy(value -> value.word)
    .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)))
    .reduce((a, b) -> new WordWithCount(a.word, a.count + b.count));
\`\`\`

**Two translation deviations:**

**1. No socket connector.** The Apache Flink \`socketTextStream(host, port)\` is a DataStream-API-only helper. The Flink SQL Gateway / Table API ships \`filesystem\`, \`kafka\`, \`jdbc\`, \`datagen\`, \`upsert-kafka\`, etc. — but no \`socket\` source. So the port substitutes \`<DataGenSource>\` for the socket reader and documents the substitution here. If you have a real TCP word source, the typical bridge is to run a tiny producer that writes lines to a Kafka topic and replace the \`<DataGenSource>\` with \`<KafkaSource topic="words">\`.

**2. Tumbling event-time instead of processing-time.** \`TumblingProcessingTimeWindows.of(...)\` becomes \`<TumbleWindow size="5 seconds" on="ts">\`. The DSL prefers event-time semantics; the schema declares a watermark on \`ts\` to make the window event-time-correct. Switching to processing-time would require a different time attribute (e.g., \`PROCTIME()\` as a generated column).

\`\`\`tsx
<DataGenSource schema={WordTsSchema} rowsPerSecond={20} />
<TumbleWindow size="5 seconds" on="ts" />
<Aggregate groupBy={['word']} select={{ word: '\\\`word\\\`', count: 'COUNT(*)' }} />
<GenericSink connector="print" />
\`\`\`

**Original: 120 lines** ([SocketWindowWordCount.java](https://github.com/apache/flink/blob/release-2.0.0/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/socket/SocketWindowWordCount.java)) → **Translated: ~24 lines** (~5× reduction).`

const WINDOW_JOIN_TRANSLATION_NOTE = `The Apache Flink original sets up two keyed event-time streams and joins them with a sliding 2-second window:

\`\`\`java
DataStream<Tuple2<String, Integer>> grades = WindowJoinSampleData.GradeSource.getSource(env);
DataStream<Tuple2<String, Integer>> salaries = WindowJoinSampleData.SalarySource.getSource(env);

DataStream<Tuple3<String, Integer, Integer>> joined = grades
    .join(salaries)
    .where(grade -> grade.f0)
    .equalTo(salary -> salary.f0)
    .window(TumblingEventTimeWindows.of(Duration.ofMillis(windowSize)))
    .apply(new JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<...>>() {
        @Override public Tuple3<String, Integer, Integer> join(Tuple2<String, Integer> g, Tuple2<String, Integer> s) {
            return new Tuple3<>(g.f0, g.f1, s.f1);
        }
    });
\`\`\`

The DSL's \`<IntervalJoin>\` lowers to a stream-to-stream join with a \`BETWEEN ... AND ...\` time predicate — semantically a *time-bounded* join rather than the Apache Flink original's *windowed* join, but the practical effect is the same: each grade row is joined with salary rows whose time falls within the bounded interval.

\`\`\`tsx
const joined = IntervalJoin({
  left: grades,
  right: salaries,
  on: '\\\`grades\\\`.\\\`name\\\` = \\\`salaries\\\`.\\\`name\\\`',
  interval: {
    from: '\\\`grades\\\`.\\\`time\\\`',
    to: "\\\`grades\\\`.\\\`time\\\` + INTERVAL '2' SECOND",
  },
  type: 'inner',
});
\`\`\`

The synthesized SQL contains a \`JOIN ... ON ... AND \\\`salaries\\\`.\\\`time\\\` BETWEEN \\\`grades\\\`.\\\`time\\\` AND \\\`grades\\\`.\\\`time\\\` + INTERVAL '2' SECOND\` — the canonical Flink SQL idiom for interval joins.

**Original: 234 lines** across two files ([WindowJoin.java](https://github.com/apache/flink/blob/release-2.0.0/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/join/WindowJoin.java) + [WindowJoinSampleData.java](https://github.com/apache/flink/blob/release-2.0.0/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/join/WindowJoinSampleData.java)) → **Translated: ~34 lines** (~7× reduction).`

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
              name: 'ds_easy',
              tables: [
                ${wordSimTable()},
                ${keyedEventSimTable()},
                ${nameGradeSimTable()},
                ${nameSalarySimTable()},
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
