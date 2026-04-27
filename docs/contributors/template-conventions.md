# Template Doc & Test Conventions

This document defines the per-template artifact contract that every
`create-fr-app` template must follow. Templates live in
`src/cli/templates/<name>.ts` and return a `TemplateFile[]` from a
`getXxxTemplates(opts)` factory function. The shared utilities in
`src/cli/templates/shared.ts` provide reusable builders for the README and
test artifacts described below.

## The four required artifacts

Every template factory must emit:

1. **Project-root `README.md`** — built via `templateReadme(opts)`.
2. **Per-pipeline `pipelines/<name>/README.md`** — built via `pipelineReadme(opts)`,
   one per pipeline directory.
3. **Per-pipeline snapshot test at `tests/pipelines/<name>.test.ts`** — built via
   `templatePipelineTestStub(opts)`, one per pipeline directory.
4. **`EXPECTED_PIPELINES` enrolment** — every template name must appear in the
   `EXPECTED_PIPELINES` map in
   `src/cli/templates/__tests__/helpers/scaffold-and-synth.ts`. The value is
   the sorted list of pipeline directory names the template ships. Adding,
   removing, or renaming a pipeline requires updating this map.

The cross-template integration test
(`src/cli/templates/__tests__/template-scaffold-synth.test.ts`) iterates over
`EXPECTED_PIPELINES` and synthesizes every template, so enrolment is what
makes a new template visible to CI.

## Project-root README

Use `templateReadme(opts)` to build it. Required fields:

- `templateName` — the slug used in the scaffolder (`'cdc-lakehouse'`, `'banking'`, …).
- `tagline` — one sentence describing what the template demonstrates.
- `pipelines` — array of `{ name, pitch }` entries, one per pipeline in
  the template (including data pumps).

Optional fields: `prerequisites` (rendered as a bullet list), `gettingStarted`
(rendered as a fenced `bash` block).

### Example

```ts
import { templateReadme } from '../shared.js'

templateReadme({
  templateName: 'cdc-lakehouse',
  tagline: 'Debezium CDC → Iceberg lakehouse upsert showcase.',
  pipelines: [
    { name: 'cdc-to-lakehouse', pitch: 'Postgres → Debezium → Kafka → Iceberg (upsert v2)' },
  ],
  prerequisites: [
    'Postgres reachable on `postgres:5432` with the Debezium connector configured',
    'Lakekeeper REST catalog at `http://lakekeeper:8181/catalog`',
  ],
  gettingStarted: [
    'pnpm install',
    'flink-reactor synth          # Preview generated SQL',
    'flink-reactor deploy --env dev',
  ],
})
```

This emits:

```markdown
# Cdc Lakehouse

Debezium CDC → Iceberg lakehouse upsert showcase.

## Pipelines

- **cdc-to-lakehouse** — Postgres → Debezium → Kafka → Iceberg (upsert v2)

## Prerequisites

- Postgres reachable on `postgres:5432` with the Debezium connector configured
- Lakekeeper REST catalog at `http://lakekeeper:8181/catalog`

## Getting Started

\`\`\`bash
pnpm install
flink-reactor synth          # Preview generated SQL
flink-reactor deploy --env dev
\`\`\`
```

## Per-pipeline README (7 sections)

Use `pipelineReadme(opts)` to build it. The structure is intentionally fixed
so READMEs stay scannable and copy-pasteable.

Section order:

1. **Title + pitch** (`pipelineName`, `tagline`)
2. **Source** (optional, `source`) — link or attribution to a Flink Java
   original where applicable. Omitted when not supplied.
3. **What it demonstrates** (`demonstrates`) — bullet list of concepts.
4. **Topology** (`topology`) — ASCII diagram fenced with triple backticks.
5. **Schemas** (`schemas`) — bullet list of schema files used.
6. **Run locally** (`runCommand`) — fenced `bash` block.
7. **Expected Output** (optional, `expectedOutput`) — sample of what the
   pipeline emits. Omitted when not supplied.
8. **Translation Notes** (optional, `translationNotes`) — only when the
   pipeline deviates from a known source. This is the appendix for the
   DataStream-migration showcase templates.

### Example

```ts
pipelineReadme({
  pipelineName: 'word-count',
  tagline: 'Streaming word counts over a tumbling window — the canonical Flink hello-world.',
  source: 'Adapted from `WordCount.java` in the Apache Flink examples.',
  demonstrates: [
    'KafkaSource → tokenize → tumbling-window aggregate → KafkaSink',
    'Group-by aggregation with a 10s window',
  ],
  topology: 'Kafka(words) → Tokenize → TumbleWindow(10s) → Aggregate → Kafka(counts)',
  schemas: ['`schemas/words.ts` — WordSchema (word: STRING, ts: TIMESTAMP(3))'],
  runCommand: 'pnpm dev',
  expectedOutput: 'window_end=2026-01-01T00:00:10Z, word=hello, count=42',
  translationNotes: 'The Java original used count windows; this DSL version uses event-time tumbling windows for clarity.',
})
```

## Per-pipeline snapshot test

Use `templatePipelineTestStub(opts)` to build the test file. The emitted
test:

- Imports `synthesizeApp` and `resetNodeIdCounter` from `@flink-reactor/dsl`.
- Imports the pipeline default export by relative path.
- Calls `resetNodeIdCounter()` in `beforeEach` to make node IDs deterministic
  across runs (a snapshot would otherwise drift on every test re-run).
- Defines a local `synth(pipeline)` helper that returns the SQL string.
- Calls `expect(synth(pipeline)).toMatchSnapshot()`.
- Emits one `expect(sql).toMatch(<pattern>)` per regex in
  `loadBearingPatterns`.

### Why both a snapshot AND `toMatch` assertions?

Snapshot-only tests are vulnerable to silent drift on `vitest -u` — a
pipeline could lose its window or sink during a refactor and the
regenerated snapshot would absorb the change with nobody noticing. The
`toMatch` regexes anchor the test to its semantic intent: pick 2-3 SQL
constructs the pipeline cannot lose without changing meaning (`TUMBLE`,
`MATCH_RECOGNIZE`, `INSERT INTO`, `GROUP BY`, etc.) and pin them.

### Example

```ts
templatePipelineTestStub({
  pipelineName: 'word-count',
  loadBearingPatterns: [/GROUP BY/, /SUM\(/, /TUMBLE\(/],
})
```

This emits:

```ts
import { beforeEach, describe, expect, it } from 'vitest'
import {
  type ConstructNode,
  resetNodeIdCounter,
  synthesizeApp,
} from '@flink-reactor/dsl'
import pipeline from '../../pipelines/word-count/index.js'

function synth(node: ConstructNode): string {
  const result = synthesizeApp({ children: [node] })
  return result.artifacts[0].sql.sql
}

describe('word-count pipeline', () => {
  beforeEach(() => {
    resetNodeIdCounter()
  })

  it('synthesizes stable SQL', () => {
    const sql = synth(pipeline)

    expect(sql).toMatchSnapshot()

    expect(sql).toMatch(/GROUP BY/)
    expect(sql).toMatch(/SUM\(/)
    expect(sql).toMatch(/TUMBLE\(/)
  })
})
```

## A complete template factory using all three helpers

```ts
import type { ScaffoldOptions, TemplateFile } from '@/cli/commands/new.js'
import {
  pipelineReadme,
  sharedFiles,
  templatePipelineTestStub,
  templateReadme,
} from './shared.js'

export function getWordCountTemplates(opts: ScaffoldOptions): TemplateFile[] {
  return [
    ...sharedFiles(opts),
    {
      path: 'schemas/words.ts',
      content: `/* … schema definition … */`,
    },
    {
      path: 'pipelines/word-count/index.tsx',
      content: `/* … pipeline … */`,
    },
    templateReadme({
      templateName: 'word-count',
      tagline: 'Streaming word counts — Flink hello-world ported to FlinkReactor.',
      pipelines: [{ name: 'word-count', pitch: 'tumbling-window word counts' }],
    }),
    pipelineReadme({
      pipelineName: 'word-count',
      tagline: 'Streaming word counts over a 10s tumbling window.',
      demonstrates: ['KafkaSource', 'TumbleWindow', 'Aggregate'],
      topology: 'Kafka(words) → Tokenize → TumbleWindow(10s) → Aggregate → Kafka(counts)',
      schemas: ['`schemas/words.ts` — WordSchema'],
      runCommand: 'pnpm dev',
    }),
    templatePipelineTestStub({
      pipelineName: 'word-count',
      loadBearingPatterns: [/GROUP BY/, /TUMBLE\(/],
    }),
  ]
}
```

Don't forget to add `'word-count'` to the `EXPECTED_PIPELINES` map in
`src/cli/templates/__tests__/helpers/scaffold-and-synth.ts` — without
this, the cross-template integration test will not synthesize the new
template.

## Checklist for template authors

Copy-paste this into your PR description when adding a new template:

```markdown
- [ ] Project-root `README.md` built via `templateReadme()`
- [ ] Per-pipeline `README.md` built via `pipelineReadme()` for every pipeline directory
- [ ] Per-pipeline snapshot test built via `templatePipelineTestStub()` with at least 2 load-bearing `toMatch` regexes
- [ ] Template name added to `EXPECTED_PIPELINES` in `scaffold-and-synth.ts`
- [ ] `pnpm build`, `pnpm test`, `pnpm lint` all pass
- [ ] Manually scaffolded the template once and confirmed `pnpm install` + `pnpm test` works inside the scaffolded project
```
