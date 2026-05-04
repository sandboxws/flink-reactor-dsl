// Shared template-factory utilities.
//
// The README and snapshot-test helpers below (`templateReadme`,
// `pipelineReadme`, `templatePipelineTestStub`) implement the per-template
// artifact contract documented at `docs/contributors/template-conventions.md`.
// New template factories should compose these helpers to keep template output
// consistent across the scaffolder.
import type { ScaffoldOptions, TemplateFile } from "@/cli/commands/new.js"

// Injected at build time by tsup/esbuild — falls back for tsx/vitest
declare const __DSL_VERSION__: string
export const DSL_VERSION: string =
  typeof __DSL_VERSION__ !== "undefined" ? __DSL_VERSION__ : "0.1.0"

export function makePackageJson(
  opts: ScaffoldOptions,
  extra?: Record<string, unknown>,
): string {
  const scripts: Record<string, string> = {
    dev: "flink-reactor dev",
    synth: "flink-reactor synth",
    validate: "flink-reactor validate",
    test: "vitest run",
    "test:watch": "vitest",
  }

  const dependencies: Record<string, string> = {
    "@flink-reactor/dsl": `^${DSL_VERSION}`,
  }

  const pkg: Record<string, unknown> = {
    name: opts.projectName,
    version: "0.1.0",
    private: true,
    type: "module",
    scripts,
    dependencies,
    devDependencies: {
      typescript: "^5.7.0",
      vitest: "^3.0.0",
    },
    ...extra,
  }
  return `${JSON.stringify(pkg, null, 2)}\n`
}

export function makeTsconfig(_opts: ScaffoldOptions): string {
  const config = {
    compilerOptions: {
      target: "ES2022",
      module: "ESNext",
      moduleResolution: "bundler",
      lib: ["ES2022"],
      strict: true,
      esModuleInterop: true,
      skipLibCheck: true,
      forceConsistentCasingInFileNames: true,
      resolveJsonModule: true,
      jsx: "react-jsx",
      jsxImportSource: "@flink-reactor/dsl",
      baseUrl: ".",
      paths: {
        "@/*": ["./*"],
      },
    },
    include: ["pipelines/**/*", "schemas/**/*", "env/**/*", "patterns/**/*"],
  }
  return `${JSON.stringify(config, null, 2)}\n`
}

export function makeConfig(opts: ScaffoldOptions): string {
  // When the user opts into Grafana at scaffold time, we render the
  // matching `metricsPlugin` import + `services.grafana: {}` + `plugins:`
  // entry. Both pieces are needed together: Grafana scrapes Prometheus,
  // and Prometheus only sees Flink metrics if `metricsPlugin` configures
  // the JM/TM Prometheus reporter. Wiring just one half would silently
  // produce empty dashboards.
  const grafanaImport = opts.grafanaEnabled
    ? "\nimport { metricsPlugin } from '@flink-reactor/dsl/plugins'"
    : ""
  const servicesLine = opts.grafanaEnabled
    ? "services: { grafana: {} },"
    : "services: {},"
  const servicesComment = opts.grafanaEnabled
    ? `// Infra services this project depends on. \`grafana: {}\` was added
  // by \`fr new --grafana\` (or the interactive prompt) — \`fr cluster up\`
  // will additionally start Prometheus + Grafana under the
  // \`observability\` Compose profile.`
    : `// Infra services this project depends on. Empty here — \`fr cluster up\`
  // and \`fr sim up\` will start only the always-on services (Flink core,
  // SQL gateway, SeaweedFS). Add services as your pipelines need them, e.g.
  // \`services: { kafka: {} }\` to enable Kafka.`
  const pluginsBlock = opts.grafanaEnabled
    ? `\n\n  // Configure the Flink Prometheus reporter for the JM/TM. Port 9249
  // matches the scrape target the bundled Grafana datasource uses
  // (see \`src/cli/cluster/observability/prometheus/prometheus.yml\`).
  plugins: [
    metricsPlugin({ reporters: [{ type: 'prometheus', port: 9249 }] }),
  ],`
    : ""

  return `import { defineConfig } from '@flink-reactor/dsl'${grafanaImport}

export default defineConfig({
  flink: { version: '${opts.flinkVersion}' },

  ${servicesComment}
  ${servicesLine}${pluginsBlock}

  environments: {
    development: {
      cluster: { url: 'http://localhost:8081' },
      dashboard: { mockMode: true },
      pipelines: { '*': { parallelism: 1 } },
    },
    production: {
      cluster: { url: 'https://flink-prod:8081' },
      kubernetes: { namespace: 'flink-prod' },
      pipelines: { '*': { parallelism: 4 } },
    },
  },
})
`
}

export function makeGitignore(): string {
  return `node_modules/
dist/
.flink-reactor/
*.tsbuildinfo
.env
.env.local
`
}

export function makeDevEnv(_opts: ScaffoldOptions): string {
  return `import { defineEnvironment } from '@flink-reactor/dsl'

export default defineEnvironment({
  name: 'dev',
  // Override pipeline defaults for local development
})
`
}

export function makeNpmrc(registry: string): string {
  return `registry=${registry}\n`
}

// Vitest needs `resolve.alias` to mirror the tsconfig `@/*` path mapping.
// Vite intentionally ignores tsconfig `paths` (it's a typecheck contract,
// not a runtime contract), so without this file every `import x from '@/...'`
// inside a pipeline blows up under `pnpm test`.
export function makeVitestConfig(): string {
  return `import { fileURLToPath } from 'node:url'
import { defineConfig } from 'vitest/config'

export default defineConfig({
  resolve: {
    alias: {
      '@': fileURLToPath(new URL('.', import.meta.url)),
    },
  },
})
`
}

// ── Grafana opt-in injector ──────────────────────────────────────────
//
// `fr new --grafana` flips `opts.grafanaEnabled` for any template; most
// templates ship hand-rolled `flink-reactor.config.ts` content (rather
// than going through `makeConfig`), so we post-process the rendered
// config after the factory returns. The helper below is idempotent:
// running it on a config that already has the metricsPlugin import or
// `grafana: {}` in services is a no-op, so per-template factories that
// emit pre-wired Grafana output (e.g. pg-fluss-paimon) stay correct.
//
// This is intentionally string-based — Grafana wiring is a 3-line
// surgical insertion, not worth pulling in a TS AST library for. The
// injectGrafanaWiringTests cover the regex shapes templates emit today.

/**
 * Inject Grafana opt-in wiring into a rendered `flink-reactor.config.ts`.
 *
 * Three transformations, each idempotent:
 *   1. Add `import { metricsPlugin } from '@flink-reactor/dsl/plugins'`
 *      after the existing `import { defineConfig } ...` line.
 *   2. Add `grafana: {}` to the `services: { ... }` block (handling both
 *      empty `services: {}` and populated `services: { foo: ... }`).
 *   3. Register `metricsPlugin({ reporters: [{ type: 'prometheus', port: 9249 }] })`
 *      either by prepending to an existing `plugins: [ ... ]` array or
 *      by adding a new `plugins:` block before `environments:`.
 *
 * Returns the original content unchanged when `enabled` is false.
 */
export function injectGrafanaWiring(
  content: string,
  enabled: boolean | undefined,
): string {
  if (!enabled) return content

  // Step 1: import. Skip if metricsPlugin is already imported (template
  // pre-wired the opt-in itself, e.g. pg-fluss-paimon). The regex
  // consumes only the single newline at end of the defineConfig import
  // line — not the blank line that often follows — so the blank line
  // stays between the imports and `export default`.
  let result = content
  if (!result.includes("@flink-reactor/dsl/plugins")) {
    result = result.replace(
      /(import \{ defineConfig \} from '@flink-reactor\/dsl';?\n)/,
      `$1import { metricsPlugin } from '@flink-reactor/dsl/plugins'\n`,
    )
  }

  // Step 2: services. Find the opening `{` after `services:` and walk
  // forward counting brace depth to handle nested objects (e.g.
  // `services: { kafka: { bootstrapServers: '...' } }`). The
  // `inner.includes('grafana:')` check guards idempotency by inspecting
  // the actual extracted slice — a regex like `services:\s*\{[^}]*grafana:`
  // would fail on populated blocks because `[^}]*` halts at the first
  // nested `}` (e.g. inside `kafka: {}`).
  const servicesMatch = /services:\s*\{/.exec(result)
  if (servicesMatch) {
    const openIdx = servicesMatch.index + servicesMatch[0].length - 1
    const closeIdx = findMatchingBrace(result, openIdx)
    if (closeIdx > openIdx) {
      const inner = result.slice(openIdx + 1, closeIdx)
      if (!inner.includes("grafana:")) {
        const trimmed = inner.trim()
        const newInner =
          trimmed === ""
            ? " grafana: {} "
            : ` ${trimmed}${trimmed.endsWith(",") ? "" : ","} grafana: {} `
        result = `${result.slice(0, openIdx + 1)}${newInner}${result.slice(closeIdx)}`
      }
    }
  }

  // Step 3: plugins. If `plugins: [` already exists, prepend our entry.
  // Otherwise add a fresh block before the `environments:` key.
  if (/metricsPlugin\(/.test(result)) {
    // Already wired — skip.
  } else if (/plugins:\s*\[/.test(result)) {
    result = result.replace(
      /plugins:\s*\[/,
      "plugins: [\n    metricsPlugin({ reporters: [{ type: 'prometheus', port: 9249 }] }),",
    )
  } else if (/\n\s*environments:\s*\{/.test(result)) {
    result = result.replace(
      /(\n\s*environments:\s*\{)/,
      "\n  plugins: [\n    metricsPlugin({ reporters: [{ type: 'prometheus', port: 9249 }] }),\n  ],$1",
    )
  }

  return result
}

function findMatchingBrace(s: string, openIdx: number): number {
  let depth = 1
  for (let i = openIdx + 1; i < s.length; i++) {
    const ch = s[i]
    if (ch === "{") depth++
    else if (ch === "}") {
      depth--
      if (depth === 0) return i
    }
  }
  return -1
}

export function sharedFiles(opts: ScaffoldOptions): TemplateFile[] {
  const files: TemplateFile[] = [
    {
      path: "package.json",
      content: makePackageJson(opts),
    },
    { path: "tsconfig.json", content: makeTsconfig(opts) },
    { path: "vitest.config.ts", content: makeVitestConfig() },
    { path: "flink-reactor.config.ts", content: makeConfig(opts) },
    { path: ".gitignore", content: makeGitignore() },
  ]
  if (opts.registry) {
    files.push({ path: ".npmrc", content: makeNpmrc(opts.registry) })
  }
  return files
}

// ── Template README + test helpers ───────────────────────────────────
//
// See `docs/contributors/template-conventions.md` for the contract these
// helpers enforce: project-root README, per-pipeline README (7 sections),
// per-pipeline snapshot test, and `EXPECTED_PIPELINES` enrolment.

export interface TemplatePipelineEntry {
  readonly name: string
  readonly pitch: string
}

export interface TemplateReadmeOpts {
  readonly templateName: string
  readonly tagline: string
  readonly pipelines: readonly TemplatePipelineEntry[]
  readonly prerequisites?: readonly string[]
  readonly gettingStarted?: readonly string[]
}

/**
 * Builds the project-root `README.md` for a scaffolded template.
 *
 * Emits a TemplateFile at `README.md` with the template name as the H1, the
 * tagline as the lead paragraph, and one bullet per pipeline. Optional
 * `prerequisites` and `gettingStarted` arrays render as ordered/fenced sections
 * when supplied.
 *
 * See `docs/contributors/template-conventions.md` for the full contract.
 */
export function templateReadme(opts: TemplateReadmeOpts): TemplateFile {
  const sections: string[] = [
    `# ${humanizeTitle(opts.templateName)}`,
    "",
    opts.tagline,
    "",
    "## Pipelines",
    "",
    ...opts.pipelines.map((p) => `- **${p.name}** — ${p.pitch}`),
    "",
  ]

  if (opts.prerequisites && opts.prerequisites.length > 0) {
    sections.push("## Prerequisites", "")
    for (const item of opts.prerequisites) {
      sections.push(`- ${item}`)
    }
    sections.push("")
  }

  if (opts.gettingStarted && opts.gettingStarted.length > 0) {
    sections.push("## Getting Started", "", "```bash")
    for (const cmd of opts.gettingStarted) {
      sections.push(cmd)
    }
    sections.push("```", "")
  }

  return {
    path: "README.md",
    content: `${sections.join("\n").replace(/\n+$/, "")}\n`,
  }
}

export interface PipelineReadmeOpts {
  readonly pipelineName: string
  readonly tagline: string
  readonly demonstrates: readonly string[]
  readonly topology: string
  readonly schemas: readonly string[]
  readonly runCommand: string
  readonly source?: string
  readonly expectedOutput?: string
  readonly translationNotes?: string
}

/**
 * Builds a per-pipeline `pipelines/<name>/README.md` following the canonical
 * 7-section structure: title + pitch, source link, what-it-demonstrates,
 * topology, schemas, run-locally, expected output. An optional 8th section,
 * Translation Notes, is appended when `translationNotes` is supplied.
 *
 * Optional sections (`source`, `expectedOutput`, `translationNotes`) are
 * omitted entirely when not supplied — no empty headings are emitted.
 *
 * See `docs/contributors/template-conventions.md` for the full contract.
 */
export function pipelineReadme(opts: PipelineReadmeOpts): TemplateFile {
  const sections: string[] = [`# ${opts.pipelineName}`, "", opts.tagline, ""]

  if (opts.source) {
    sections.push("## Source", "", opts.source, "")
  }

  sections.push("## What it demonstrates", "")
  for (const item of opts.demonstrates) {
    sections.push(`- ${item}`)
  }
  sections.push("")

  sections.push("## Topology", "", "```", opts.topology.trimEnd(), "```", "")

  sections.push("## Schemas", "")
  for (const item of opts.schemas) {
    sections.push(`- ${item}`)
  }
  sections.push("")

  sections.push(
    "## Run locally",
    "",
    "```bash",
    opts.runCommand.trim(),
    "```",
    "",
  )

  if (opts.expectedOutput) {
    sections.push(
      "## Expected Output",
      "",
      "```",
      opts.expectedOutput.trimEnd(),
      "```",
      "",
    )
  }

  if (opts.translationNotes) {
    sections.push("## Translation Notes", "", opts.translationNotes.trim(), "")
  }

  return {
    path: `pipelines/${opts.pipelineName}/README.md`,
    content: `${sections.join("\n").replace(/\n+$/, "")}\n`,
  }
}

export interface PipelineTestStubOpts {
  readonly pipelineName: string
  readonly loadBearingPatterns: readonly RegExp[]
}

/**
 * Builds a per-pipeline snapshot test at
 * `tests/pipelines/<pipelineName>.test.ts`. The emitted source:
 *   1. Imports `synthesizeApp` and `resetNodeIdCounter` from `@flink-reactor/dsl`
 *   2. Imports the pipeline default export by relative path
 *   3. Calls `resetNodeIdCounter()` in `beforeEach` to make snapshots stable
 *   4. Defines a local `synth(pipeline)` helper that returns the SQL string
 *   5. Calls `expect(synth(pipeline)).toMatchSnapshot()`
 *   6. Emits one `expect(sql).toMatch(...)` per pattern in
 *      `loadBearingPatterns` so semantically-load-bearing SQL constructs
 *      (e.g. `TUMBLE`, `MATCH_RECOGNIZE`, `INSERT INTO`) cannot drift silently
 *      on `vitest -u`.
 *
 * See `docs/contributors/template-conventions.md` for the full contract.
 */
export function templatePipelineTestStub(
  opts: PipelineTestStubOpts,
): TemplateFile {
  const patternLines = opts.loadBearingPatterns
    .map((p) => `    expect(sql).toMatch(${p.toString()})`)
    .join("\n")

  const content = `import { beforeEach, describe, expect, it } from 'vitest'
import {
  type ConstructNode,
  resetNodeIdCounter,
  synthesizeApp,
} from '@flink-reactor/dsl'
import pipeline from '../../pipelines/${opts.pipelineName}/index.js'

function synth(node: ConstructNode): string {
  const result = synthesizeApp({ name: '${opts.pipelineName}', children: [node] })
  return result.pipelines[0].sql.sql
}

describe('${opts.pipelineName} pipeline', () => {
  beforeEach(() => {
    resetNodeIdCounter()
  })

  it('synthesizes stable SQL', () => {
    const sql = synth(pipeline)

    expect(sql).toMatchSnapshot()

${patternLines}
  })
})
`

  return {
    path: `tests/pipelines/${opts.pipelineName}.test.ts`,
    content,
  }
}

function humanizeTitle(slug: string): string {
  return slug
    .split(/[-_]/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ")
}
