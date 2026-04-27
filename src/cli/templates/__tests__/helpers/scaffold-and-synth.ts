import { existsSync, mkdirSync, mkdtempSync, symlinkSync } from "node:fs"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"
import type { ScaffoldOptions, TemplateName } from "@/cli/commands/new.js"
import { scaffoldProject } from "@/cli/commands/new.js"
import { runSynth } from "@/cli/commands/synth.js"
import type { PipelineArtifact } from "@/core/app.js"

const REPO_ROOT = resolve(__dirname, "../../../../../")

// ── Expected pipeline count per template ─────────────────────────────
// `minimal` + `monorepo` are structural templates with no pipelines of
// their own; every other template ships ≥1 pipeline under `pipelines/`.
// Kept explicit (not derived) so adding/removing a pipeline forces an
// update here, making the template surface area observable.

export const EXPECTED_PIPELINES: Record<TemplateName, readonly string[]> = {
  starter: ["hello-world"],
  minimal: [],
  monorepo: [],
  "cdc-lakehouse": ["cdc-to-lakehouse"],
  "realtime-analytics": ["page-view-analytics"],
  ecommerce: [
    "ecom-customer-360",
    "ecom-order-enrichment",
    "ecom-revenue-analytics",
    "pump-ecom",
  ],
  "ride-sharing": ["rides-surge-pricing", "rides-trip-tracking"],
  "grocery-delivery": ["grocery-order-fulfillment", "grocery-store-rankings"],
  banking: ["bank-compliance-agg", "bank-fraud-detection"],
  "iot-factory": ["iot-predictive-maintenance", "pump-iot"],
  "lakehouse-ingestion": ["lakehouse-ingest", "pump-lakehouse"],
  "lakehouse-analytics": [
    "medallion-bronze",
    "medallion-gold",
    "medallion-silver",
    "pump-medallion",
  ],
  "stock-basics": [
    "getting-started",
    "stream-sql-union",
    "stream-window-sql",
    "wordcount-sql",
  ],
}

/**
 * Scaffolds `template` into a fresh temp dir, symlinks the built
 * `@flink-reactor/dsl` package so template imports resolve, then runs
 * `runSynth` and returns the artifacts plus the temp dir (for cleanup
 * by the caller).
 *
 * Requires `pnpm build` to have run — checked once on first call.
 */
export async function scaffoldAndSynth(
  template: TemplateName,
  overrides?: Partial<ScaffoldOptions>,
): Promise<{ artifacts: PipelineArtifact[]; tempRoot: string }> {
  ensureBuilt()

  const tempRoot = mkdtempSync(join(tmpdir(), "fr-scaffold-"))
  const projectDir = join(tempRoot, "app")

  const opts: ScaffoldOptions = {
    projectName: "scaffold-synth-test",
    template,
    pm: "pnpm",
    flinkVersion: "2.0",
    gitInit: false,
    installDeps: false,
    ...overrides,
  }

  scaffoldProject(projectDir, opts)
  linkDslPackage(projectDir)

  const artifacts = await runSynth({ outdir: "dist", projectDir })
  return { artifacts, tempRoot }
}

function linkDslPackage(projectDir: string): void {
  const scope = join(projectDir, "node_modules", "@flink-reactor")
  mkdirSync(scope, { recursive: true })
  symlinkSync(REPO_ROOT, join(scope, "dsl"), "dir")
}

let builtChecked = false
function ensureBuilt(): void {
  if (builtChecked) return
  const distIndex = join(REPO_ROOT, "dist", "index.js")
  if (!existsSync(distIndex)) {
    throw new Error(
      `Expected ${distIndex} to exist. Run \`pnpm build\` before these tests.`,
    )
  }
  builtChecked = true
}
