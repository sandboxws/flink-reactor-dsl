// Bundle README helper for stock-example templates.
//
// `bundleReadme` delegates to `templateReadme()` from shared.ts to keep the
// project-root README format consistent with the per-template convention,
// then appends a "Mapping to Apache Flink stock examples" table that links
// each pipeline back to its Java source. Per-pipeline READMEs themselves are
// emitted via `pipelineReadme()` from shared.ts — this helper covers only
// the project-root index.
import type { TemplateFile } from "@/cli/commands/new.js"
import { templateReadme } from "../shared.js"

export interface BundlePipelineEntry {
  readonly name: string
  readonly pitch: string
  /** File name of the Apache Flink Java source (e.g. 'WordCountSQLExample.java'). */
  readonly sourceFile: string
}

export interface BundleReadmeOpts {
  readonly templateName: string
  readonly tagline: string
  readonly pipelines: readonly BundlePipelineEntry[]
  readonly prerequisites?: readonly string[]
  readonly gettingStarted?: readonly string[]
}

/**
 * Build the project-root `README.md` for a stock-example template bundle.
 *
 * Reuses `templateReadme()` for the standard top section (title, tagline,
 * pipeline bullets, optional prerequisites / getting-started), and embeds
 * a per-pipeline `[README](pipelines/<name>/README.md)` link directly in
 * each bullet's pitch so readers can jump straight to the per-pipeline
 * documentation. Appends a "Mapping to Apache Flink stock examples"
 * section listing each pipeline's Apache Flink Java source.
 */
export function bundleReadme(opts: BundleReadmeOpts): TemplateFile {
  const pipelinesWithLinks = opts.pipelines.map((p) => ({
    name: p.name,
    pitch: `${p.pitch} ([README](pipelines/${p.name}/README.md))`,
  }))

  const baseFile = templateReadme({
    templateName: opts.templateName,
    tagline: opts.tagline,
    pipelines: pipelinesWithLinks,
    prerequisites: opts.prerequisites,
    gettingStarted: opts.gettingStarted,
  })

  const mappingLines: string[] = [
    "## Mapping to Apache Flink stock examples",
    "",
    "| Pipeline | Apache Flink Java source |",
    "|---|---|",
    ...opts.pipelines.map((p) => `| \`${p.name}\` | \`${p.sourceFile}\` |`),
    "",
  ]

  const baseContent = baseFile.content.replace(/\n+$/, "")
  return {
    path: baseFile.path,
    content: `${baseContent}\n\n${mappingLines.join("\n")}`,
  }
}
