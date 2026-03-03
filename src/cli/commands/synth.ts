import { mkdirSync, writeFileSync } from "node:fs"
import { join } from "node:path"
import type { Command } from "commander"
import pc from "picocolors"
import { loadPipeline, resolveProjectContext } from "@/cli/discovery.js"
import { toYaml } from "@/codegen/crd-generator.js"
import { type PipelineArtifact, synthesizeApp } from "@/core/app.js"

export function registerSynthCommand(program: Command): void {
  program
    .command("synth")
    .description("Synthesize pipelines to Flink SQL and CRDs")
    .option("-p, --pipeline <name>", "Synthesize a specific pipeline")
    .option("-e, --env <name>", "Environment name (loads env/<name>.ts)")
    .option("-o, --outdir <dir>", "Output directory", "dist")
    .action(
      async (opts: { pipeline?: string; env?: string; outdir: string }) => {
        await runSynth(opts)
      },
    )
}

export async function runSynth(opts: {
  pipeline?: string
  env?: string
  outdir: string
  projectDir?: string
}): Promise<PipelineArtifact[]> {
  const projectDir = opts.projectDir ?? process.cwd()
  const ctx = await resolveProjectContext(projectDir, {
    pipeline: opts.pipeline,
    env: opts.env,
  })

  if (ctx.pipelines.length === 0) {
    console.log(pc.yellow("No pipelines found in pipelines/ directory."))
    return []
  }

  console.log(pc.dim(`Synthesizing ${ctx.pipelines.length} pipeline(s)...\n`))

  const allArtifacts: PipelineArtifact[] = []

  for (const discovered of ctx.pipelines) {
    const pipelineNode = await loadPipeline(discovered.entryPoint, projectDir)

    const result = synthesizeApp(
      {
        name: discovered.name,
        children: pipelineNode,
      },
      {
        env: ctx.env ?? undefined,
        config: ctx.config ?? undefined,
      },
    )

    for (const artifact of result.pipelines) {
      allArtifacts.push(artifact)
      writePipelineOutput(artifact, opts.outdir, projectDir)
    }

    // If no pipelines were extracted (the node itself is the pipeline),
    // treat the whole tree as a single pipeline.
    if (result.pipelines.length === 0) {
      const { generateSql } = await import("@/codegen/sql-generator.js")
      const { generateCrd } = await import("@/codegen/crd-generator.js")

      const flinkVersion = ctx.config?.flink?.version ?? "2.0"
      const sql = generateSql(pipelineNode, { flinkVersion })
      const crd = generateCrd(pipelineNode, { flinkVersion })

      const artifact: PipelineArtifact = {
        name: discovered.name,
        sql,
        crd,
      }

      allArtifacts.push(artifact)
      writePipelineOutput(artifact, opts.outdir, projectDir)
    }
  }

  console.log(
    pc.green(
      `\nSynthesis complete. ${allArtifacts.length} pipeline(s) written.\n`,
    ),
  )

  for (const artifact of allArtifacts) {
    const stmtCount = artifact.sql.statements.length
    console.log(
      `  ${pc.cyan(artifact.name)} ${pc.dim(`(${stmtCount} statement${stmtCount !== 1 ? "s" : ""})`)} ${pc.dim(`→ ${opts.outdir}/${artifact.name}/`)}`,
    )
  }

  console.log("")
  return allArtifacts
}

function writePipelineOutput(
  artifact: PipelineArtifact,
  outdir: string,
  projectDir: string,
): void {
  const pipelineDir = join(projectDir, outdir, artifact.name)
  mkdirSync(pipelineDir, { recursive: true })

  // Write SQL
  writeFileSync(join(pipelineDir, "pipeline.sql"), artifact.sql.sql, "utf-8")

  // Write CRD YAML
  const crdYaml = toYaml(artifact.crd)
  writeFileSync(join(pipelineDir, "deployment.yaml"), crdYaml, "utf-8")

  // Write ConfigMap (SQL mounted as K8s ConfigMap)
  const configMap = buildConfigMapYaml(artifact)
  writeFileSync(join(pipelineDir, "configmap.yaml"), configMap, "utf-8")
}

function buildConfigMapYaml(artifact: PipelineArtifact): string {
  const configMapObj = {
    apiVersion: "v1",
    kind: "ConfigMap",
    metadata: {
      name: `${artifact.name}-sql`,
    },
    data: {
      "pipeline.sql": artifact.sql.sql,
    },
  }

  return toYaml(configMapObj)
}
