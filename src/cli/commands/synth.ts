import { join } from "node:path"
import type { Command } from "commander"
import { Effect } from "effect"
import pc from "picocolors"
import { resolveConsoleUrl } from "@/cli/console-url.js"
import { loadPipeline, resolveProjectContext } from "@/cli/discovery.js"
import { runCommand } from "@/cli/effect-runner.js"
import { pushTapManifest } from "@/cli/tap-push.js"
import { generateCrd, toYaml } from "@/codegen/crd-generator.js"
import { generateSql, generateTapManifest } from "@/codegen/sql-generator.js"
import { type PipelineArtifact, synthesizeApp } from "@/core/app.js"
import { DiscoveryError, type FileSystemError } from "@/core/errors.js"
import { generatePipelineManifest } from "@/core/manifest.js"
import { FrFileSystem } from "@/core/services.js"

export function registerSynthCommand(program: Command): void {
  program
    .command("synth")
    .description("Synthesize pipelines to Flink SQL and CRDs")
    .option("-p, --pipeline <name>", "Synthesize a specific pipeline")
    .option(
      "-f, --file <path>",
      "Synthesize a specific .tsx file or directory (bypasses pipelines/ convention)",
    )
    .option("-e, --env <name>", "Environment name (loads env/<name>.ts)")
    .option("-o, --outdir <dir>", "Output directory", "dist")
    .option(
      "--deep-validate",
      "Submit EXPLAIN to a running Flink cluster for semantic validation",
    )
    .option(
      "--console-url <url>",
      "Push tap manifests to reactor-console at this URL",
    )
    .action(
      async (opts: {
        pipeline?: string
        file?: string
        env?: string
        outdir: string
        deepValidate?: boolean
        consoleUrl?: string
      }) => {
        await runCommand(runSynthEffect(opts))
      },
    )
}

// ── Imperative variant (used by dev command's internal calls) ───────

export async function runSynth(opts: {
  pipeline?: string
  file?: string
  env?: string
  outdir: string
  projectDir?: string
  consoleUrl?: string
}): Promise<PipelineArtifact[]> {
  const projectDir = opts.projectDir ?? process.cwd()
  const ctx = await resolveProjectContext(projectDir, {
    pipeline: opts.pipeline,
    file: opts.file,
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
        resolvedConfig: ctx.resolvedConfig ?? undefined,
      },
    )

    for (const artifact of result.pipelines) {
      allArtifacts.push(artifact)
      writePipelineOutput(artifact, opts.outdir, projectDir)
    }

    // If no pipelines were extracted (the node itself is the pipeline),
    // treat the whole tree as a single pipeline.
    if (result.pipelines.length === 0) {
      const { generateSql, generateTapManifest } = await import(
        "@/codegen/sql-generator.js"
      )
      const { generateCrd } = await import("@/codegen/crd-generator.js")

      const flinkVersion = ctx.config?.flink?.version ?? "2.0"
      const sql = generateSql(pipelineNode, { flinkVersion })
      const crd = generateCrd(pipelineNode, { flinkVersion })
      const { manifest: tapManifest } = generateTapManifest(pipelineNode, {
        flinkVersion,
        devMode: true,
      })

      const pipelineManifest = generatePipelineManifest(pipelineNode)

      const artifact: PipelineArtifact = {
        name: discovered.name,
        sql,
        crd,
        tapManifest,
        pipelineManifest,
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

  // Push tap manifests to console if URL is available
  const targetUrl = resolveConsoleUrl({
    consoleUrl: opts.consoleUrl,
    resolvedConfig: ctx.resolvedConfig ?? undefined,
  })
  if (targetUrl) {
    for (const artifact of allArtifacts) {
      if (artifact.tapManifest) {
        await pushTapManifest(artifact.tapManifest, targetUrl)
      }
    }
  }

  return allArtifacts
}

function writePipelineOutput(
  artifact: PipelineArtifact,
  outdir: string,
  projectDir: string,
): void {
  const { mkdirSync, writeFileSync } =
    require("node:fs") as typeof import("node:fs")
  const pipelineDir = join(projectDir, outdir, artifact.name)
  mkdirSync(pipelineDir, { recursive: true })

  writeFileSync(join(pipelineDir, "pipeline.sql"), artifact.sql.sql, "utf-8")

  const crdYaml = toYaml(artifact.crd)
  writeFileSync(join(pipelineDir, "deployment.yaml"), crdYaml, "utf-8")

  const configMap = buildConfigMapYaml(artifact)
  writeFileSync(join(pipelineDir, "configmap.yaml"), configMap, "utf-8")

  if (artifact.tapManifest) {
    const outdirPath = join(projectDir, outdir)
    mkdirSync(outdirPath, { recursive: true })
    writeFileSync(
      join(outdirPath, `${artifact.name}.tap-manifest.json`),
      JSON.stringify(artifact.tapManifest, null, 2),
      "utf-8",
    )
  }
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

// ── Effect variant ──────────────────────────────────────────────────

/**
 * Effect-based synth program.
 *
 * Returns an Effect with typed errors and console output.
 * Uses FrFileSystem service for file writes.
 */
export function runSynthEffect(opts: {
  pipeline?: string
  file?: string
  env?: string
  outdir: string
  projectDir?: string
  consoleUrl?: string
}): Effect.Effect<
  readonly PipelineArtifact[],
  DiscoveryError | FileSystemError,
  FrFileSystem
> {
  return Effect.gen(function* () {
    const fs = yield* FrFileSystem
    const projectDir = opts.projectDir ?? process.cwd()

    const ctx = yield* Effect.tryPromise({
      try: () =>
        resolveProjectContext(projectDir, {
          pipeline: opts.pipeline,
          file: opts.file,
          env: opts.env,
        }),
      catch: (err) =>
        new DiscoveryError({
          reason: "config_not_found",
          message: (err as Error).message,
          path: projectDir,
        }),
    })

    if (ctx.pipelines.length === 0) {
      yield* Effect.sync(() =>
        console.log(pc.yellow("No pipelines found in pipelines/ directory.")),
      )
      return [] as readonly PipelineArtifact[]
    }

    yield* Effect.sync(() =>
      console.log(
        pc.dim(`Synthesizing ${ctx.pipelines.length} pipeline(s)...\n`),
      ),
    )

    const allArtifacts: PipelineArtifact[] = []

    for (const discovered of ctx.pipelines) {
      const pipelineNode = yield* Effect.tryPromise({
        try: () => loadPipeline(discovered.entryPoint, projectDir),
        catch: (err) =>
          new DiscoveryError({
            reason: "import_failure",
            message: (err as Error).message,
            path: discovered.entryPoint,
          }),
      })

      const result = synthesizeApp(
        {
          name: discovered.name,
          children: pipelineNode,
        },
        {
          env: ctx.env ?? undefined,
          config: ctx.config ?? undefined,
          resolvedConfig: ctx.resolvedConfig ?? undefined,
        },
      )

      for (const artifact of result.pipelines) {
        allArtifacts.push(artifact)
        yield* writePipelineOutputEffect(artifact, opts.outdir, projectDir, fs)
      }

      // Fallback: treat whole tree as single pipeline
      if (result.pipelines.length === 0) {
        const flinkVersion = ctx.config?.flink?.version ?? "2.0"
        const sql = generateSql(pipelineNode, { flinkVersion })
        const crd = generateCrd(pipelineNode, { flinkVersion })
        const { manifest: tapManifest } = generateTapManifest(pipelineNode, {
          flinkVersion,
          devMode: true,
        })

        const pipelineManifest = generatePipelineManifest(pipelineNode)

        const artifact: PipelineArtifact = {
          name: discovered.name,
          sql,
          crd,
          tapManifest,
          pipelineManifest,
        }

        allArtifacts.push(artifact)
        yield* writePipelineOutputEffect(artifact, opts.outdir, projectDir, fs)
      }
    }

    // Print summary
    yield* Effect.sync(() => {
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
    })

    // Push tap manifests to console if URL is available
    const targetUrl = resolveConsoleUrl({
      consoleUrl: opts.consoleUrl,
      resolvedConfig: ctx.resolvedConfig ?? undefined,
    })
    if (targetUrl) {
      for (const artifact of allArtifacts) {
        const manifest = artifact.tapManifest
        if (manifest) {
          yield* Effect.tryPromise({
            try: () => pushTapManifest(manifest, targetUrl),
            catch: () => undefined as never, // pushTapManifest never throws
          })
        }
      }
    }

    return allArtifacts as readonly PipelineArtifact[]
  })
}

function writePipelineOutputEffect(
  artifact: PipelineArtifact,
  outdir: string,
  projectDir: string,
  fs: FrFileSystem["Type"],
): Effect.Effect<void, FileSystemError> {
  return Effect.gen(function* () {
    const pipelineDir = join(projectDir, outdir, artifact.name)
    yield* fs.mkdir(pipelineDir, { recursive: true })

    yield* fs.writeFile(join(pipelineDir, "pipeline.sql"), artifact.sql.sql)

    const crdYaml = toYaml(artifact.crd)
    yield* fs.writeFile(join(pipelineDir, "deployment.yaml"), crdYaml)

    const configMap = buildConfigMapYaml(artifact)
    yield* fs.writeFile(join(pipelineDir, "configmap.yaml"), configMap)

    if (artifact.tapManifest) {
      const outdirPath = join(projectDir, outdir)
      yield* fs.mkdir(outdirPath, { recursive: true })
      yield* fs.writeFile(
        join(outdirPath, `${artifact.name}.tap-manifest.json`),
        JSON.stringify(artifact.tapManifest, null, 2),
      )
    }
  })
}
