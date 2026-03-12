import type { Command } from "commander"
import { Effect } from "effect"
import pc from "picocolors"
import { loadPipeline, resolveProjectContext } from "@/cli/discovery.js"
import { DiscoveryError, ValidationError } from "@/core/errors.js"
import { FlinkVersionCompat } from "@/core/flink-compat.js"
import {
  SynthContext,
  type ValidationDiagnostic,
} from "@/core/synth-context.js"
import type { ConstructNode, FlinkMajorVersion } from "@/core/types.js"

// ── Types ───────────────────────────────────────────────────────────

export interface PipelineValidationResult {
  readonly name: string
  readonly errors: readonly ValidationDiagnostic[]
  readonly warnings: readonly ValidationDiagnostic[]
}

// ── Command registration ────────────────────────────────────────────

export function registerValidateCommand(program: Command): void {
  program
    .command("validate")
    .description("Validate pipeline definitions and configuration")
    .option("-p, --pipeline <name>", "Validate a specific pipeline")
    .option("-e, --env <name>", "Environment name")
    .action(async (opts: { pipeline?: string; env?: string }) => {
      const success = await runValidate(opts)
      if (!success) {
        process.exitCode = 1
      }
    })
}

// ── Validate logic ──────────────────────────────────────────────────

export async function runValidate(opts: {
  pipeline?: string
  env?: string
  projectDir?: string
}): Promise<boolean> {
  const projectDir = opts.projectDir ?? process.cwd()
  const projectCtx = await resolveProjectContext(projectDir, {
    pipeline: opts.pipeline,
    env: opts.env,
  })

  if (projectCtx.pipelines.length === 0) {
    console.log(pc.yellow("No pipelines found in pipelines/ directory."))
    return true
  }

  const flinkVersion: FlinkMajorVersion =
    projectCtx.config?.flink?.version ?? "2.0"

  console.log(
    pc.dim(`Validating ${projectCtx.pipelines.length} pipeline(s)...\n`),
  )

  const results: PipelineValidationResult[] = []
  let hasErrors = false

  for (const discovered of projectCtx.pipelines) {
    const pipelineNode = await loadPipeline(discovered.entryPoint, projectDir)
    const result = validatePipeline(pipelineNode, discovered.name, flinkVersion)
    results.push(result)

    if (result.errors.length > 0) {
      hasErrors = true
    }
  }

  // Print results
  for (const result of results) {
    if (result.errors.length === 0 && result.warnings.length === 0) {
      console.log(
        `  ${pc.green("\u2713")} ${pc.bold(result.name)} ${pc.dim("— valid")}`,
      )
    } else {
      if (result.errors.length > 0) {
        console.log(
          `  ${pc.red("\u2717")} ${pc.bold(result.name)} ${pc.dim(`— ${result.errors.length} error(s)`)}`,
        )
        for (const err of result.errors) {
          console.log(`    ${pc.red("error:")} ${err.message}`)
          if (err.component) {
            console.log(`    ${pc.dim(`component: ${err.component}`)}`)
          }
        }
      }

      if (result.warnings.length > 0) {
        console.log(
          `  ${pc.yellow("!")} ${pc.bold(result.name)} ${pc.dim(`— ${result.warnings.length} warning(s)`)}`,
        )
        for (const warn of result.warnings) {
          console.log(`    ${pc.yellow("warning:")} ${warn.message}`)
        }
      }
    }
  }

  console.log("")

  if (hasErrors) {
    console.log(pc.red("Validation failed."))
  } else {
    console.log(pc.green("All pipelines valid."))
  }

  return !hasErrors
}

// ── Per-pipeline validation ─────────────────────────────────────────

function validatePipeline(
  pipelineNode: ConstructNode,
  name: string,
  flinkVersion: FlinkMajorVersion,
): PipelineValidationResult {
  const ctx = new SynthContext()
  ctx.buildFromTree(pipelineNode)

  // Run tree-aware DAG validation.
  //
  // In the construct tree, edges go parent→child (downstream→upstream).
  // Sources are always leaf nodes, so the standard detectOrphanSources
  // (which checks outgoing edges) would flag ALL sources, including
  // connected ones. We use tree-aware checks instead:
  //
  // - Orphan source: a Source whose only parent is Pipeline (not part
  //   of a transform/sink chain)
  // - Dangling sink: a Sink with no children (nothing feeds it)
  // - Cycle detection: standard DFS (still valid for tree model)
  // - Changelog mismatch: standard check (still valid)
  const dagDiagnostics = validateTreeAware(pipelineNode, ctx)

  // Run Flink version validation (feature gating)
  const versionDiagnostics = validateFlinkVersionFeatures(
    pipelineNode,
    flinkVersion,
  )

  const allDiagnostics = [...dagDiagnostics, ...versionDiagnostics]

  return {
    name,
    errors: allDiagnostics.filter((d) => d.severity === "error"),
    warnings: allDiagnostics.filter((d) => d.severity === "warning"),
  }
}

/**
 * Tree-aware validation that understands the construct tree's
 * parent→child (downstream→upstream) edge direction.
 */
function validateTreeAware(
  pipelineNode: ConstructNode,
  ctx: SynthContext,
): ValidationDiagnostic[] {
  const diagnostics: ValidationDiagnostic[] = []

  // Orphan sources: Sources that are direct children of Pipeline
  // (not nested inside a transform or sink chain).
  if (pipelineNode.kind === "Pipeline") {
    for (const child of pipelineNode.children) {
      if (child.kind === "Source") {
        diagnostics.push({
          severity: "error",
          message: `Orphan source '${child.component}' (${child.id}): declared but never consumed`,
          nodeId: child.id,
          component: child.component,
        })
      }
    }
  }

  // Dangling sinks: Sinks with no children (no upstream input)
  const sinks = ctx.getNodesByKind("Sink")
  for (const sink of sinks) {
    if (sink.children.length === 0) {
      const outgoing = ctx.getOutgoing(sink.id)
      if (outgoing.size === 0) {
        diagnostics.push({
          severity: "error",
          message: `Dangling sink '${sink.component}' (${sink.id}): no input path`,
          nodeId: sink.id,
          component: sink.component,
        })
      }
    }
  }

  // Cycles (DFS-based — works regardless of edge direction)
  diagnostics.push(...ctx.detectCycles())

  // Changelog mode mismatches
  diagnostics.push(...ctx.detectChangelogMismatch())

  return diagnostics
}

// ── Flink version feature gating ────────────────────────────────────

function validateFlinkVersionFeatures(
  node: ConstructNode,
  version: FlinkMajorVersion,
): ValidationDiagnostic[] {
  const diagnostics: ValidationDiagnostic[] = []

  function walk(n: ConstructNode): void {
    // Check for gated features based on component type
    const _featureChecks: Array<{ component: string; feature: string }> = [
      { component: "MatchRecognize", feature: "MATERIALIZED_TABLE" },
    ]

    // Check any component that declares requiredFeature
    if (n.props.requiredFeature) {
      const gate = FlinkVersionCompat.checkFeature(
        n.props.requiredFeature as string,
        version,
      )
      if (gate) {
        diagnostics.push({
          severity: "error",
          message: gate.message,
          nodeId: n.id,
          component: n.component,
        })
      }
    }

    for (const child of n.children) {
      walk(child)
    }
  }

  walk(node)
  return diagnostics
}

// ── Effect variant ──────────────────────────────────────────────────

/**
 * Effect-based validation program.
 *
 * Returns validation results with typed errors for discovery failures
 * and validation failures.
 */
export function runValidateEffect(opts: {
  pipeline?: string
  env?: string
  projectDir?: string
}): Effect.Effect<
  readonly PipelineValidationResult[],
  DiscoveryError | ValidationError
> {
  return Effect.gen(function* () {
    const projectDir = opts.projectDir ?? process.cwd()

    const projectCtx = yield* Effect.tryPromise({
      try: () =>
        resolveProjectContext(projectDir, {
          pipeline: opts.pipeline,
          env: opts.env,
        }),
      catch: (err) =>
        new DiscoveryError({
          reason: "config_not_found",
          message: (err as Error).message,
          path: projectDir,
        }),
    })

    if (projectCtx.pipelines.length === 0) {
      return [] as readonly PipelineValidationResult[]
    }

    const flinkVersion: FlinkMajorVersion =
      projectCtx.config?.flink?.version ?? "2.0"

    const results: PipelineValidationResult[] = []

    for (const discovered of projectCtx.pipelines) {
      const pipelineNode = yield* Effect.tryPromise({
        try: () => loadPipeline(discovered.entryPoint, projectDir),
        catch: (err) =>
          new DiscoveryError({
            reason: "import_failure",
            message: (err as Error).message,
            path: discovered.entryPoint,
          }),
      })

      results.push(
        validatePipeline(pipelineNode, discovered.name, flinkVersion),
      )
    }

    // Check for validation errors across all pipelines
    const allErrors = results.flatMap((r) => [...r.errors])
    if (allErrors.length > 0) {
      return yield* Effect.fail(new ValidationError({ diagnostics: allErrors }))
    }

    return results as readonly PipelineValidationResult[]
  })
}
