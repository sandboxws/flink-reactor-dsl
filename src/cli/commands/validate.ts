import type { Command } from "commander"
import { Effect } from "effect"
import pc from "picocolors"
import { loadPipeline, resolveProjectContext } from "@/cli/discovery.js"
import { runCommand } from "@/cli/effect-runner.js"
import { synthesizeApp } from "@/core/app.js"
import { validateConnectorProperties } from "@/core/connector-validation.js"
import { DiscoveryError, ValidationError } from "@/core/errors.js"
import { FlinkVersionCompat } from "@/core/flink-compat.js"
import {
  validateExpressionSyntax,
  validateSchemaReferences,
} from "@/core/schema-validation.js"
import { resolveSiblingChains } from "@/core/sibling-chain.js"
import {
  SynthContext,
  type ValidationDiagnostic,
} from "@/core/synth-context.js"
import type { ConstructNode, FlinkMajorVersion } from "@/core/types.js"
import {
  SqlGatewayClient,
  SqlGatewayClientError,
} from "@/lib/sql-gateway/client.js"

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
    .option(
      "--deep-validate",
      "Submit EXPLAIN to a running Flink cluster for semantic validation",
    )
    .action(
      async (opts: {
        pipeline?: string
        env?: string
        deepValidate?: boolean
      }) => {
        await runCommand(runValidateEffect(opts))
      },
    )
}

// ── Validate logic ──────────────────────────────────────────────────

export async function runValidate(opts: {
  pipeline?: string
  env?: string
  projectDir?: string
  deepValidate?: boolean
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
    let result = await validatePipeline(
      pipelineNode,
      discovered.name,
      flinkVersion,
    )

    // Deep validation: dt-sql-parser parse check + EXPLAIN against a running Flink cluster
    if (opts.deepValidate && result.errors.length === 0) {
      // Tier 2a: dt-sql-parser full statement parsing
      const { deepVerifySql } = await import("@/codegen/sql/sql-verifier.js")
      const { generateSql } = await import("@/codegen/sql-generator.js")
      const sqlResult = generateSql(pipelineNode, { flinkVersion })
      const parseDiags = await deepVerifySql(sqlResult.statements)
      if (parseDiags.length > 0) {
        result = {
          ...result,
          errors: [
            ...result.errors,
            ...parseDiags.filter((d) => d.severity === "error"),
          ],
          warnings: [
            ...result.warnings,
            ...parseDiags.filter((d) => d.severity === "warning"),
          ],
        }
      }

      // Tier 2b: SQL Gateway EXPLAIN semantic validation
      const deepDiags = await runDeepValidation(
        pipelineNode,
        discovered.name,
        flinkVersion,
        projectCtx,
      )
      if (deepDiags.length > 0) {
        result = {
          ...result,
          errors: [
            ...result.errors,
            ...deepDiags.filter((d) => d.severity === "error"),
          ],
          warnings: [
            ...result.warnings,
            ...deepDiags.filter((d) => d.severity === "warning"),
          ],
        }
      }
    }

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

async function validatePipeline(
  pipelineNode: ConstructNode,
  name: string,
  flinkVersion: FlinkMajorVersion,
): Promise<PipelineValidationResult> {
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

  // Run schema validation (cross-component column reference checks)
  const schemaDiagnostics = validateSchemaReferences(pipelineNode)

  // Run expression syntax validation (SQL parse checks)
  const expressionDiagnostics = await validateExpressionSyntax(pipelineNode)

  // Run connector property validation (standalone mode: warn for infra-provided props)
  const connectorDiagnostics = validateConnectorProperties(pipelineNode, {
    standalone: true,
  })

  const allDiagnostics = [
    ...dagDiagnostics,
    ...versionDiagnostics,
    ...schemaDiagnostics,
    ...expressionDiagnostics,
    ...connectorDiagnostics,
  ]

  return {
    name,
    errors: allDiagnostics.filter((d) => d.severity === "error"),
    warnings: allDiagnostics.filter((d) => d.severity === "warning"),
  }
}

/**
 * Tree-aware validation that understands the construct tree's
 * parent→child (downstream→upstream) edge direction AND the sibling-chain
 * topology (Source, zero-or-more Transforms, Sink under a common container),
 * which codegen stitches via buildSiblingChainQuery / Pattern-2 resolution.
 */
function validateTreeAware(
  pipelineNode: ConstructNode,
  ctx: SynthContext,
): ValidationDiagnostic[] {
  const diagnostics: ValidationDiagnostic[] = []
  const { sinkToSource, sourceToSinks } = resolveSiblingChains(pipelineNode)

  // A Source listed directly under <Pipeline>/<StatementSet> is considered
  // consumed if ANY of:
  //   1. the sibling-chain resolver bound it to a downstream Sink, OR
  //   2. some operator node (Join/Transform/Window/CEP/Sink) has it as a
  //      graph child — the join-embedded-source pattern, e.g. IntervalJoin
  //      was called with this source as `left`/`right`, which adds it as a
  //      child of the Join in addition to its declared position, OR
  //   3. another node's props reference it by its SQL name (LookupJoin's
  //      `table` prop pointing at a JdbcSource — name-based, not tree-based).
  const nameIndex = buildSourceNameIndex(pipelineNode)
  const referencedByName = collectNameReferences(pipelineNode, nameIndex)
  const visitForOrphans = (node: ConstructNode): void => {
    if (node.component === "Pipeline" || node.component === "StatementSet") {
      for (const child of node.children) {
        if (child.kind !== "Source") continue
        if (sourceToSinks.has(child.id)) continue
        if (hasOperatorParent(ctx, child.id)) continue
        if (referencedByName.has(child.id)) continue
        diagnostics.push({
          severity: "error",
          message: `Orphan source '${child.component}' (${child.id}): declared but never consumed`,
          nodeId: child.id,
          component: child.component,
        })
      }
    }
    for (const child of node.children) visitForOrphans(child)
  }
  visitForOrphans(pipelineNode)

  // Dangling sinks: Sinks with no explicit children (Pattern 1) AND no
  // incoming graph edge AND no sibling-chain source (Pattern 2).
  const sinks = ctx.getNodesByKind("Sink")
  for (const sink of sinks) {
    if (sink.children.length > 0) continue
    if (ctx.getOutgoing(sink.id).size > 0) continue
    if (sinkToSource.has(sink.id)) continue
    diagnostics.push({
      severity: "error",
      message: `Dangling sink '${sink.component}' (${sink.id}): no input path`,
      nodeId: sink.id,
      component: sink.component,
    })
  }

  // Cycles (DFS-based — works regardless of edge direction)
  diagnostics.push(...ctx.detectCycles())

  // Changelog mode mismatches — now handled by SynthContext.validate()
  // via validateChangelogModes() (full propagation, not just sink checks)

  return diagnostics
}

/**
 * True when `sourceId` is a graph child of some non-container node (Join,
 * Transform, Sink, etc.). Join factories like IntervalJoin/TemporalJoin/
 * LookupJoin add their `left`/`right`/`input`/`stream`/`temporal` props as
 * children — so a source declared at Pipeline level AND passed to a join
 * ends up with two parents: the Pipeline (declaration) and the operator
 * (real use). Only the latter counts as consumption.
 */
function hasOperatorParent(ctx: SynthContext, sourceId: string): boolean {
  for (const parentId of ctx.getIncoming(sourceId)) {
    const parent = ctx.getNode(parentId)
    if (!parent) continue
    if (parent.component === "Pipeline") continue
    if (parent.component === "StatementSet") continue
    return true
  }
  return false
}

/**
 * Build an index from SQL identifier → source node id, covering the source's
 * own id plus any explicit `name` or `table` props. Used to resolve string
 * references like `LookupJoin({ table: "customers" })` back to the JdbcSource
 * that declares that table.
 */
function buildSourceNameIndex(root: ConstructNode): Map<string, string> {
  const map = new Map<string, string>()
  const walk = (node: ConstructNode): void => {
    if (node.kind === "Source") {
      map.set(node.id, node.id)
      const nameProp = node.props.name
      if (typeof nameProp === "string") map.set(nameProp, node.id)
      const tableProp = node.props.table
      if (typeof tableProp === "string") map.set(tableProp, node.id)
    }
    for (const child of node.children) walk(child)
  }
  walk(root)
  return map
}

/**
 * Scan every non-source node's prop values for strings matching a known
 * source identifier. Any hit means the corresponding source is referenced
 * (typically via a lookup-table or temporal-table name reference).
 */
function collectNameReferences(
  root: ConstructNode,
  nameIndex: ReadonlyMap<string, string>,
): Set<string> {
  const refs = new Set<string>()
  const consider = (value: unknown): void => {
    if (typeof value !== "string") return
    const sourceId = nameIndex.get(value)
    if (sourceId) refs.add(sourceId)
  }
  const walk = (node: ConstructNode): void => {
    if (node.kind !== "Source") {
      for (const value of Object.values(node.props)) {
        if (Array.isArray(value)) value.forEach(consider)
        else consider(value)
      }
    }
    for (const child of node.children) walk(child)
  }
  walk(root)
  return refs
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
  deepValidate?: boolean
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
      yield* Effect.sync(() =>
        console.log(pc.yellow("No pipelines found in pipelines/ directory.")),
      )
      return [] as readonly PipelineValidationResult[]
    }

    const flinkVersion: FlinkMajorVersion =
      projectCtx.config?.flink?.version ?? "2.0"

    yield* Effect.sync(() =>
      console.log(
        pc.dim(`Validating ${projectCtx.pipelines.length} pipeline(s)...\n`),
      ),
    )

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

      const result = yield* Effect.tryPromise({
        try: () =>
          validatePipeline(pipelineNode, discovered.name, flinkVersion),
        catch: (err) =>
          new ValidationError({
            diagnostics: [
              {
                severity: "error",
                message: `Validation failed: ${(err as Error).message}`,
              },
            ],
          }),
      })
      results.push(result)
    }

    // Print results
    yield* Effect.sync(() => {
      let hasErrors = false

      for (const result of results) {
        if (result.errors.length === 0 && result.warnings.length === 0) {
          console.log(
            `  ${pc.green("✓")} ${pc.bold(result.name)} ${pc.dim("— valid")}`,
          )
        } else {
          if (result.errors.length > 0) {
            hasErrors = true
            console.log(
              `  ${pc.red("✗")} ${pc.bold(result.name)} ${pc.dim(`— ${result.errors.length} error(s)`)}`,
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
    })

    // Fail with typed error if there are validation errors
    const allErrors = results.flatMap((r) => [...r.errors])
    if (allErrors.length > 0) {
      return yield* Effect.fail(new ValidationError({ diagnostics: allErrors }))
    }

    return results as readonly PipelineValidationResult[]
  })
}

// ── Deep validation via SQL Gateway ─────────────────────────────────

const DEFAULT_SQL_GATEWAY_URL = "http://localhost:8083"

/**
 * Submit EXPLAIN <sql> to a running Flink cluster via SQL Gateway.
 * Returns diagnostics for planner errors.
 */
async function runDeepValidation(
  pipelineNode: ConstructNode,
  name: string,
  flinkVersion: FlinkMajorVersion,
  projectCtx: {
    config?: { flink?: { version?: FlinkMajorVersion } } | null
    resolvedConfig?: { cluster?: { url?: unknown } } | null
  },
): Promise<ValidationDiagnostic[]> {
  const diagnostics: ValidationDiagnostic[] = []

  const rawUrl = projectCtx.resolvedConfig?.cluster?.url
  const clusterUrl =
    typeof rawUrl === "string" ? rawUrl : DEFAULT_SQL_GATEWAY_URL

  const client = new SqlGatewayClient(clusterUrl)

  // Synthesize SQL
  const appResult = synthesizeApp(
    { name, children: pipelineNode },
    { flinkVersion, synthesizedAt: new Date().toISOString() },
  )

  if (appResult.pipelines.length === 0) return diagnostics

  let sessionHandle: string
  try {
    sessionHandle = await client.openSession()
  } catch (err) {
    if (err instanceof SqlGatewayClientError) {
      diagnostics.push({
        severity: "warning",
        message: `Deep validation skipped: Flink SQL Gateway unavailable at ${clusterUrl} — ${err.message}`,
        category: "expression",
      })
      return diagnostics
    }
    throw err
  }

  try {
    for (const artifact of appResult.pipelines) {
      // Submit each statement as DDL setup, then EXPLAIN the final query
      for (const stmt of artifact.sql.statements) {
        const explainSql = `EXPLAIN ${stmt}`
        try {
          const opHandle = await client.submitStatement(
            sessionHandle,
            explainSql,
          )
          // Poll for completion
          let status = await client.getOperationStatus(sessionHandle, opHandle)
          let attempts = 0
          while (status === "RUNNING" && attempts < 30) {
            await new Promise((r) => setTimeout(r, 500))
            status = await client.getOperationStatus(sessionHandle, opHandle)
            attempts++
          }

          if (status === "ERROR") {
            diagnostics.push({
              severity: "error",
              message: `Deep validation: Flink planner rejected statement in '${artifact.name}': EXPLAIN failed`,
              component: artifact.name,
              category: "expression",
            })
          }
        } catch (err) {
          if (err instanceof SqlGatewayClientError) {
            diagnostics.push({
              severity: "error",
              message: `Deep validation: Flink planner error in '${artifact.name}': ${err.message}`,
              component: artifact.name,
              category: "expression",
            })
          } else {
            throw err
          }
        }
      }
    }
  } finally {
    try {
      await client.closeSession(sessionHandle)
    } catch {
      // Best-effort cleanup
    }
  }

  return diagnostics
}
