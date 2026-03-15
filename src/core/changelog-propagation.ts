import type { SynthContext, ValidationDiagnostic } from "./synth-context.js"
import type { ChangelogMode, ConstructNode } from "./types.js"

// ── Changelog mode computation rules ────────────────────────────────

/** Sinks that natively support retract/upsert streams */
const CHANGELOG_CAPABLE_SINKS: ReadonlySet<string> = new Set([
  "JdbcSink", // with upsertMode=true
  "PaimonSink",
  "IcebergSink",
])

function sinkAcceptsChangelog(node: ConstructNode): boolean {
  if (node.component === "JdbcSink") {
    return node.props.upsertMode === true
  }
  return CHANGELOG_CAPABLE_SINKS.has(node.component)
}

/** Components that pass through changelog mode unchanged */
const PASSTHROUGH_COMPONENTS: ReadonlySet<string> = new Set([
  "Filter",
  "Map",
  "FlatMap",
  "TopN",
])

/**
 * Check whether an Aggregate node is inside a window by looking
 * at its parent nodes in the construct tree. A windowed aggregate
 * produces append-only output; an unbounded aggregate produces retract.
 */
function isWindowedAggregate(node: ConstructNode, ctx: SynthContext): boolean {
  const parents = ctx.getIncoming(node.id)
  for (const parentId of parents) {
    const parent = ctx.getNode(parentId)
    if (parent?.kind === "Window") return true
  }
  return false
}

/**
 * Compute the output changelog mode for a single node given its
 * input modes (from upstream nodes).
 */
function computeNodeOutputMode(
  node: ConstructNode,
  inputModes: ChangelogMode[],
  ctx: SynthContext,
): ChangelogMode {
  // Sources declare their own mode
  if (node.kind === "Source") {
    return (node.props.changelogMode as ChangelogMode) ?? "append-only"
  }

  // Pipeline and Window are structural — pass through
  if (node.kind === "Pipeline" || node.kind === "Window") {
    return inputModes.length > 0 ? inputModes[0] : "append-only"
  }

  // Passthrough transforms: preserve upstream mode
  if (PASSTHROUGH_COMPONENTS.has(node.component)) {
    return inputModes.length > 0 ? inputModes[0] : "append-only"
  }

  // Aggregate: retract if unbounded, append-only if windowed
  if (node.component === "Aggregate") {
    if (isWindowedAggregate(node, ctx)) {
      return "append-only"
    }
    return "retract"
  }

  // Deduplicate: always produces append-only (ROW_NUMBER WHERE rownum = 1)
  if (node.component === "Deduplicate") {
    return "append-only"
  }

  // Union: retract if any input is retract
  if (node.component === "Union") {
    return inputModes.some((m) => m === "retract") ? "retract" : "append-only"
  }

  // Join: conservative — if any input is retract, output is retract
  if (node.kind === "Join") {
    return inputModes.some((m) => m === "retract") ? "retract" : "append-only"
  }

  // Sinks, views, and other nodes: pass through
  if (node.kind === "Sink") {
    return inputModes.length > 0 ? inputModes[0] : "append-only"
  }

  // Default: pass through
  return inputModes.length > 0 ? inputModes[0] : "append-only"
}

// ── Public API ──────────────────────────────────────────────────────

/**
 * Compute changelog modes for every node in the pipeline DAG.
 *
 * Traverses nodes in topological order (sources first, sinks last)
 * and computes each node's output changelog mode based on its
 * component semantics and input modes.
 */
export function computeChangelogModes(
  ctx: SynthContext,
): Map<string, ChangelogMode> {
  const modes = new Map<string, ChangelogMode>()
  const sorted = ctx.topologicalSort()

  for (const node of sorted) {
    // Collect modes from all incoming (upstream) nodes
    const incoming = ctx.getIncoming(node.id)
    const inputModes: ChangelogMode[] = []
    for (const parentId of incoming) {
      const parentMode = modes.get(parentId)
      if (parentMode) inputModes.push(parentMode)
    }

    const outputMode = computeNodeOutputMode(node, inputModes, ctx)
    modes.set(node.id, outputMode)
  }

  return modes
}

/**
 * Validate changelog mode compatibility at every node in the pipeline.
 *
 * Checks:
 * 1. Sinks receive compatible changelog modes (append-only sinks
 *    reject retract/upsert streams)
 * 2. Intermediate nodes receive compatible input modes
 *
 * Returns diagnostics with `category: "changelog"`.
 */
export function validateChangelogModes(
  ctx: SynthContext,
): ValidationDiagnostic[] {
  const diagnostics: ValidationDiagnostic[] = []
  const modes = computeChangelogModes(ctx)

  for (const [nodeId, _mode] of modes) {
    const node = ctx.getNode(nodeId)
    if (!node) continue

    // Check sink compatibility
    if (node.kind === "Sink") {
      if (!sinkAcceptsChangelog(node)) {
        const incoming = ctx.getIncoming(node.id)
        for (const parentId of incoming) {
          const inputMode = modes.get(parentId)
          if (inputMode && inputMode !== "append-only") {
            diagnostics.push({
              severity: "error",
              message: `Sink '${node.component}' (${node.id}) does not support '${inputMode}' streams. Use an upsert-capable sink or add a changelog normalization step.`,
              nodeId: node.id,
              component: node.component,
              category: "changelog",
            })
            break
          }
        }
      }
    }

    // Check intermediate node compatibility:
    // Deduplicate requires append-only input (it's a first/last-row pattern)
    if (node.component === "Deduplicate") {
      const incoming = ctx.getIncoming(node.id)
      for (const parentId of incoming) {
        const inputMode = modes.get(parentId)
        if (inputMode && inputMode === "retract") {
          diagnostics.push({
            severity: "warning",
            message: `Deduplicate '${node.id}' receives a retract stream — deduplication on retract streams may produce unexpected results.`,
            nodeId: node.id,
            component: node.component,
            category: "changelog",
          })
          break
        }
      }
    }
  }

  return diagnostics
}
