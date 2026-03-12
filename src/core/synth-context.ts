import { Either } from "effect"
import { CycleDetectedError, ValidationError } from "./errors.js"
import type { PluginValidator } from "./plugin.js"
import type { ChangelogMode, ConstructNode, NodeKind } from "./types.js"

// ── Changelog compatibility ─────────────────────────────────────────

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

// ── Graph types ──────────────────────────────────────────────────────

export interface GraphEdge {
  readonly from: string
  readonly to: string
}

export interface ValidationDiagnostic {
  readonly severity: "error" | "warning"
  readonly message: string
  readonly nodeId?: string
  readonly component?: string
}

// ── SynthContext ──────────────────────────────────────────────────────

export class SynthContext {
  private readonly nodes: Map<string, ConstructNode> = new Map()
  private readonly adjacency: Map<string, Set<string>> = new Map()
  private readonly reverseAdj: Map<string, Set<string>> = new Map()

  /**
   * Register a construct node in the graph.
   */
  addNode(node: ConstructNode): void {
    this.nodes.set(node.id, node)
    if (!this.adjacency.has(node.id)) {
      this.adjacency.set(node.id, new Set())
    }
    if (!this.reverseAdj.has(node.id)) {
      this.reverseAdj.set(node.id, new Set())
    }
  }

  /**
   * Add a directed edge (from → to).
   */
  addEdge(from: string, to: string): void {
    if (!this.adjacency.has(from)) {
      this.adjacency.set(from, new Set())
    }
    this.adjacency.get(from)?.add(to)

    if (!this.reverseAdj.has(to)) {
      this.reverseAdj.set(to, new Set())
    }
    this.reverseAdj.get(to)?.add(from)
  }

  /**
   * Get a node by ID.
   */
  getNode(id: string): ConstructNode | undefined {
    return this.nodes.get(id)
  }

  /**
   * Get all outgoing neighbor IDs for a node.
   */
  getOutgoing(id: string): ReadonlySet<string> {
    return this.adjacency.get(id) ?? new Set()
  }

  /**
   * Get all incoming neighbor IDs for a node.
   */
  getIncoming(id: string): ReadonlySet<string> {
    return this.reverseAdj.get(id) ?? new Set()
  }

  /**
   * Collect all nodes matching the given kind(s).
   */
  getNodesByKind(...kinds: NodeKind[]): ConstructNode[] {
    const kindSet = new Set(kinds)
    const result: ConstructNode[] = []
    for (const node of this.nodes.values()) {
      if (kindSet.has(node.kind)) {
        result.push(node)
      }
    }
    return result
  }

  /**
   * Get all registered nodes.
   */
  getAllNodes(): ConstructNode[] {
    return [...this.nodes.values()]
  }

  /**
   * Get all edges in the graph.
   */
  getAllEdges(): GraphEdge[] {
    const edges: GraphEdge[] = []
    for (const [from, tos] of this.adjacency) {
      for (const to of tos) {
        edges.push({ from, to })
      }
    }
    return edges
  }

  // ── Build from construct tree ────────────────────────────────────

  /**
   * Recursively walk a construct tree and register all nodes and edges.
   * Parent → child edges represent the linear pipeline chain.
   */
  buildFromTree(root: ConstructNode): void {
    this.addNode(root)
    for (const child of root.children) {
      this.addNode(child)
      this.addEdge(root.id, child.id)
      this.buildFromTree(child)
    }
  }

  // ── Topological sort ─────────────────────────────────────────────

  /**
   * Return nodes in topological order (Kahn's algorithm).
   * Throws if the graph contains a cycle.
   */
  topologicalSort(): ConstructNode[] {
    const inDegree = new Map<string, number>()
    for (const id of this.nodes.keys()) {
      inDegree.set(id, 0)
    }
    for (const [, tos] of this.adjacency) {
      for (const to of tos) {
        inDegree.set(to, (inDegree.get(to) ?? 0) + 1)
      }
    }

    const queue: string[] = []
    for (const [id, deg] of inDegree) {
      if (deg === 0) queue.push(id)
    }

    const sorted: ConstructNode[] = []
    while (queue.length > 0) {
      const id = queue.shift()
      if (id === undefined) break
      const node = this.nodes.get(id)
      if (node) sorted.push(node)

      for (const neighbor of this.adjacency.get(id) ?? []) {
        const newDeg = (inDegree.get(neighbor) ?? 1) - 1
        inDegree.set(neighbor, newDeg)
        if (newDeg === 0) queue.push(neighbor)
      }
    }

    if (sorted.length !== this.nodes.size) {
      throw new Error("Cycle detected in pipeline graph")
    }

    return sorted
  }

  // ── Validation ───────────────────────────────────────────────────

  /**
   * Detect orphan sources: Source nodes with no outgoing edges
   * (declared but never consumed by a downstream component).
   */
  detectOrphanSources(): ValidationDiagnostic[] {
    const diagnostics: ValidationDiagnostic[] = []
    for (const node of this.nodes.values()) {
      if (node.kind === "Source") {
        const outgoing = this.adjacency.get(node.id)
        if (!outgoing || outgoing.size === 0) {
          diagnostics.push({
            severity: "error",
            message: `Orphan source '${node.component}' (${node.id}): declared but never consumed`,
            nodeId: node.id,
            component: node.component,
          })
        }
      }
    }
    return diagnostics
  }

  /**
   * Detect dangling sinks: Sink nodes with no incoming edges
   * (no input path to feed data).
   */
  detectDanglingSinks(): ValidationDiagnostic[] {
    const diagnostics: ValidationDiagnostic[] = []
    for (const node of this.nodes.values()) {
      if (node.kind === "Sink") {
        const incoming = this.reverseAdj.get(node.id)
        if (!incoming || incoming.size === 0) {
          diagnostics.push({
            severity: "error",
            message: `Dangling sink '${node.component}' (${node.id}): no input path`,
            nodeId: node.id,
            component: node.component,
          })
        }
      }
    }
    return diagnostics
  }

  /**
   * Detect cycles using DFS coloring.
   * Returns diagnostics if cycles are found.
   */
  detectCycles(): ValidationDiagnostic[] {
    const WHITE = 0,
      GRAY = 1,
      BLACK = 2
    const color = new Map<string, number>()
    for (const id of this.nodes.keys()) {
      color.set(id, WHITE)
    }

    const cycleNodes: string[] = []

    const dfs = (id: string): boolean => {
      color.set(id, GRAY)
      for (const neighbor of this.adjacency.get(id) ?? []) {
        if (color.get(neighbor) === GRAY) {
          cycleNodes.push(neighbor)
          return true
        }
        if (color.get(neighbor) === WHITE && dfs(neighbor)) {
          return true
        }
      }
      color.set(id, BLACK)
      return false
    }

    for (const id of this.nodes.keys()) {
      if (color.get(id) === WHITE) {
        if (dfs(id)) break
      }
    }

    if (cycleNodes.length > 0) {
      const node = this.nodes.get(cycleNodes[0])
      return [
        {
          severity: "error",
          message: `Cycle detected involving node '${node?.component ?? cycleNodes[0]}' (${cycleNodes[0]})`,
          nodeId: cycleNodes[0],
          component: node?.component,
        },
      ]
    }

    return []
  }

  /**
   * Detect changelog mode mismatches: sinks that only support append-only
   * streams receiving retract or upsert input.
   *
   * Walks backward from each sink to find the changelog mode propagated
   * through the incoming path. The first node with a `changelogMode` prop
   * determines the mode for that path.
   */
  detectChangelogMismatch(): ValidationDiagnostic[] {
    const diagnostics: ValidationDiagnostic[] = []

    for (const node of this.nodes.values()) {
      if (node.kind !== "Sink") continue
      if (sinkAcceptsChangelog(node)) continue

      // Walk incoming edges to find changelog mode
      const incoming = this.reverseAdj.get(node.id)
      if (!incoming) continue

      for (const parentId of incoming) {
        const mode = this.resolveChangelogMode(parentId)
        if (mode && mode !== "append-only") {
          diagnostics.push({
            severity: "error",
            message: `Sink '${node.component}' (${node.id}) does not support '${mode}' streams. Use an upsert-capable sink or add a changelog normalization step.`,
            nodeId: node.id,
            component: node.component,
          })
          break
        }
      }
    }

    return diagnostics
  }

  /**
   * Walk backward from a node to find the effective ChangelogMode.
   * Returns the first changelogMode found on the path, or undefined.
   */
  private resolveChangelogMode(nodeId: string): ChangelogMode | undefined {
    const node = this.nodes.get(nodeId)
    if (!node) return undefined

    const mode = node.props.changelogMode as ChangelogMode | undefined
    if (mode) return mode

    // Recurse through incoming edges
    const incoming = this.reverseAdj.get(nodeId)
    if (!incoming) return undefined

    for (const parentId of incoming) {
      const parentMode = this.resolveChangelogMode(parentId)
      if (parentMode) return parentMode
    }

    return undefined
  }

  /**
   * Detect materialized table structural issues: missing catalog or
   * missing upstream query (children).
   */
  detectMaterializedTableIssues(): ValidationDiagnostic[] {
    const diagnostics: ValidationDiagnostic[] = []
    for (const node of this.nodes.values()) {
      if (node.kind !== "MaterializedTable") continue

      if (!node.props.catalogName) {
        diagnostics.push({
          severity: "error",
          message: `MaterializedTable '${node.id}' requires a managed catalog`,
          nodeId: node.id,
          component: node.component,
        })
      }

      if (node.children.length === 0) {
        diagnostics.push({
          severity: "error",
          message: `MaterializedTable '${node.id}' requires an upstream query (children)`,
          nodeId: node.id,
          component: node.component,
        })
      }
    }
    return diagnostics
  }

  /**
   * Detect Qualify node structural issues: missing condition prop.
   */
  detectQualifyIssues(): ValidationDiagnostic[] {
    const diagnostics: ValidationDiagnostic[] = []
    for (const node of this.nodes.values()) {
      if (node.kind !== "Qualify") continue

      if (!node.props.condition) {
        diagnostics.push({
          severity: "error",
          message: `Qualify '${node.id}' requires a 'condition' prop`,
          nodeId: node.id,
          component: node.component,
        })
      }
    }
    return diagnostics
  }

  /**
   * Run all validations and return combined diagnostics.
   * When plugin validators are provided, they run after built-in checks
   * and receive the built-in diagnostics for context.
   */
  validate(
    root?: ConstructNode,
    pluginValidators?: readonly PluginValidator[],
  ): ValidationDiagnostic[] {
    const builtIn = [
      ...this.detectOrphanSources(),
      ...this.detectDanglingSinks(),
      ...this.detectCycles(),
      ...this.detectChangelogMismatch(),
      ...this.detectMaterializedTableIssues(),
      ...this.detectQualifyIssues(),
    ]

    if (!pluginValidators || pluginValidators.length === 0 || !root) {
      return builtIn
    }

    const pluginDiagnostics: ValidationDiagnostic[] = []
    for (const validator of pluginValidators) {
      pluginDiagnostics.push(
        ...validator(root, [...builtIn, ...pluginDiagnostics]),
      )
    }

    return [...builtIn, ...pluginDiagnostics]
  }

  // ── Effect-typed variants ─────────────────────────────────────────

  /**
   * Topological sort returning Either with typed error.
   * Synchronous, no I/O — uses Either for pure error signaling.
   */
  topologicalSortEither(): Either.Either<ConstructNode[], CycleDetectedError> {
    try {
      return Either.right(this.topologicalSort())
    } catch {
      // Find cycle participants for error context
      const cycleNodes = this.detectCycles()
      const nodeIds = cycleNodes
        .map((d) => d.nodeId)
        .filter(Boolean) as string[]
      return Either.left(
        new CycleDetectedError({
          nodeIds,
          message: "Cycle detected in pipeline graph",
        }),
      )
    }
  }

  /**
   * Validate returning Either with typed error.
   * Returns Right(warnings) or Left(ValidationError with errors).
   */
  validateEither(
    root?: ConstructNode,
    pluginValidators?: readonly PluginValidator[],
  ): Either.Either<ValidationDiagnostic[], ValidationError> {
    const all = this.validate(root, pluginValidators)
    const errors = all.filter((d) => d.severity === "error")
    const warnings = all.filter((d) => d.severity === "warning")

    if (errors.length > 0) {
      return Either.left(new ValidationError({ diagnostics: errors }))
    }

    return Either.right(warnings)
  }
}
