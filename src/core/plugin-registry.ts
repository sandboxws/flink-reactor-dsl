import type { FlinkDeploymentCrd } from "../codegen/crd-generator.js"
import type {
  FlinkReactorPlugin,
  PluginDdlGenerator,
  PluginSqlGenerator,
  PluginValidator,
} from "./plugin.js"
import type { ConstructNode, NodeKind } from "./types.js"

// ── Resolved plugin chain ───────────────────────────────────────────

/**
 * The result of resolving and ordering plugins.
 * Contains merged maps and composed functions ready for use
 * in the synthesis pipeline.
 */
export interface ResolvedPluginChain {
  /** Ordered list of plugin names (for debugging/logging) */
  readonly order: readonly string[]
  /** Merged component registrations (component name → NodeKind) */
  readonly components: ReadonlyMap<string, NodeKind>
  /** Composed tree transformer (identity if no plugins have transformTree) */
  readonly transformTree: (tree: ConstructNode) => ConstructNode
  /** Merged SQL query generators (component name → generator) */
  readonly sqlGenerators: ReadonlyMap<string, PluginSqlGenerator>
  /** Merged DDL generators (component name → generator) */
  readonly ddlGenerators: ReadonlyMap<string, PluginDdlGenerator>
  /** Composed CRD transformer (identity if no plugins have transformCrd) */
  readonly transformCrd: (
    crd: FlinkDeploymentCrd,
    pipeline: ConstructNode,
  ) => FlinkDeploymentCrd
  /** Ordered list of plugin validators */
  readonly validators: readonly PluginValidator[]
  /** Ordered beforeSynth hooks */
  readonly beforeSynth: readonly FlinkReactorPlugin["beforeSynth"][]
  /** Ordered afterSynth hooks */
  readonly afterSynth: readonly FlinkReactorPlugin["afterSynth"][]
}

// ── Empty chain constant ────────────────────────────────────────────

const IDENTITY_TREE = (tree: ConstructNode): ConstructNode => tree
const IDENTITY_CRD = (crd: FlinkDeploymentCrd): FlinkDeploymentCrd => crd

export const EMPTY_PLUGIN_CHAIN: ResolvedPluginChain = {
  order: [],
  components: new Map(),
  transformTree: IDENTITY_TREE,
  sqlGenerators: new Map(),
  ddlGenerators: new Map(),
  transformCrd: IDENTITY_CRD,
  validators: [],
  beforeSynth: [],
  afterSynth: [],
}

// ── resolvePlugins ──────────────────────────────────────────────────

/**
 * Resolve, validate, order, and compose an array of plugins into a
 * single ResolvedPluginChain ready for the synthesis pipeline.
 *
 * Steps:
 * 1. Validate: no duplicate names
 * 2. Order: topological sort on before/after constraints,
 *    with alphabetical name as deterministic tiebreaker
 * 3. Merge: build combined maps, detecting conflicts
 * 4. Compose: left-fold tree/CRD transformers
 *
 * @throws Error on duplicate plugin names, conflicting registrations,
 *         or circular ordering constraints
 */
export function resolvePlugins(
  plugins: readonly FlinkReactorPlugin[],
): ResolvedPluginChain {
  if (plugins.length === 0) return EMPTY_PLUGIN_CHAIN

  // 1. Validate unique names
  validateUniqueNames(plugins)

  // 2. Topological sort with alphabetical tiebreaker
  const ordered = topologicalSortPlugins(plugins)

  // 3. Merge maps with conflict detection
  const components = mergeComponents(ordered)
  const sqlGenerators = mergeSqlGenerators(ordered)
  const ddlGenerators = mergeDdlGenerators(ordered)

  // 4. Compose transformers (left-fold)
  const transformTree = composeTreeTransformers(ordered)
  const transformCrd = composeCrdTransformers(ordered)

  // 5. Collect validators and hooks in order
  const validators = ordered
    .filter((p) => p.validate != null)
    .map((p) => p.validate!)

  const beforeSynth = ordered
    .filter((p) => p.beforeSynth != null)
    .map((p) => p.beforeSynth!)

  const afterSynth = ordered
    .filter((p) => p.afterSynth != null)
    .map((p) => p.afterSynth!)

  return {
    order: ordered.map((p) => p.name),
    components,
    transformTree,
    sqlGenerators,
    ddlGenerators,
    transformCrd,
    validators,
    beforeSynth,
    afterSynth,
  }
}

// ── Validation ──────────────────────────────────────────────────────

function validateUniqueNames(plugins: readonly FlinkReactorPlugin[]): void {
  const seen = new Set<string>()
  for (const plugin of plugins) {
    if (seen.has(plugin.name)) {
      throw new Error(`Duplicate plugin name '${plugin.name}'`)
    }
    seen.add(plugin.name)
  }
}

// ── Topological sort ────────────────────────────────────────────────

/**
 * Topological sort on plugin ordering constraints.
 * Uses Kahn's algorithm with a sorted queue for deterministic tiebreaking.
 *
 * "before" constraint: plugin A lists B in `ordering.before` → A comes before B (edge A → B)
 * "after" constraint: plugin A lists B in `ordering.after` → B comes before A (edge B → A)
 */
function topologicalSortPlugins(
  plugins: readonly FlinkReactorPlugin[],
): FlinkReactorPlugin[] {
  const byName = new Map<string, FlinkReactorPlugin>()
  for (const p of plugins) {
    byName.set(p.name, p)
  }

  // Build adjacency: edges[a] = set of b means a must come before b
  const edges = new Map<string, Set<string>>()
  const inDegree = new Map<string, number>()

  for (const p of plugins) {
    edges.set(p.name, new Set())
    inDegree.set(p.name, 0)
  }

  for (const p of plugins) {
    // "before" means this plugin must come before the listed plugins
    if (p.ordering?.before) {
      for (const target of p.ordering.before) {
        if (byName.has(target)) {
          edges.get(p.name)?.add(target)
          inDegree.set(target, (inDegree.get(target) ?? 0) + 1)
        }
      }
    }
    // "after" means the listed plugins must come before this plugin
    if (p.ordering?.after) {
      for (const dep of p.ordering.after) {
        if (byName.has(dep)) {
          edges.get(dep)?.add(p.name)
          inDegree.set(p.name, (inDegree.get(p.name) ?? 0) + 1)
        }
      }
    }
  }

  // Kahn's algorithm with sorted queue for deterministic output
  const queue: string[] = []
  for (const [name, deg] of inDegree) {
    if (deg === 0) queue.push(name)
  }
  // Sort alphabetically for deterministic tiebreaking
  queue.sort()

  const result: FlinkReactorPlugin[] = []
  while (queue.length > 0) {
    const name = queue.shift()!
    result.push(byName.get(name)!)

    for (const neighbor of edges.get(name) ?? []) {
      const newDeg = (inDegree.get(neighbor) ?? 1) - 1
      inDegree.set(neighbor, newDeg)
      if (newDeg === 0) {
        // Insert into sorted position for deterministic ordering
        const insertIdx = queue.findIndex((q) => q > neighbor)
        if (insertIdx === -1) {
          queue.push(neighbor)
        } else {
          queue.splice(insertIdx, 0, neighbor)
        }
      }
    }
  }

  if (result.length !== plugins.length) {
    const missing = plugins
      .filter((p) => !result.some((r) => r.name === p.name))
      .map((p) => p.name)
    throw new Error(
      `Circular ordering constraint among plugins: ${missing.join(", ")}`,
    )
  }

  return result
}

// ── Map merging with conflict detection ─────────────────────────────

function mergeComponents(
  plugins: readonly FlinkReactorPlugin[],
): ReadonlyMap<string, NodeKind> {
  const merged = new Map<string, NodeKind>()
  const owners = new Map<string, string>()

  for (const plugin of plugins) {
    if (!plugin.components) continue
    for (const [name, kind] of plugin.components) {
      if (merged.has(name)) {
        throw new Error(
          `Component '${name}' registered by both '${owners.get(name)}' and '${plugin.name}'`,
        )
      }
      merged.set(name, kind)
      owners.set(name, plugin.name)
    }
  }

  return merged
}

function mergeSqlGenerators(
  plugins: readonly FlinkReactorPlugin[],
): ReadonlyMap<string, PluginSqlGenerator> {
  const merged = new Map<string, PluginSqlGenerator>()
  const owners = new Map<string, string>()

  for (const plugin of plugins) {
    if (!plugin.sqlGenerators) continue
    for (const [name, gen] of plugin.sqlGenerators) {
      if (merged.has(name)) {
        throw new Error(
          `SQL generator for '${name}' registered by both '${owners.get(name)}' and '${plugin.name}'`,
        )
      }
      merged.set(name, gen)
      owners.set(name, plugin.name)
    }
  }

  return merged
}

function mergeDdlGenerators(
  plugins: readonly FlinkReactorPlugin[],
): ReadonlyMap<string, PluginDdlGenerator> {
  const merged = new Map<string, PluginDdlGenerator>()
  const owners = new Map<string, string>()

  for (const plugin of plugins) {
    if (!plugin.ddlGenerators) continue
    for (const [name, gen] of plugin.ddlGenerators) {
      if (merged.has(name)) {
        throw new Error(
          `DDL generator for '${name}' registered by both '${owners.get(name)}' and '${plugin.name}'`,
        )
      }
      merged.set(name, gen)
      owners.set(name, plugin.name)
    }
  }

  return merged
}

// ── Transformer composition ─────────────────────────────────────────

function composeTreeTransformers(
  plugins: readonly FlinkReactorPlugin[],
): (tree: ConstructNode) => ConstructNode {
  const transformers = plugins
    .filter((p) => p.transformTree != null)
    .map((p) => p.transformTree!)

  if (transformers.length === 0) return IDENTITY_TREE
  if (transformers.length === 1) return transformers[0]

  // Left-fold composition: plugin 1 output → plugin 2 input
  return (tree: ConstructNode) => transformers.reduce((t, fn) => fn(t), tree)
}

function composeCrdTransformers(
  plugins: readonly FlinkReactorPlugin[],
): (crd: FlinkDeploymentCrd, pipeline: ConstructNode) => FlinkDeploymentCrd {
  const transformers = plugins
    .filter((p) => p.transformCrd != null)
    .map((p) => p.transformCrd!)

  if (transformers.length === 0) return IDENTITY_CRD
  if (transformers.length === 1) return transformers[0]

  return (crd: FlinkDeploymentCrd, pipeline: ConstructNode) =>
    transformers.reduce((c, fn) => fn(c, pipeline), crd)
}
