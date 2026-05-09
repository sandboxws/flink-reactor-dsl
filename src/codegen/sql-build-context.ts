import type { PluginDdlGenerator, PluginSqlGenerator } from "@/core/plugin.js"
import type { ConstructNode, FlinkMajorVersion } from "@/core/types.js"

/** Maps a statement index to the construct node that produced it. */
export interface StatementOrigin {
  /** Unique node ID from the ConstructNode tree */
  readonly nodeId: string
  /** Component class name (e.g. "KafkaSource", "Filter", "Aggregate") */
  readonly component: string
  /** Node kind (e.g. "Source", "Sink", "Transform", "Pipeline") */
  readonly kind: string
}

/**
 * Per-synthesis state threaded through every internal codegen helper.
 *
 * Every codegen function downstream of `generateSql` takes a `BuildContext`
 * as its first argument. This replaces the module-scoped `_synthVersion`
 * and `_fragments` state that prior versions of `sql-generator.ts`
 * captured via closure.
 *
 * Why a context:
 *   - Reentrant codegen — a plugin generator that re-enters `generateSql`
 *     during synthesis — corrupts module-level accumulators. The boxed
 *     `synthDepth` counter is the tripwire.
 *   - Splitting `sql-generator.ts` into focused modules (DDL, DML, query
 *     builders) is impossible while every builder reads module-private
 *     state. A `BuildContext` is the seam that lets each builder live in
 *     its own file.
 *
 * Mutability: `fragments` is a mutable accumulator (matching today's
 * push-style fragment tracking) and `synthDepth` is boxed so the value can
 * be shared across functions that increment/decrement it. Every other
 * field is read-only.
 */
/**
 * Recursive dispatcher callback. The orchestrator owns the master
 * `buildQuery` switch (or, post-C.15, a handler registry) and threads it
 * through `BuildContext` so that builders extracted into peer modules can
 * recurse into the dispatcher without taking a circular module dependency
 * back to the orchestrator.
 */
export type BuildQueryFn = (ctx: BuildContext, node: ConstructNode) => string

export interface BuildContext {
  /** Flink major version targeted by the synthesis pass. */
  readonly version: FlinkMajorVersion
  /**
   * Mutable accumulator of byte-range attributions from each component.
   * Set per-statement by `beginFragmentCollection`; consumed by callers
   * that build dashboard tooltips. `null` when fragment tracking is off
   * (e.g. between statements).
   */
  fragments: SqlFragment[] | null
  /** Reentrancy guard. Boxed so callers can mutate the counter. */
  readonly synthDepth: { value: number }
  /**
   * Per-synthesis lookup of nodes by ID. Built once from the (optimised)
   * construct tree and threaded through every builder so dispatchers can
   * resolve cross-references without walking the tree repeatedly.
   */
  readonly nodeIndex: Map<string, ConstructNode>
  /** Plugin-provided custom SQL builders, keyed by component name. */
  readonly pluginSqlGenerators?: ReadonlyMap<string, PluginSqlGenerator>
  /** Plugin-provided custom DDL builders, keyed by component name. */
  readonly pluginDdlGenerators?: ReadonlyMap<string, PluginDdlGenerator>
  /**
   * Dispatcher callback used by every builder that recurses into a child
   * (Union, FlatMap with subqueries, Aggregate's pseudo-window forwarding,
   * `getUpstream` chasing a non-Source upstream, etc.). Mutable to allow
   * the orchestrator to install the dispatcher *after* `createBuildContext`
   * runs — the typical `createBuildContext({ ...opts, buildQuery })` form
   * still works because `buildQuery` is a hoisted function declaration.
   */
  buildQuery: BuildQueryFn
}

/** A fragment of a SQL statement attributed to a construct node. */
export interface SqlFragment {
  /** Byte offset from the start of the statement. */
  readonly offset: number
  /** Length in bytes. */
  readonly length: number
  /** The construct node that produced this fragment. */
  readonly origin: StatementOrigin
}

/** Factory for a fresh synthesis context. */
export function createBuildContext(opts: {
  readonly version: FlinkMajorVersion
  readonly nodeIndex: Map<string, ConstructNode>
  readonly buildQuery: BuildQueryFn
  readonly pluginSqlGenerators?: ReadonlyMap<string, PluginSqlGenerator>
  readonly pluginDdlGenerators?: ReadonlyMap<string, PluginDdlGenerator>
}): BuildContext {
  return {
    version: opts.version,
    fragments: null,
    synthDepth: { value: 0 },
    nodeIndex: opts.nodeIndex,
    buildQuery: opts.buildQuery,
    pluginSqlGenerators: opts.pluginSqlGenerators,
    pluginDdlGenerators: opts.pluginDdlGenerators,
  }
}

/**
 * Module-level synthesis depth. Each `generateSql` call gets its own
 * `BuildContext` with its own `synthDepth.value`, so we additionally
 * track depth at the module level — that's what the reentrancy tripwire
 * checks. A plugin that synchronously calls `generateSql` inside a tree
 * transformer would otherwise just create a second ctx and proceed.
 */
let _activeSynthesisCount = 0

/**
 * Enter a fresh synthesis pass. Throws on reentrant calls — a plugin that
 * calls `generateSql` inside a tree transformer or hook is the usual
 * trigger, and that path is unsupported because fragment accumulators
 * would interleave incorrectly.
 */
export function enterSynthesis(ctx: BuildContext): void {
  if (_activeSynthesisCount > 0) {
    throw new Error(
      "generateSql() is not reentrant — a previous synthesis is still in flight. " +
        "This usually means a plugin called generateSql() inside a tree transformer or hook. " +
        "Plugins must not invoke synthesis directly.",
    )
  }
  _activeSynthesisCount = 1
  ctx.synthDepth.value = 1
  ctx.fragments = null
}

/** Tear down synthesis state. Always run in a `finally`. */
export function exitSynthesis(ctx: BuildContext): void {
  _activeSynthesisCount = 0
  ctx.synthDepth.value = 0
  ctx.fragments = null
}

/** Begin collecting fragments for the current statement. */
export function beginFragmentCollection(ctx: BuildContext): SqlFragment[] {
  const fragments: SqlFragment[] = []
  ctx.fragments = fragments
  return fragments
}

/** Stop collecting fragments for the current statement. */
export function endFragmentCollection(ctx: BuildContext): void {
  ctx.fragments = null
}

/**
 * Push a fragment for the current node's contribution. No-op when not
 * collecting (i.e. between statements).
 */
export function pushFragment(
  ctx: BuildContext,
  offset: number,
  length: number,
  node: ConstructNode,
): void {
  if (ctx.fragments) {
    ctx.fragments.push({
      offset,
      length,
      origin: { nodeId: node.id, component: node.component, kind: node.kind },
    })
  }
}

/** Shift all fragments from `startIndex` onward by `delta` bytes. */
export function shiftFragmentsSince(
  ctx: BuildContext,
  startIndex: number,
  delta: number,
): void {
  if (!ctx.fragments) return
  for (let i = startIndex; i < ctx.fragments.length; i++) {
    ctx.fragments[i] = {
      ...ctx.fragments[i],
      offset: ctx.fragments[i].offset + delta,
    }
  }
}
