import type { PluginDdlGenerator, PluginSqlGenerator } from "@/core/plugin.js"
import type { ConstructNode, FlinkMajorVersion } from "@/core/types.js"

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
  /** Plugin-provided custom SQL builders, keyed by component name. */
  readonly pluginSqlGenerators?: ReadonlyMap<string, PluginSqlGenerator>
  /** Plugin-provided custom DDL builders, keyed by component name. */
  readonly pluginDdlGenerators?: ReadonlyMap<string, PluginDdlGenerator>
}

/** A fragment of a SQL statement attributed to a construct node. */
export interface SqlFragment {
  /** Byte offset from the start of the statement. */
  readonly offset: number
  /** Length in bytes. */
  readonly length: number
  /** The construct node that produced this fragment. */
  readonly origin: {
    readonly nodeId: string
    readonly component: string
    readonly kind: string
  }
}

/** Factory for a fresh synthesis context. */
export function createBuildContext(opts: {
  readonly version: FlinkMajorVersion
  readonly pluginSqlGenerators?: ReadonlyMap<string, PluginSqlGenerator>
  readonly pluginDdlGenerators?: ReadonlyMap<string, PluginDdlGenerator>
}): BuildContext {
  return {
    version: opts.version,
    fragments: null,
    synthDepth: { value: 0 },
    pluginSqlGenerators: opts.pluginSqlGenerators,
    pluginDdlGenerators: opts.pluginDdlGenerators,
  }
}

/**
 * Enter a fresh synthesis pass. Throws on reentrant calls — a plugin that
 * calls `generateSql` inside a tree transformer or hook is the usual
 * trigger, and that path is unsupported because module-scoped fragment
 * accumulators would interleave incorrectly.
 */
export function enterSynthesis(ctx: BuildContext): void {
  if (ctx.synthDepth.value > 0) {
    throw new Error(
      "generateSql() is not reentrant — a previous synthesis is still in flight. " +
        "This usually means a plugin called generateSql() inside a tree transformer or hook. " +
        "Plugins must not invoke synthesis directly.",
    )
  }
  ctx.synthDepth.value = 1
  ctx.fragments = null
}

/** Tear down synthesis state. Always run in a `finally`. */
export function exitSynthesis(ctx: BuildContext): void {
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
