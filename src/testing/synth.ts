import { Effect } from "effect"
import { type AnyFlinkCrd, generateCrd } from "@/codegen/crd-generator.js"
import { generateSql } from "@/codegen/sql-generator.js"
import type {
  CrdGenerationError,
  PluginError,
  SqlGenerationError,
} from "@/core/errors.js"
import {
  registerComponentKinds,
  resetComponentKinds,
} from "@/core/jsx-runtime.js"
import type { FlinkReactorPlugin } from "@/core/plugin.js"
import { EMPTY_PLUGIN_CHAIN, resolvePlugins } from "@/core/plugin-registry.js"
import { rekindTree } from "@/core/tree-utils.js"
import type { ConstructNode, FlinkMajorVersion } from "@/core/types.js"

// ── synth() test helper ──────────────────────────────────────────────

export interface SynthResult {
  readonly sql: string
  readonly crd: AnyFlinkCrd
}

export interface SynthOptions {
  readonly flinkVersion?: FlinkMajorVersion
  /** Plugins to apply during synthesis */
  readonly plugins?: readonly FlinkReactorPlugin[]
}

/**
 * Synthesize a Pipeline JSX element in-memory and return
 * the generated SQL and CRD without writing to disk.
 *
 * Designed for use in Vitest tests with `toMatchSnapshot()`.
 *
 * @example
 * ```ts
 * import { synth } from 'flink-reactor/testing';
 *
 * it('produces correct SQL', () => {
 *   const result = synth(<OrderProcessing />);
 *   expect(result.sql).toMatchSnapshot();
 *   expect(result.crd).toMatchSnapshot();
 * });
 * ```
 */
export function synth(
  pipeline: ConstructNode,
  options?: SynthOptions,
): SynthResult {
  const flinkVersion = options?.flinkVersion ?? "2.0"

  // Resolve plugins if provided
  const chain =
    options?.plugins && options.plugins.length > 0
      ? resolvePlugins(options.plugins)
      : EMPTY_PLUGIN_CHAIN

  if (chain.components.size > 0) {
    registerComponentKinds(chain.components)
  }

  try {
    // Re-resolve node kinds for plugin-registered components
    let node =
      chain.components.size > 0
        ? rekindTree(pipeline, chain.components)
        : pipeline

    // Apply tree transformers
    node = chain.transformTree(node)

    const sqlResult = generateSql(node, {
      flinkVersion,
      pluginSqlGenerators:
        chain.sqlGenerators.size > 0 ? chain.sqlGenerators : undefined,
      pluginDdlGenerators:
        chain.ddlGenerators.size > 0 ? chain.ddlGenerators : undefined,
    })

    let crd = generateCrd(node, { flinkVersion })

    // Apply CRD transformers
    crd = chain.transformCrd(crd, node)

    return {
      sql: sqlResult.sql,
      crd,
    }
  } finally {
    if (chain.components.size > 0) {
      resetComponentKinds()
    }
  }
}

// ── Effect variant ──────────────────────────────────────────────────

/**
 * Effect-based synthesis test helper.
 *
 * Same as `synth()` but returns an Effect with typed errors.
 * Useful when composing synthesis with other Effect operations
 * in test pipelines.
 *
 * @example
 * ```ts
 * import { synthEffect } from 'flink-reactor/testing';
 *
 * it('produces correct SQL', async () => {
 *   const result = await Effect.runPromise(synthEffect(<OrderProcessing />));
 *   expect(result.sql).toMatchSnapshot();
 * });
 * ```
 */
export function synthEffect(
  pipeline: ConstructNode,
  options?: SynthOptions,
): Effect.Effect<
  SynthResult,
  PluginError | SqlGenerationError | CrdGenerationError
> {
  return Effect.acquireUseRelease(
    // Acquire: resolve plugins
    Effect.sync(() => {
      const chain =
        options?.plugins && options.plugins.length > 0
          ? resolvePlugins(options.plugins)
          : EMPTY_PLUGIN_CHAIN

      if (chain.components.size > 0) {
        registerComponentKinds(chain.components)
      }

      return chain
    }),
    // Use: run synthesis
    (chain) =>
      Effect.try({
        try: () => {
          const flinkVersion = options?.flinkVersion ?? "2.0"

          let node =
            chain.components.size > 0
              ? rekindTree(pipeline, chain.components)
              : pipeline

          node = chain.transformTree(node)

          const sqlResult = generateSql(node, {
            flinkVersion,
            pluginSqlGenerators:
              chain.sqlGenerators.size > 0 ? chain.sqlGenerators : undefined,
            pluginDdlGenerators:
              chain.ddlGenerators.size > 0 ? chain.ddlGenerators : undefined,
          })

          let crd = generateCrd(node, { flinkVersion })
          crd = chain.transformCrd(crd, node)

          return { sql: sqlResult.sql, crd } as SynthResult
        },
        catch: (err) => err as Error,
      }) as Effect.Effect<SynthResult>,
    // Release: clean up
    (chain) =>
      Effect.sync(() => {
        if (chain.components.size > 0) {
          resetComponentKinds()
        }
      }),
  )
}
