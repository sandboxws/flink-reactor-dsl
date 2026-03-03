import type { FlinkReactorPlugin } from "@/core/plugin.js"
import { EMPTY_PLUGIN_CHAIN, resolvePlugins } from "@/core/plugin-registry.js"
import {
  SynthContext,
  type ValidationDiagnostic,
} from "@/core/synth-context.js"
import type { ConstructNode } from "@/core/types.js"

// ── Diagnostic type (re-export for convenience) ──────────────────────

export type { ValidationDiagnostic as Diagnostic } from "@/core/synth-context.js"

// ── validate() test helper ───────────────────────────────────────────

export interface ValidateResult {
  readonly errors: readonly ValidationDiagnostic[]
  readonly warnings: readonly ValidationDiagnostic[]
}

export interface ValidateOptions {
  /** Plugins whose validators should be included */
  readonly plugins?: readonly FlinkReactorPlugin[]
}

/**
 * Validate a Pipeline JSX element and return diagnostics
 * without writing anything to disk.
 *
 * Runs all pipeline validations (orphan sources, dangling sinks,
 * cycle detection, changelog mode mismatches) plus any plugin validators.
 *
 * @example
 * ```ts
 * import { validate } from 'flink-reactor/testing';
 *
 * it('detects orphan sources', () => {
 *   const result = validate(<InvalidPipeline />);
 *   expect(result.errors).toHaveLength(1);
 *   expect(result.errors[0].message).toContain('orphan');
 * });
 * ```
 */
export function validate(
  pipeline: ConstructNode,
  options?: ValidateOptions,
): ValidateResult {
  const ctx = new SynthContext()
  ctx.buildFromTree(pipeline)

  const chain =
    options?.plugins && options.plugins.length > 0
      ? resolvePlugins(options.plugins)
      : EMPTY_PLUGIN_CHAIN

  const diagnostics = ctx.validate(
    chain.validators.length > 0 ? pipeline : undefined,
    chain.validators.length > 0 ? chain.validators : undefined,
  )

  return {
    errors: diagnostics.filter((d) => d.severity === "error"),
    warnings: diagnostics.filter((d) => d.severity === "warning"),
  }
}
