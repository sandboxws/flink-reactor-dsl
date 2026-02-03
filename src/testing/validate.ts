import type { ConstructNode } from '../core/types.js';
import { SynthContext, type ValidationDiagnostic } from '../core/synth-context.js';

// ── Diagnostic type (re-export for convenience) ──────────────────────

export type { ValidationDiagnostic as Diagnostic } from '../core/synth-context.js';

// ── validate() test helper ───────────────────────────────────────────

export interface ValidateResult {
  readonly errors: readonly ValidationDiagnostic[];
  readonly warnings: readonly ValidationDiagnostic[];
}

/**
 * Validate a Pipeline JSX element and return diagnostics
 * without writing anything to disk.
 *
 * Runs all pipeline validations (orphan sources, dangling sinks,
 * cycle detection, changelog mode mismatches).
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
export function validate(pipeline: ConstructNode): ValidateResult {
  const ctx = new SynthContext();
  ctx.buildFromTree(pipeline);

  const diagnostics = ctx.validate();

  return {
    errors: diagnostics.filter((d) => d.severity === 'error'),
    warnings: diagnostics.filter((d) => d.severity === 'warning'),
  };
}
