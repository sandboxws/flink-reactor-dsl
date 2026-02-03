import type { ConstructNode, FlinkMajorVersion } from '../core/types.js';
import { generateSql } from '../codegen/sql-generator.js';
import { generateCrd, type FlinkDeploymentCrd } from '../codegen/crd-generator.js';

// ── synth() test helper ──────────────────────────────────────────────

export interface SynthResult {
  readonly sql: string;
  readonly crd: FlinkDeploymentCrd;
}

export interface SynthOptions {
  readonly flinkVersion?: FlinkMajorVersion;
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
  const flinkVersion = options?.flinkVersion ?? '2.0';

  const sqlResult = generateSql(pipeline, { flinkVersion });
  const crd = generateCrd(pipeline, { flinkVersion });

  return {
    sql: sqlResult.sql,
    crd,
  };
}
