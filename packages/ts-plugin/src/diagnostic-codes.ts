/**
 * Standardized diagnostic codes and message conventions.
 *
 * All flink-reactor plugin diagnostics use codes in the 90xxx range
 * to avoid collision with TypeScript's built-in codes (1xxx–9xxx).
 *
 * Naming convention: FR_<CATEGORY>_<SPECIFIC>
 * Message convention: "'<subject>' <problem>. <guidance>"
 */

/** Diagnostic source identifier used in all plugin diagnostics */
export const DIAGNOSTIC_SOURCE = "flink-reactor" as const

/**
 * Diagnostic code registry.
 *
 * Ranges:
 * - 90100–90199: Nesting / hierarchy validation
 * - 90200–90299: Reserved for completions
 * - 90300–90399: Reserved for type checking
 */
export const DiagnosticCodes = {
  /** Invalid parent-child nesting in JSX component hierarchy */
  INVALID_NESTING: 90100,
} as const

export type DiagnosticCode =
  (typeof DiagnosticCodes)[keyof typeof DiagnosticCodes]

/**
 * Create a standardized diagnostic message for invalid nesting.
 */
export function invalidNestingMessage(
  childName: string,
  parentName: string,
  allowed: string[],
): string {
  return `'${childName}' is not a valid child of '${parentName}'. Expected: ${allowed.join(", ")}`
}
