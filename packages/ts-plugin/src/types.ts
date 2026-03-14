/**
 * Shared type contracts for flink-reactor ts-plugin modules.
 *
 * Each module exposes its behavior through these interfaces,
 * enabling clear boundaries and testable seams.
 */
import type ts from "typescript"

/** Extracts component names from JSX tag expressions */
export interface ContextDetector {
  getComponentName(
    tagName: ts.JsxTagNameExpression,
    tsModule: typeof ts,
  ): string | undefined
}

/** Validates parent-child component hierarchy */
export interface ComponentRulesRegistry {
  getAllowedChildren(parent: string): string[] | "*" | undefined
  isValidChild(parent: string, child: string): boolean
  /** List all parents that have explicit rules */
  getRegisteredParents(): string[]
  /** List all component names referenced in rules (parents + children) */
  getAllReferencedComponents(): string[]
}

/** Produces diagnostics for a source file */
export interface DiagnosticsProvider {
  getDiagnostics(
    sourceFile: ts.SourceFile,
    registry: ComponentRulesRegistry,
    tsModule: typeof ts,
  ): ts.Diagnostic[]
}

/** Filters completions based on JSX parent context */
export interface CompletionsFilter {
  filterByContext(
    entries: readonly ts.CompletionEntry[],
    parentTagName: ts.JsxTagNameExpression | undefined,
    registry: ComponentRulesRegistry,
    tsModule: typeof ts,
  ): ts.CompletionEntry[]
}
