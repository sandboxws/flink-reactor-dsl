/**
 * Completion filtering for flink-reactor JSX components.
 *
 * Uses the component rules registry to filter completion entries
 * based on the current JSX parent context. This module provides
 * the interface and utilities for context-aware completions.
 */
import type ts from "typescript"
import type { ComponentRulesRegistry } from "./types"
import { getComponentName } from "./context-detector"

/**
 * Filter completion entries to only show valid children for the
 * current JSX parent context.
 *
 * Returns the original entries unmodified if:
 * - The cursor is not inside a JSX element
 * - The parent component is unrecognized (allows everything)
 * - The parent has a wildcard rule
 */
export function filterCompletionsByContext(
  entries: readonly ts.CompletionEntry[],
  parentTagName: ts.JsxTagNameExpression | undefined,
  registry: ComponentRulesRegistry,
  tsModule: typeof ts,
): ts.CompletionEntry[] {
  if (!parentTagName) return [...entries]

  const parentName = getComponentName(parentTagName, tsModule)
  if (parentName === undefined) return [...entries]

  const allowed = registry.getAllowedChildren(parentName)
  if (allowed === undefined || allowed === "*") return [...entries]

  return entries.filter((entry) => allowed.includes(entry.name))
}
