/**
 * Completion processing for flink-reactor JSX components.
 *
 * Uses the component rules registry to filter or rank completion entries
 * based on the current JSX parent context. Supports two strategies:
 * - `filter`: removes invalid DSL child suggestions entirely
 * - `rank`: promotes valid children to the top while preserving all entries
 */
import type ts from "typescript"
import {
  DSL_COMPONENTS,
  HIERARCHY_ONLY_COMPONENTS,
} from "./component-inventory"
import { getComponentName } from "./context-detector"
import type { ComponentRulesRegistry } from "./types"

/** Check if an entry name is a known DSL component (including hierarchy-only sub-components) */
function isDSLComponent(name: string): boolean {
  return DSL_COMPONENTS.has(name) || HIERARCHY_ONLY_COMPONENTS.includes(name)
}

/**
 * Resolve parent-child rules from a tag name expression.
 * Returns undefined if the context is ambiguous (passthrough).
 */
function resolveAllowed(
  parentTagName: ts.JsxTagNameExpression | undefined,
  registry: ComponentRulesRegistry,
  tsModule: typeof ts,
): string[] | "*" | undefined {
  if (!parentTagName) return undefined

  const parentName = getComponentName(parentTagName, tsModule)
  if (parentName === undefined) return undefined

  return registry.getAllowedChildren(parentName)
}

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
  const allowed = resolveAllowed(parentTagName, registry, tsModule)
  if (allowed === undefined || allowed === "*") return [...entries]

  return entries.filter(
    (entry) => allowed.includes(entry.name) || !isDSLComponent(entry.name),
  )
}

/**
 * Rank completion entries so valid DSL children appear first.
 *
 * All entries are preserved (including non-DSL entries), but valid
 * DSL children are promoted with a lower sortText prefix ("0"),
 * invalid DSL children are demoted ("2"), and non-DSL entries
 * keep their original sort position ("1" prefix).
 */
export function rankCompletionsByContext(
  entries: readonly ts.CompletionEntry[],
  parentTagName: ts.JsxTagNameExpression | undefined,
  registry: ComponentRulesRegistry,
  tsModule: typeof ts,
): ts.CompletionEntry[] {
  const allowed = resolveAllowed(parentTagName, registry, tsModule)
  if (allowed === undefined || allowed === "*") return [...entries]

  return entries.map((entry) => {
    const isDSL = isDSLComponent(entry.name)
    if (!isDSL) return entry

    const isValid = allowed.includes(entry.name)
    return {
      ...entry,
      sortText: isValid ? `0${entry.sortText}` : `2${entry.sortText}`,
    }
  })
}
