/**
 * Completion filter for JSX context-aware autocomplete.
 *
 * Filters TypeScript completion entries based on the parent JSX component,
 * keeping only valid children for recognized parents. Non-component completions
 * (variables, keywords, snippets) are always preserved.
 */
import type ts from 'typescript';
import type { ComponentRulesRegistry } from './component-rules';

/** ScriptElementKind values that represent potential JSX component completions */
const COMPONENT_KINDS = new Set<string>([
  'function',         // ScriptElementKind.functionElement
  'class',            // ScriptElementKind.classElement
  'alias',            // ScriptElementKind.alias (re-exports, type aliases)
  'const',            // ScriptElementKind.constElement (arrow function components)
  'let',              // ScriptElementKind.letElement
  'var',              // ScriptElementKind.variableElement
]);

/**
 * Determine if a completion entry looks like a JSX component name.
 * JSX components start with an uppercase letter.
 */
function isComponentEntry(entry: ts.CompletionEntry): boolean {
  if (!COMPONENT_KINDS.has(entry.kind)) return false;
  // JSX component names start with uppercase
  return entry.name.length > 0 && entry.name[0] === entry.name[0].toUpperCase()
    && entry.name[0] !== entry.name[0].toLowerCase();
}

/**
 * Filter completions based on the parent component's allowed children.
 *
 * - If parentName is undefined → passthrough (not in JSX context)
 * - If parent is unrecognized → passthrough
 * - If parent has '*' rule → passthrough
 * - If parent has explicit children list → filter to only those component names
 * - Non-component entries (keywords, variables, snippets) always pass through
 */
export function filterCompletions(
  original: ts.CompletionInfo,
  parentName: string | undefined,
  registry: ComponentRulesRegistry,
): ts.CompletionInfo {
  if (parentName === undefined) return original;

  const allowed = registry.getAllowedChildren(parentName);
  if (allowed === undefined || allowed === '*') return original;

  const allowedSet = new Set(allowed);

  const filtered = original.entries.filter((entry) => {
    if (!isComponentEntry(entry)) return true;
    return allowedSet.has(entry.name);
  });

  return {
    ...original,
    entries: filtered,
  };
}
