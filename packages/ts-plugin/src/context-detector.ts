/**
 * AST context detector for JSX component identification.
 *
 * Extracts component names from JSX tag name expressions.
 * Used by the diagnostics module to identify parent-child relationships.
 */
import type ts from 'typescript';

/**
 * Extract the component name from a JSX opening/self-closing element's tag name.
 * Handles simple identifiers (`Pipeline`) and property access (`Route.Branch`).
 */
export function getComponentName(
  tagName: ts.JsxTagNameExpression,
  tsModule: typeof ts,
): string | undefined {
  if (tsModule.isIdentifier(tagName)) {
    // Empty text occurs when TypeScript's error-recovery parser creates
    // a broken JsxElement for an incomplete tag (e.g., typing `<` mid-edit)
    return tagName.text || undefined;
  }
  if (tsModule.isPropertyAccessExpression(tagName)) {
    const obj = tagName.expression;
    if (tsModule.isIdentifier(obj)) {
      return `${obj.text}.${tagName.name.text}`;
    }
  }
  return undefined;
}
