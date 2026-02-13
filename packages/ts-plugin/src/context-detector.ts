/**
 * AST context detector for JSX parent component identification.
 *
 * Walks the TypeScript AST to find the nearest parent JSX component
 * at a given cursor position. Used to determine which completions
 * and diagnostics to apply.
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
    return tagName.text;
  }
  if (tsModule.isPropertyAccessExpression(tagName)) {
    const obj = tagName.expression;
    if (tsModule.isIdentifier(obj)) {
      return `${obj.text}.${tagName.name.text}`;
    }
  }
  return undefined;
}

/**
 * Find the nearest parent JSX component name at a given position.
 *
 * Walks down the AST from the root to find the deepest token containing
 * the position, tracking JSX element ancestors along the way. Returns
 * the component name of the nearest enclosing JsxElement (not JsxSelfClosingElement,
 * since self-closing elements don't have children to complete).
 *
 * Fragments (`<>...</>`) are transparent — the function walks past them.
 */
export function findParentJsxComponent(
  sourceFile: ts.SourceFile,
  position: number,
  tsModule: typeof ts,
): string | undefined {
  // Walk the tree, tracking the innermost JSX parent
  let result: string | undefined;

  function visit(node: ts.Node): void {
    // Only descend into nodes that contain the position
    if (position < node.getStart(sourceFile) || position > node.getEnd()) {
      return;
    }

    // If this is a JsxElement (has opening + closing tags), it could be a parent
    if (tsModule.isJsxElement(node)) {
      const tagName = node.openingElement.tagName;
      const name = getComponentName(tagName, tsModule);
      if (name !== undefined) {
        result = name;
      }
      // Fragments: if no name resolved, don't update result (transparent)
    }

    // Recurse into children
    tsModule.forEachChild(node, visit);
  }

  visit(sourceFile);
  return result;
}
