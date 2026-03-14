/**
 * AST context detector for JSX component identification.
 *
 * Extracts component names from JSX tag name expressions and resolves
 * the parent JSX element at a given cursor position.
 */
import type ts from "typescript"

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
    return tagName.text || undefined
  }
  if (tsModule.isPropertyAccessExpression(tagName)) {
    const obj = tagName.expression
    if (tsModule.isIdentifier(obj)) {
      return `${obj.text}.${tagName.name.text}`
    }
  }
  return undefined
}

/**
 * Find the tag name of the nearest enclosing JSX element at a given position.
 *
 * Walks up the AST from the deepest node containing `position` and returns
 * the tag name of the first JsxElement whose children span includes the cursor.
 * Returns undefined if the position is not inside a JSX element's children.
 */
export function getParentTagAtPosition(
  sourceFile: ts.SourceFile,
  position: number,
  tsModule: typeof ts,
): ts.JsxTagNameExpression | undefined {
  function walk(node: ts.Node): ts.JsxTagNameExpression | undefined {
    // If the cursor isn't within this node's span, skip it
    if (position < node.getStart(sourceFile) || position > node.getEnd()) {
      return undefined
    }

    // Check children first (deepest match wins)
    let childResult: ts.JsxTagNameExpression | undefined
    tsModule.forEachChild(node, (child) => {
      if (childResult) return
      const result = walk(child)
      if (result) childResult = result
    })
    if (childResult) return childResult

    // If this node is a JsxElement and the position is inside its children
    // (between opening and closing tags), return its tag name
    if (tsModule.isJsxElement(node)) {
      const openEnd = node.openingElement.getEnd()
      const closeStart = node.closingElement.getStart(sourceFile)
      if (position >= openEnd && position <= closeStart) {
        return node.openingElement.tagName
      }
    }

    return undefined
  }

  return walk(sourceFile)
}
