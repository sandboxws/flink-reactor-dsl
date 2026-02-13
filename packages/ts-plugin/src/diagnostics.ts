/**
 * Nesting diagnostics for flink-reactor JSX components.
 *
 * Walks all JsxElement nodes in a source file and checks that each
 * direct JSX child is valid for its parent according to the component
 * hierarchy registry. Produces Warning-level diagnostics for invalid
 * nesting.
 */
import type ts from 'typescript';
import type { ComponentRulesRegistry } from './component-rules';
import { getComponentName } from './context-detector';

const DIAGNOSTIC_SOURCE = 'flink-reactor';

/**
 * Generate nesting diagnostics for a source file.
 *
 * Walks the AST and for each JsxElement, checks its direct JSX children
 * against the parent's rules. Returns an array of Warning diagnostics.
 */
export function getNestingDiagnostics(
  sourceFile: ts.SourceFile,
  registry: ComponentRulesRegistry,
  tsModule: typeof ts,
): ts.Diagnostic[] {
  const diagnostics: ts.Diagnostic[] = [];

  function visit(node: ts.Node): void {
    if (tsModule.isJsxElement(node)) {
      const parentName = getComponentName(node.openingElement.tagName, tsModule);
      if (parentName !== undefined) {
        const allowed = registry.getAllowedChildren(parentName);
        // Skip if parent is unrecognized or has wildcard rule
        if (allowed !== undefined && allowed !== '*') {
          checkChildren(node, parentName, allowed, sourceFile, tsModule, diagnostics);
        }
      }
    }

    tsModule.forEachChild(node, visit);
  }

  visit(sourceFile);
  return diagnostics;
}

function checkChildren(
  parentNode: ts.JsxElement,
  parentName: string,
  allowed: string[],
  sourceFile: ts.SourceFile,
  tsModule: typeof ts,
  diagnostics: ts.Diagnostic[],
): void {
  for (const child of parentNode.children) {
    let childName: string | undefined;
    let tagSpan: { start: number; length: number } | undefined;

    if (tsModule.isJsxElement(child)) {
      childName = getComponentName(child.openingElement.tagName, tsModule);
      if (childName !== undefined) {
        const tagNameNode = child.openingElement.tagName;
        tagSpan = {
          start: tagNameNode.getStart(sourceFile),
          length: tagNameNode.getEnd() - tagNameNode.getStart(sourceFile),
        };
      }
    } else if (tsModule.isJsxSelfClosingElement(child)) {
      childName = getComponentName(child.tagName, tsModule);
      if (childName !== undefined) {
        const tagNameNode = child.tagName;
        tagSpan = {
          start: tagNameNode.getStart(sourceFile),
          length: tagNameNode.getEnd() - tagNameNode.getStart(sourceFile),
        };
      }
    }

    if (childName === undefined || tagSpan === undefined) continue;

    if (!allowed.includes(childName)) {
      diagnostics.push({
        category: tsModule.DiagnosticCategory.Warning,
        code: 90100,
        file: sourceFile,
        start: tagSpan.start,
        length: tagSpan.length,
        messageText: `'${childName}' is not a valid child of '${parentName}'. Expected: ${allowed.join(', ')}`,
        source: DIAGNOSTIC_SOURCE,
      });
    }
  }
}
