import ts from "typescript"
import { describe, expect, it } from "vitest"
import { getComponentName } from "../src/context-detector"

/** Parse a TSX string and return the source file */
function parse(source: string): ts.SourceFile {
  return ts.createSourceFile(
    "test.tsx",
    source,
    ts.ScriptTarget.Latest,
    /* setParentNodes */ true,
    ts.ScriptKind.TSX,
  )
}

describe("getComponentName", () => {
  it("extracts simple identifier", () => {
    const sf = parse("<Pipeline />")
    const jsx = sf.statements[0] as ts.ExpressionStatement
    const element = jsx.expression as ts.JsxSelfClosingElement
    expect(getComponentName(element.tagName, ts)).toBe("Pipeline")
  })

  it("extracts dot-notation name", () => {
    const sf = parse("<Route.Branch />")
    const jsx = sf.statements[0] as ts.ExpressionStatement
    const element = jsx.expression as ts.JsxSelfClosingElement
    expect(getComponentName(element.tagName, ts)).toBe("Route.Branch")
  })
})
