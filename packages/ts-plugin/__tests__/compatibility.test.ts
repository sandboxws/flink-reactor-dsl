/**
 * Compatibility tests for the architecture refactor.
 *
 * These tests verify that the refactored module structure preserves
 * the same externally observable behavior as the original monolithic
 * implementation. They cover the full integration path: index → service
 * → diagnostics → rules → context-detector.
 */
import ts from "typescript"
import { describe, expect, it } from "vitest"
import { filterCompletionsByContext } from "../src/completions"
import { createRulesRegistry } from "../src/component-rules"
import { getNestingDiagnostics } from "../src/diagnostics"

function parse(source: string): ts.SourceFile {
  return ts.createSourceFile(
    "test.tsx",
    source,
    ts.ScriptTarget.Latest,
    true,
    ts.ScriptKind.TSX,
  )
}

describe("compatibility: diagnostics behavior preserved", () => {
  const registry = createRulesRegistry()

  it("valid Pipeline children produce zero diagnostics", () => {
    const components = [
      "KafkaSource",
      "Filter",
      "Map",
      "KafkaSink",
      "Route",
      "Query",
      "SideOutput",
      "Validate",
      "View",
    ]
    for (const comp of components) {
      const sf = parse(`<Pipeline><${comp} /></Pipeline>`)
      const diags = getNestingDiagnostics(sf, registry, ts)
      expect(diags, `${comp} should be valid in Pipeline`).toHaveLength(0)
    }
  })

  it("Route only accepts Route.Branch and Route.Default", () => {
    const validChildren = ["Route.Branch", "Route.Default"]
    for (const child of validChildren) {
      const sf = parse(`<Route><${child}></${child}></Route>`)
      const diags = getNestingDiagnostics(sf, registry, ts)
      expect(diags, `${child} should be valid in Route`).toHaveLength(0)
    }

    const invalidChildren = ["Filter", "Map", "KafkaSink", "Pipeline"]
    for (const child of invalidChildren) {
      const sf = parse(`<Route><${child} /></Route>`)
      const diags = getNestingDiagnostics(sf, registry, ts)
      expect(diags, `${child} should be invalid in Route`).toHaveLength(1)
      expect(diags[0].code).toBe(90100)
      expect(diags[0].source).toBe("flink-reactor")
      expect(diags[0].category).toBe(ts.DiagnosticCategory.Warning)
    }
  })

  it("Query accepts clause children and sources", () => {
    const valid = [
      "Query.Select",
      "Query.Where",
      "Query.GroupBy",
      "KafkaSource",
    ]
    for (const child of valid) {
      const sf = parse(`<Query><${child}></${child}></Query>`)
      const diags = getNestingDiagnostics(sf, registry, ts)
      expect(diags, `${child} should be valid in Query`).toHaveLength(0)
    }

    const sf = parse("<Query><Filter /></Query>")
    const diags = getNestingDiagnostics(sf, registry, ts)
    expect(diags).toHaveLength(1)
  })

  it("wildcard parents accept any child", () => {
    const wildcardParents = [
      "Route.Branch",
      "Route.Default",
      "SideOutput.Sink",
      "Validate.Reject",
    ]
    for (const parent of wildcardParents) {
      const sf = parse(`<${parent}><Filter /></${parent}>`)
      const diags = getNestingDiagnostics(sf, registry, ts)
      expect(diags, `${parent} should accept any child`).toHaveLength(0)
    }
  })

  it("unrecognized parents produce no diagnostics", () => {
    const sf = parse("<CustomWrapper><Anything /></CustomWrapper>")
    const diags = getNestingDiagnostics(sf, registry, ts)
    expect(diags).toHaveLength(0)
  })

  it("diagnostic message format is stable", () => {
    const sf = parse("<Route><Filter /></Route>")
    const diags = getNestingDiagnostics(sf, registry, ts)
    expect(diags[0].messageText).toBe(
      "'Filter' is not a valid child of 'Route'. Expected: Route.Branch, Route.Default",
    )
  })

  it("diagnostic span targets the child tag name", () => {
    const source = "<Route><Filter /></Route>"
    const sf = parse(source)
    const diags = getNestingDiagnostics(sf, registry, ts)
    const spanText = source.slice(
      diags[0].start!,
      diags[0].start! + diags[0].length!,
    )
    expect(spanText).toBe("Filter")
  })
})

describe("compatibility: registry contract preserved", () => {
  it("createRulesRegistry returns all expected interface methods", () => {
    const registry = createRulesRegistry()
    expect(typeof registry.getAllowedChildren).toBe("function")
    expect(typeof registry.isValidChild).toBe("function")
    expect(typeof registry.getRegisteredParents).toBe("function")
    expect(typeof registry.getAllReferencedComponents).toBe("function")
  })

  it("user rules merge correctly with built-in rules", () => {
    const custom = createRulesRegistry({
      MyContainer: ["Filter", "Map"],
    })
    expect(custom.getAllowedChildren("MyContainer")).toEqual(["Filter", "Map"])
    expect(custom.getAllowedChildren("Pipeline")).toContain("KafkaSource")
  })
})

describe("compatibility: completions filter", () => {
  const registry = createRulesRegistry()

  it("filters entries by parent context", () => {
    const sf = parse("<Route></Route>")
    const jsxEl = (
      (sf.statements[0] as ts.ExpressionStatement).expression as ts.JsxElement
    ).openingElement
    const entries: ts.CompletionEntry[] = [
      {
        name: "Route.Branch",
        kind: ts.ScriptElementKind.unknown,
        sortText: "0",
        kindModifiers: "",
      },
      {
        name: "Filter",
        kind: ts.ScriptElementKind.unknown,
        sortText: "1",
        kindModifiers: "",
      },
    ]
    const filtered = filterCompletionsByContext(
      entries,
      jsxEl.tagName,
      registry,
      ts,
    )
    expect(filtered).toHaveLength(1)
    expect(filtered[0].name).toBe("Route.Branch")
  })

  it("returns all entries when no parent context", () => {
    const entries: ts.CompletionEntry[] = [
      {
        name: "Filter",
        kind: ts.ScriptElementKind.unknown,
        sortText: "0",
        kindModifiers: "",
      },
    ]
    const filtered = filterCompletionsByContext(
      entries,
      undefined,
      registry,
      ts,
    )
    expect(filtered).toHaveLength(1)
  })
})
