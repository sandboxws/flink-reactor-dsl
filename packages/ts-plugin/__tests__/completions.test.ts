/**
 * Unit tests for context-aware completion behavior.
 *
 * Tests both filter and rank strategies across all key DSL parent contexts:
 * Pipeline, Route, Query, SideOutput, and Validate.
 */
import ts from "typescript"
import { describe, expect, it } from "vitest"
import {
  filterCompletionsByContext,
  rankCompletionsByContext,
} from "../src/completions"
import { createRulesRegistry } from "../src/component-rules"
import {
  getComponentName,
  getParentTagAtPosition,
} from "../src/context-detector"

function parse(source: string): ts.SourceFile {
  return ts.createSourceFile(
    "test.tsx",
    source,
    ts.ScriptTarget.Latest,
    true,
    ts.ScriptKind.TSX,
  )
}

/** Create a minimal CompletionEntry for testing */
function entry(name: string, sortText = "11"): ts.CompletionEntry {
  return {
    name,
    kind: ts.ScriptElementKind.unknown,
    sortText,
    kindModifiers: "",
  }
}

/**
 * Find parent tag at a marker position in source.
 * Use `|` to mark cursor position — it's removed before parsing.
 */
function parentTagAt(
  markedSource: string,
): ts.JsxTagNameExpression | undefined {
  const pos = markedSource.indexOf("|")
  const source = markedSource.replace("|", "")
  const sf = parse(source)
  return getParentTagAtPosition(sf, pos, ts)
}

function parentNameAt(markedSource: string): string | undefined {
  const tag = parentTagAt(markedSource)
  if (!tag) return undefined
  return getComponentName(tag, ts)
}

const registry = createRulesRegistry()

// -- Context detection tests --------------------------------------------------

describe("getParentTagAtPosition", () => {
  it("detects Pipeline as parent when cursor is between tags", () => {
    expect(parentNameAt("<Pipeline>|</Pipeline>")).toBe("Pipeline")
  })

  it("detects Route as parent", () => {
    expect(parentNameAt("<Route>|</Route>")).toBe("Route")
  })

  it("detects Query as parent", () => {
    expect(parentNameAt("<Query>|</Query>")).toBe("Query")
  })

  it("detects SideOutput as parent", () => {
    expect(parentNameAt("<SideOutput>|</SideOutput>")).toBe("SideOutput")
  })

  it("detects Validate as parent", () => {
    expect(parentNameAt("<Validate>|</Validate>")).toBe("Validate")
  })

  it("detects dotted parent (Route.Branch)", () => {
    expect(parentNameAt("<Route.Branch>|</Route.Branch>")).toBe("Route.Branch")
  })

  it("detects nested parent correctly", () => {
    expect(parentNameAt("<Pipeline><Route>|</Route></Pipeline>")).toBe("Route")
  })

  it("returns undefined outside any JSX element", () => {
    expect(parentNameAt("|const x = 1")).toBeUndefined()
  })

  it("returns undefined when cursor is inside a tag (not children)", () => {
    // Cursor inside the opening tag itself — not between open/close
    expect(parentNameAt("<Pipeline| ></Pipeline>")).toBeUndefined()
  })
})

// -- Filter strategy tests ----------------------------------------------------

describe("filterCompletionsByContext", () => {
  const dslEntries = [
    entry("KafkaSource"),
    entry("Filter"),
    entry("Route.Branch"),
    entry("Route.Default"),
    entry("Query.Select"),
    entry("Map"),
  ]
  const mixedEntries = [...dslEntries, entry("myVariable"), entry("console")]

  it("Pipeline: keeps all valid Pipeline children", () => {
    const sf = parse("<Pipeline></Pipeline>")
    const tag = (
      (sf.statements[0] as ts.ExpressionStatement).expression as ts.JsxElement
    ).openingElement.tagName
    const result = filterCompletionsByContext(mixedEntries, tag, registry, ts)

    // KafkaSource, Filter, Map are valid Pipeline children
    expect(result.map((e) => e.name)).toContain("KafkaSource")
    expect(result.map((e) => e.name)).toContain("Filter")
    expect(result.map((e) => e.name)).toContain("Map")
    // Route.Branch is NOT a valid Pipeline child
    expect(result.map((e) => e.name)).not.toContain("Route.Branch")
    // Non-DSL entries preserved
    expect(result.map((e) => e.name)).toContain("myVariable")
    expect(result.map((e) => e.name)).toContain("console")
  })

  it("Route: keeps only Route.Branch and Route.Default (plus non-DSL)", () => {
    const sf = parse("<Route></Route>")
    const tag = (
      (sf.statements[0] as ts.ExpressionStatement).expression as ts.JsxElement
    ).openingElement.tagName
    const result = filterCompletionsByContext(mixedEntries, tag, registry, ts)

    expect(result.map((e) => e.name)).toContain("Route.Branch")
    expect(result.map((e) => e.name)).toContain("Route.Default")
    expect(result.map((e) => e.name)).not.toContain("KafkaSource")
    expect(result.map((e) => e.name)).not.toContain("Filter")
    // Non-DSL preserved
    expect(result.map((e) => e.name)).toContain("myVariable")
  })

  it("Query: keeps query clauses and sources", () => {
    const sf = parse("<Query></Query>")
    const tag = (
      (sf.statements[0] as ts.ExpressionStatement).expression as ts.JsxElement
    ).openingElement.tagName
    const result = filterCompletionsByContext(mixedEntries, tag, registry, ts)

    expect(result.map((e) => e.name)).toContain("Query.Select")
    expect(result.map((e) => e.name)).toContain("KafkaSource")
    expect(result.map((e) => e.name)).not.toContain("Route.Branch")
    expect(result.map((e) => e.name)).not.toContain("Route.Default")
  })

  it("SideOutput: keeps SideOutput children and allowed transforms", () => {
    const sf = parse("<SideOutput></SideOutput>")
    const tag = (
      (sf.statements[0] as ts.ExpressionStatement).expression as ts.JsxElement
    ).openingElement.tagName
    const entries = [
      entry("SideOutput.Sink"),
      entry("Filter"),
      entry("Map"),
      entry("FlatMap"),
      entry("KafkaSource"),
      entry("Route.Branch"),
      entry("Aggregate"),
    ]
    const result = filterCompletionsByContext(entries, tag, registry, ts)

    expect(result.map((e) => e.name)).toContain("SideOutput.Sink")
    expect(result.map((e) => e.name)).toContain("Filter")
    expect(result.map((e) => e.name)).toContain("KafkaSource")
    expect(result.map((e) => e.name)).not.toContain("Route.Branch")
    expect(result.map((e) => e.name)).not.toContain("Aggregate")
  })

  it("Validate: keeps Validate children and allowed transforms", () => {
    const sf = parse("<Validate></Validate>")
    const tag = (
      (sf.statements[0] as ts.ExpressionStatement).expression as ts.JsxElement
    ).openingElement.tagName
    const entries = [
      entry("Validate.Reject"),
      entry("Filter"),
      entry("Map"),
      entry("FlatMap"),
      entry("KafkaSource"),
      entry("Route.Branch"),
      entry("Aggregate"),
    ]
    const result = filterCompletionsByContext(entries, tag, registry, ts)

    expect(result.map((e) => e.name)).toContain("Validate.Reject")
    expect(result.map((e) => e.name)).toContain("Filter")
    expect(result.map((e) => e.name)).toContain("KafkaSource")
    expect(result.map((e) => e.name)).not.toContain("Route.Branch")
    expect(result.map((e) => e.name)).not.toContain("Aggregate")
  })

  it("passes through all entries when no parent context", () => {
    const result = filterCompletionsByContext(
      mixedEntries,
      undefined,
      registry,
      ts,
    )
    expect(result).toHaveLength(mixedEntries.length)
  })

  it("passes through all entries for wildcard parents", () => {
    const sf = parse("<Route.Branch></Route.Branch>")
    const tag = (
      (sf.statements[0] as ts.ExpressionStatement).expression as ts.JsxElement
    ).openingElement.tagName
    const result = filterCompletionsByContext(mixedEntries, tag, registry, ts)
    expect(result).toHaveLength(mixedEntries.length)
  })

  it("passes through all entries for unknown parents", () => {
    const sf = parse("<CustomComponent></CustomComponent>")
    const tag = (
      (sf.statements[0] as ts.ExpressionStatement).expression as ts.JsxElement
    ).openingElement.tagName
    const result = filterCompletionsByContext(mixedEntries, tag, registry, ts)
    expect(result).toHaveLength(mixedEntries.length)
  })
})

// -- Rank strategy tests ------------------------------------------------------

describe("rankCompletionsByContext", () => {
  it("promotes valid DSL children and demotes invalid ones", () => {
    const sf = parse("<Route></Route>")
    const tag = (
      (sf.statements[0] as ts.ExpressionStatement).expression as ts.JsxElement
    ).openingElement.tagName
    const entries = [
      entry("Route.Branch", "11"),
      entry("Filter", "11"),
      entry("myVariable", "11"),
    ]
    const result = rankCompletionsByContext(entries, tag, registry, ts)

    // Route.Branch is valid → promoted
    expect(result.find((e) => e.name === "Route.Branch")?.sortText).toBe("011")
    // Filter is DSL but invalid → demoted
    expect(result.find((e) => e.name === "Filter")?.sortText).toBe("211")
    // myVariable is not DSL → unchanged
    expect(result.find((e) => e.name === "myVariable")?.sortText).toBe("11")
  })

  it("preserves all entries (never removes)", () => {
    const sf = parse("<Route></Route>")
    const tag = (
      (sf.statements[0] as ts.ExpressionStatement).expression as ts.JsxElement
    ).openingElement.tagName
    const entries = [
      entry("Route.Branch"),
      entry("Filter"),
      entry("Map"),
      entry("myVar"),
    ]
    const result = rankCompletionsByContext(entries, tag, registry, ts)
    expect(result).toHaveLength(entries.length)
  })

  it("passes through all entries unchanged for wildcard parents", () => {
    const sf = parse("<Route.Branch></Route.Branch>")
    const tag = (
      (sf.statements[0] as ts.ExpressionStatement).expression as ts.JsxElement
    ).openingElement.tagName
    const entries = [entry("Filter", "11"), entry("Map", "11")]
    const result = rankCompletionsByContext(entries, tag, registry, ts)
    expect(result[0].sortText).toBe("11")
    expect(result[1].sortText).toBe("11")
  })

  it("passes through all entries unchanged for unknown parents", () => {
    const sf = parse("<CustomComponent></CustomComponent>")
    const tag = (
      (sf.statements[0] as ts.ExpressionStatement).expression as ts.JsxElement
    ).openingElement.tagName
    const entries = [entry("Filter", "11")]
    const result = rankCompletionsByContext(entries, tag, registry, ts)
    expect(result[0].sortText).toBe("11")
  })

  it("Pipeline: promotes Pipeline children, demotes non-children", () => {
    const sf = parse("<Pipeline></Pipeline>")
    const tag = (
      (sf.statements[0] as ts.ExpressionStatement).expression as ts.JsxElement
    ).openingElement.tagName
    const entries = [
      entry("KafkaSource", "11"),
      entry("Route.Branch", "11"),
      entry("console", "11"),
    ]
    const result = rankCompletionsByContext(entries, tag, registry, ts)

    expect(result.find((e) => e.name === "KafkaSource")?.sortText).toBe("011")
    expect(result.find((e) => e.name === "Route.Branch")?.sortText).toBe("211")
    expect(result.find((e) => e.name === "console")?.sortText).toBe("11")
  })
})

// -- Regression: fallback and non-DSL preservation ----------------------------

describe("regression: incomplete TSX fallback", () => {
  it("returns undefined parent for incomplete opening tag", () => {
    // Simulates typing `<` with nothing else — TS parser creates a broken node
    expect(parentNameAt("<|")).toBeUndefined()
  })

  it("detects parent even for unclosed JSX element (TS error recovery)", () => {
    // TypeScript's parser error-recovers unclosed JSX into a valid JsxElement,
    // so context detection still works — this is desired behavior.
    expect(parentNameAt("<Pipeline>|")).toBe("Pipeline")
  })

  it("returns undefined parent for self-closing element (no children region)", () => {
    expect(parentNameAt("<Filter| />")).toBeUndefined()
  })

  it("filter passes through all entries on incomplete JSX", () => {
    const entries = [entry("KafkaSource"), entry("myVar")]
    // No parent context → full passthrough
    const result = filterCompletionsByContext(entries, undefined, registry, ts)
    expect(result).toHaveLength(2)
    expect(result.map((e) => e.name)).toEqual(["KafkaSource", "myVar"])
  })

  it("rank passes through all entries unchanged on incomplete JSX", () => {
    const entries = [entry("KafkaSource", "11"), entry("myVar", "11")]
    const result = rankCompletionsByContext(entries, undefined, registry, ts)
    expect(result).toHaveLength(2)
    expect(result[0].sortText).toBe("11")
    expect(result[1].sortText).toBe("11")
  })
})

describe("regression: non-DSL completion preservation", () => {
  it("filter never removes non-DSL entries regardless of parent", () => {
    const nonDSLEntries = [
      entry("useState"),
      entry("console"),
      entry("myCustomHook"),
      entry("React"),
      entry("Object"),
    ]
    // Test with a restrictive parent (Route only allows Branch/Default)
    const sf = parse("<Route></Route>")
    const tag = (
      (sf.statements[0] as ts.ExpressionStatement).expression as ts.JsxElement
    ).openingElement.tagName

    const result = filterCompletionsByContext(nonDSLEntries, tag, registry, ts)
    expect(result).toHaveLength(nonDSLEntries.length)
    expect(result.map((e) => e.name)).toEqual(nonDSLEntries.map((e) => e.name))
  })

  it("rank never modifies sortText of non-DSL entries", () => {
    const nonDSLEntries = [
      entry("useState", "05"),
      entry("console", "10"),
      entry("myVar", "15"),
    ]
    const sf = parse("<Route></Route>")
    const tag = (
      (sf.statements[0] as ts.ExpressionStatement).expression as ts.JsxElement
    ).openingElement.tagName

    const result = rankCompletionsByContext(nonDSLEntries, tag, registry, ts)
    expect(result[0].sortText).toBe("05")
    expect(result[1].sortText).toBe("10")
    expect(result[2].sortText).toBe("15")
  })

  it("filter preserves mixed DSL and non-DSL entries correctly", () => {
    const sf = parse("<Query></Query>")
    const tag = (
      (sf.statements[0] as ts.ExpressionStatement).expression as ts.JsxElement
    ).openingElement.tagName
    const entries = [
      entry("Query.Select"), // valid DSL child → keep
      entry("Filter"), // invalid DSL child → remove
      entry("myQueryHelper"), // non-DSL → keep
      entry("KafkaSource"), // valid DSL child → keep
    ]
    const result = filterCompletionsByContext(entries, tag, registry, ts)

    expect(result.map((e) => e.name)).toEqual([
      "Query.Select",
      "myQueryHelper",
      "KafkaSource",
    ])
  })
})
