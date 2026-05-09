import { describe, expect, it } from "vitest"
import {
  quoteIdentifier,
  quoteQualifiedName,
  quoteStringLiteral,
} from "@/codegen/sql/sql-identifiers.js"

describe("quoteIdentifier", () => {
  it("wraps simple names in backticks", () => {
    expect(quoteIdentifier("orders")).toBe("`orders`")
  })

  it("preserves names containing dots, dashes, and spaces", () => {
    // The whole input goes between the backticks — Flink treats the
    // backticked literal as one identifier, dots and all.
    expect(quoteIdentifier("my.table")).toBe("`my.table`")
    expect(quoteIdentifier("kebab-name")).toBe("`kebab-name`")
    expect(quoteIdentifier("with space")).toBe("`with space`")
  })

  it("escapes embedded backticks by doubling them", () => {
    // Without escape: `tick`evil` would parse as `tick` + evil + `
    // and break the surrounding statement.
    expect(quoteIdentifier("tick`evil")).toBe("`tick``evil`")
    // Two embedded backticks → each doubles → four; plus the outer pair = six.
    expect(quoteIdentifier("``")).toBe("``````")
  })

  it("handles empty strings (caller's responsibility to validate)", () => {
    expect(quoteIdentifier("")).toBe("``")
  })
})

describe("quoteQualifiedName", () => {
  it("joins quoted parts with dots", () => {
    expect(quoteQualifiedName("cat", "db", "tbl")).toBe("`cat`.`db`.`tbl`")
  })

  it("works with a single part", () => {
    expect(quoteQualifiedName("only")).toBe("`only`")
  })

  it("escapes backticks inside any part", () => {
    expect(quoteQualifiedName("a", "b`c")).toBe("`a`.`b``c`")
  })
})

describe("quoteStringLiteral", () => {
  it("wraps in single quotes", () => {
    expect(quoteStringLiteral("hello")).toBe("'hello'")
  })

  it("doubles embedded single quotes", () => {
    expect(quoteStringLiteral("it's")).toBe("'it''s'")
  })

  it("preserves backticks inside string literals (only single quotes are special)", () => {
    expect(quoteStringLiteral("with `tick`")).toBe("'with `tick`'")
  })
})
