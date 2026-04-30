import { describe, expect, it } from "vitest"
import { flussInitStatements } from "@/cli/cluster/fluss-init.js"

describe("flussInitStatements", () => {
  it("returns empty array when no databases given", () => {
    expect(flussInitStatements([])).toEqual([])
  })

  it("emits CREATE CATALOG + USE CATALOG + CREATE DATABASE for a single db", () => {
    const stmts = flussInitStatements(["benchmark"])
    expect(stmts).toMatchSnapshot()
  })

  it("emits one CREATE DATABASE per name in input order", () => {
    const stmts = flussInitStatements(["orders", "analytics", "lineage"])
    expect(stmts).toMatchSnapshot()
  })

  it("uses the default fluss-coordinator:9123 bootstrap when omitted", () => {
    const stmts = flussInitStatements(["benchmark"])
    expect(stmts[0]).toContain("'bootstrap.servers' = 'fluss-coordinator:9123'")
  })

  it("threads a custom bootstrap.servers override", () => {
    const stmts = flussInitStatements(["benchmark"], "fluss-coord.prod:9123")
    expect(stmts[0]).toContain("'bootstrap.servers' = 'fluss-coord.prod:9123'")
  })

  it("backtick-quotes database identifiers", () => {
    const stmts = flussInitStatements(["with-dash"])
    expect(stmts[stmts.length - 1]).toBe(
      "CREATE DATABASE IF NOT EXISTS `with-dash`",
    )
  })
})
