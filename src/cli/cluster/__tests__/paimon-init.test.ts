import { describe, expect, it } from "vitest"
import { paimonInitStatements } from "@/cli/cluster/paimon-init.js"

describe("paimonInitStatements", () => {
  it("returns empty array when no databases given", () => {
    expect(paimonInitStatements([])).toEqual([])
  })

  it("emits CREATE CATALOG + USE CATALOG + CREATE DATABASE for a single db", () => {
    const stmts = paimonInitStatements(["benchmark"])
    expect(stmts).toMatchSnapshot()
  })

  it("emits one CREATE DATABASE per name in input order", () => {
    const stmts = paimonInitStatements(["orders", "analytics", "lineage"])
    expect(stmts).toMatchSnapshot()
  })

  it("uses the default s3://flink-state/paimon warehouse when omitted", () => {
    // The default uses `s3://` rather than `s3a://` so Paimon routes
    // through the bundled paimon-s3 plugin rather than Hadoop's
    // S3AFileSystem (which isn't on the user classpath).
    const stmts = paimonInitStatements(["benchmark"])
    expect(stmts[0]).toContain("'warehouse' = 's3://flink-state/paimon'")
  })

  it("threads a custom warehouse override", () => {
    const stmts = paimonInitStatements(["benchmark"], "s3://my-bucket/lake")
    expect(stmts[0]).toContain("'warehouse' = 's3://my-bucket/lake'")
  })

  it("includes SeaweedFS-flavored S3 client overrides in the catalog DDL", () => {
    const stmts = paimonInitStatements(["benchmark"])
    expect(stmts[0]).toContain(
      "'s3.endpoint' = 'http://seaweedfs.flink-demo.svc:8333'",
    )
    expect(stmts[0]).toContain("'s3.path-style-access' = 'true'")
    expect(stmts[0]).toContain("'s3.access-key' = 'admin'")
    expect(stmts[0]).toContain("'s3.secret-key' = 'admin'")
  })

  it("backtick-quotes database identifiers", () => {
    const stmts = paimonInitStatements(["with-dash"])
    expect(stmts[stmts.length - 1]).toBe(
      "CREATE DATABASE IF NOT EXISTS `with-dash`",
    )
  })
})
