/**
 * Release quality checks.
 *
 * Verifies that package metadata and documentation claims
 * match the actually implemented plugin capabilities.
 */
import { readFileSync, existsSync } from "node:fs"
import { resolve } from "node:path"
import { describe, expect, it } from "vitest"

const PKG_ROOT = resolve(__dirname, "..")
const pkg = JSON.parse(
  readFileSync(resolve(PKG_ROOT, "package.json"), "utf-8"),
)

describe("package metadata", () => {
  it("has a name", () => {
    expect(pkg.name).toBe("@flink-reactor/ts-plugin")
  })

  it("has a non-empty description", () => {
    expect(typeof pkg.description).toBe("string")
    expect(pkg.description.length).toBeGreaterThan(0)
  })

  it("description mentions diagnostics", () => {
    const desc = pkg.description.toLowerCase()
    expect(
      desc.includes("diagnostic") || desc.includes("validation") || desc.includes("nesting"),
      `Description "${pkg.description}" should reference diagnostics capability`,
    ).toBe(true)
  })

  it("description mentions completions", () => {
    const desc = pkg.description.toLowerCase()
    expect(
      desc.includes("completion"),
      `Description "${pkg.description}" should reference completions capability`,
    ).toBe(true)
  })

  it("has a valid main entry point", () => {
    expect(pkg.main).toBe("./dist/index.js")
  })

  it("has type declarations", () => {
    expect(pkg.types).toBe("./dist/index.d.ts")
  })

  it("has a build script", () => {
    expect(pkg.scripts.build).toBeDefined()
  })

  it("has a test script", () => {
    expect(pkg.scripts.test).toBeDefined()
  })

  it("requires TypeScript as a peer dependency", () => {
    expect(pkg.peerDependencies?.typescript).toBeDefined()
  })

  it("files array only includes dist", () => {
    expect(pkg.files).toEqual(["dist"])
  })
})

describe("source file structure", () => {
  const expectedModules = [
    "index.ts",
    "component-rules.ts",
    "completions.ts",
    "context-detector.ts",
    "diagnostics.ts",
    "diagnostic-codes.ts",
    "service.ts",
    "types.ts",
    "component-inventory.ts",
    "parity-check.ts",
    "host-compatibility.ts",
  ]

  for (const mod of expectedModules) {
    it(`src/${mod} exists`, () => {
      const filePath = resolve(PKG_ROOT, "src", mod)
      expect(
        existsSync(filePath),
        `Expected source module src/${mod} to exist`,
      ).toBe(true)
    })
  }
})
