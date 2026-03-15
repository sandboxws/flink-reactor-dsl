import { existsSync, readFileSync } from "node:fs"
import { resolve } from "node:path"
import { describe, expect, it } from "vitest"

const distDir = resolve(import.meta.dirname, "../../../dist")
const browserBundle = resolve(distDir, "browser.js")

describe("browser bundle smoke tests", () => {
  it("dist/browser.js exists after build", () => {
    expect(
      existsSync(browserBundle),
      "dist/browser.js must exist — run `pnpm build` first",
    ).toBe(true)
  })

  it("contains no require() calls", () => {
    const content = readFileSync(browserBundle, "utf-8")
    const matches = content.match(/\brequire\s*\(/g)
    expect(
      matches,
      "browser bundle must not contain require() calls",
    ).toBeNull()
  })

  it("contains no node: built-in references", () => {
    const content = readFileSync(browserBundle, "utf-8")
    const matches = content.match(/["']node:/g)
    expect(
      matches,
      "browser bundle must not reference node: built-ins",
    ).toBeNull()
  })
})
