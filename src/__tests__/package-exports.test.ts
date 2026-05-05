// Regression test for the `package.json#exports` contract.
//
// Background: scaffolded `flink-reactor.config.ts` files import from
// subpaths like `@flink-reactor/dsl/plugins`. Node's ESM resolver
// refuses to resolve any subpath that isn't explicitly listed in the
// package's `exports` map — even if `dist/<subpath>.js` exists. When
// we forgot to list `./plugins`, every config rendered with the
// Grafana opt-in failed to load, which the CLI's catch-all swallowed
// silently → only always-on services started in `cluster up`.
//
// This test pins down the contract: any subpath the templates emit
// must be both (a) listed in `package.json#exports` and (b) wired up
// as a tsup build entry. Either half missing reproduces the bug.

import { readFileSync } from "node:fs"
import { resolve } from "node:path"
import { describe, expect, it } from "vitest"

const repoRoot = resolve(__dirname, "..", "..")
const pkg = JSON.parse(readFileSync(resolve(repoRoot, "package.json"), "utf-8"))
const tsupConfig = readFileSync(resolve(repoRoot, "tsup.config.ts"), "utf-8")

// Subpaths that scaffolded user configs are allowed to import. Add to
// this list whenever a template starts emitting a new subpath import.
const REQUIRED_SUBPATH_EXPORTS = [
  ".", // `@flink-reactor/dsl`
  "./jsx-runtime",
  "./jsx-dev-runtime",
  "./plugins", // `metricsPlugin` etc. — wired by `--grafana` opt-in
] as const

describe("package.json#exports contract", () => {
  for (const subpath of REQUIRED_SUBPATH_EXPORTS) {
    it(`exposes "${subpath}" with both import + types entries`, () => {
      const entry = pkg.exports?.[subpath]
      expect(entry, `missing exports["${subpath}"]`).toBeDefined()
      expect(entry.import, `missing import condition`).toMatch(
        /^\.\/dist\/.+\.js$/,
      )
      expect(entry.types, `missing types condition`).toMatch(
        /^\.\/dist\/.+\.d\.ts$/,
      )
    })
  }

  it("plugins subpath is wired as a tsup entry — without this `dist/plugins.js` is never built", () => {
    // We don't try to import the real tsup config (it's TS, would need
    // build infra). Pattern-matching the source is sufficient: if
    // someone removes the entry, this test fails before `dist/` is
    // even built. The regex tolerates whitespace/quoting variants.
    expect(tsupConfig).toMatch(/plugins:\s*["']src\/plugins\/index\.ts["']/)
  })
})
