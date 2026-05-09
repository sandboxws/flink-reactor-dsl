import { resolve } from "node:path"
import { defineConfig } from "vitest/config"

export default defineConfig({
  resolve: {
    alias: {
      "@": resolve(__dirname, "src"),
      // The library declares itself as the jsxImportSource, so esbuild
      // (vitest's transpiler) emits `import { jsx } from "@flink-reactor/dsl/jsx-runtime"`
      // for every JSX expression. In dev/tests we resolve those imports
      // back to the in-tree source files. Production consumers resolve
      // the same path via package.json#exports.
      "@flink-reactor/dsl/jsx-runtime": resolve(
        __dirname,
        "src/jsx-runtime.ts",
      ),
      "@flink-reactor/dsl/jsx-dev-runtime": resolve(
        __dirname,
        "src/jsx-dev-runtime.ts",
      ),
    },
  },
  test: {
    globals: true,
    environment: "node",
    include: ["src/**/*.test.ts"],
    typecheck: {
      enabled: true,
      include: ["src/**/*.test-d.{ts,tsx}"],
    },
    coverage: {
      provider: "v8",
      include: ["src/**/*.ts"],
      exclude: ["src/**/*.test.ts", "src/**/*.d.ts"],
      // Modest, achievable thresholds. Raise once Step 17's CLI smoke
      // tests land. The synthesis path (core, codegen) is well-tested;
      // the CLI is the gap.
      thresholds: {
        "src/core/**": {
          lines: 75,
          functions: 75,
          branches: 70,
          statements: 75,
        },
        "src/codegen/**": {
          lines: 70,
          functions: 70,
          branches: 65,
          statements: 70,
        },
        // sql-generator.ts is a 4.5k-line synthesis hub queued for
        // decomposition. Its tests are the regression net for that work, so
        // we lock in the achieved baseline as a per-file floor: any
        // extraction commit that drops below these numbers is a red flag.
        // The thresholds tick down slightly with each extraction as covered
        // helpers move to dedicated files (which keep their own coverage).
        "src/codegen/sql-generator.ts": {
          lines: 94,
          functions: 100,
          branches: 87,
          statements: 94,
        },
      },
    },
  },
})
