import { readFileSync } from "node:fs"
import { resolve } from "node:path"
import { defineConfig } from "tsup"

const srcAlias = { "@": resolve("src") }
const pkg = JSON.parse(readFileSync("package.json", "utf-8"))
const dslVersion = pkg.version as string

export default defineConfig([
  // Library entry — importable as `import { ... } from '@flink-reactor/dsl'`
  {
    entry: { index: "src/index.ts", "jsx-runtime": "src/jsx-runtime.ts" },
    format: ["esm"],
    target: "node18",
    outDir: "dist",
    clean: true,
    dts: true,
    sourcemap: true,
    splitting: false,
    esbuildOptions(options) {
      options.alias = srcAlias
    },
  },
  // CLI entry — `flink-reactor` bin
  {
    entry: { cli: "src/cli/index.ts" },
    format: ["esm"],
    target: "node18",
    outDir: "dist",
    clean: false,
    dts: false,
    sourcemap: true,
    splitting: false,
    shims: true,
    banner: {
      js: "#!/usr/bin/env node",
    },
    esbuildOptions(options) {
      options.alias = srcAlias
      options.define = {
        ...options.define,
        __DSL_VERSION__: JSON.stringify(dslVersion),
      }
    },
  },
  // Browser entry — importable as `import { ... } from '@flink-reactor/dsl/browser'`
  {
    entry: { browser: "src/browser.ts" },
    format: ["esm"],
    target: "esnext",
    platform: "browser",
    outDir: "dist",
    clean: false,
    dts: true,
    sourcemap: true,
    splitting: false,
    minify: true,
    noExternal: ["effect", "dt-sql-parser"],
    esbuildOptions(options) {
      options.alias = srcAlias
    },
  },
])
