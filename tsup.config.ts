import { resolve } from "node:path"
import { defineConfig } from "tsup"

const srcAlias = { "@": resolve("src") }

export default defineConfig([
  // Library entry — importable as `import { ... } from 'flink-reactor'`
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
    },
  },
  // Browser entry — importable as `import { ... } from 'flink-reactor/browser'`
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
    noExternal: ["effect", "dt-sql-parser"],
    esbuildOptions(options) {
      options.alias = srcAlias
    },
  },
])
