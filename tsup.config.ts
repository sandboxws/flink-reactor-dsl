import { defineConfig } from 'tsup';

export default defineConfig([
  // Library entry — importable as `import { ... } from 'flink-reactor'`
  {
    entry: { index: 'src/index.ts', 'jsx-runtime': 'src/jsx-runtime.ts' },
    format: ['esm'],
    target: 'node18',
    outDir: 'dist',
    clean: true,
    dts: true,
    sourcemap: true,
    splitting: false,
  },
  // CLI entry — `flink-reactor` bin
  {
    entry: { cli: 'src/cli/index.ts' },
    format: ['esm'],
    target: 'node18',
    outDir: 'dist',
    clean: false,
    dts: false,
    sourcemap: true,
    splitting: false,
    shims: true,
    banner: {
      js: '#!/usr/bin/env node',
    },
  },
]);
