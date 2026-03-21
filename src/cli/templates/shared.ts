import type { ScaffoldOptions, TemplateFile } from "@/cli/commands/new.js"

// Injected at build time by tsup/esbuild — falls back for tsx/vitest
declare const __DSL_VERSION__: string
export const DSL_VERSION: string =
  typeof __DSL_VERSION__ !== "undefined" ? __DSL_VERSION__ : "0.1.0"

export function makePackageJson(
  opts: ScaffoldOptions,
  extra?: Record<string, unknown>,
): string {
  const scripts: Record<string, string> = {
    dev: "flink-reactor dev",
    synth: "flink-reactor synth",
    validate: "flink-reactor validate",
    test: "vitest run",
    "test:watch": "vitest",
  }

  const dependencies: Record<string, string> = {
    "@flink-reactor/dsl": `^${DSL_VERSION}`,
  }

  const pkg: Record<string, unknown> = {
    name: opts.projectName,
    version: "0.1.0",
    private: true,
    type: "module",
    scripts,
    dependencies,
    devDependencies: {
      typescript: "^5.7.0",
      vitest: "^3.0.0",
    },
    ...extra,
  }
  return `${JSON.stringify(pkg, null, 2)}\n`
}

export function makeTsconfig(_opts: ScaffoldOptions): string {
  const config = {
    compilerOptions: {
      target: "ES2022",
      module: "ESNext",
      moduleResolution: "bundler",
      lib: ["ES2022"],
      strict: true,
      esModuleInterop: true,
      skipLibCheck: true,
      forceConsistentCasingInFileNames: true,
      resolveJsonModule: true,
      jsx: "react-jsx",
      jsxImportSource: "@flink-reactor/dsl",
      baseUrl: ".",
      paths: {
        "@/*": ["./*"],
      },
    },
    include: ["pipelines/**/*", "schemas/**/*", "env/**/*", "patterns/**/*"],
  }
  return `${JSON.stringify(config, null, 2)}\n`
}

export function makeConfig(opts: ScaffoldOptions): string {
  return `import { defineConfig } from '@flink-reactor/dsl'

export default defineConfig({
  flink: { version: '${opts.flinkVersion}' },

  environments: {
    development: {
      cluster: { url: 'http://localhost:8081' },
      dashboard: { mockMode: true },
      pipelines: { '*': { parallelism: 1 } },
    },
    production: {
      cluster: { url: 'https://flink-prod:8081' },
      kubernetes: { namespace: 'flink-prod' },
      pipelines: { '*': { parallelism: 4 } },
    },
  },
})
`
}

export function makeGitignore(): string {
  return `node_modules/
dist/
.flink-reactor/
*.tsbuildinfo
.env
.env.local
`
}

export function makeDevEnv(_opts: ScaffoldOptions): string {
  return `import { defineEnvironment } from '@flink-reactor/dsl'

export default defineEnvironment({
  name: 'dev',
  // Override pipeline defaults for local development
})
`
}

export function makeNpmrc(registry: string): string {
  return `registry=${registry}\n`
}

export function sharedFiles(opts: ScaffoldOptions): TemplateFile[] {
  const files: TemplateFile[] = [
    {
      path: "package.json",
      content: makePackageJson(opts),
    },
    { path: "tsconfig.json", content: makeTsconfig(opts) },
    { path: "flink-reactor.config.ts", content: makeConfig(opts) },
    { path: ".gitignore", content: makeGitignore() },
  ]
  if (opts.registry) {
    files.push({ path: ".npmrc", content: makeNpmrc(opts.registry) })
  }
  return files
}
