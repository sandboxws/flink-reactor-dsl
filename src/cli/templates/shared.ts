import type { ScaffoldOptions, TemplateFile } from "@/cli/commands/new.js"

export interface SharedFileOptions {
  /** Include @flink-reactor/dashboard dependency and scripts. Default: true */
  readonly dashboard?: boolean
}

export function makePackageJson(
  opts: ScaffoldOptions,
  extra?: Record<string, unknown>,
  sharedOpts?: SharedFileOptions,
): string {
  const includeDashboard = sharedOpts?.dashboard !== false

  const scripts: Record<string, string> = {
    dev: "flink-reactor dev",
    synth: "flink-reactor synth",
    validate: "flink-reactor validate",
    test: "vitest run",
    "test:watch": "vitest",
  }

  if (includeDashboard) {
    scripts.dashboard = "flink-reactor-dashboard start"
    scripts["dashboard:mock"] = "flink-reactor-dashboard start --mock"
  }

  const dependencies: Record<string, string> = {
    "flink-reactor": "^0.1.0",
  }

  if (includeDashboard) {
    dependencies["@flink-reactor/dashboard"] = "^0.1.0"
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
      jsxImportSource: "flink-reactor",
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
  return `import { defineConfig } from 'flink-reactor'

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
  return `import { defineEnvironment } from 'flink-reactor'

export default defineEnvironment({
  name: 'dev',
  // Override pipeline defaults for local development
})
`
}

export function makeNpmrc(registry: string): string {
  return `registry=${registry}\n`
}

export function sharedFiles(
  opts: ScaffoldOptions,
  sharedOpts?: SharedFileOptions,
): TemplateFile[] {
  const files: TemplateFile[] = [
    {
      path: "package.json",
      content: makePackageJson(opts, undefined, sharedOpts),
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
