import type { ScaffoldOptions, TemplateFile } from "../commands/new.js"
import { makeConfig, makeGitignore } from "./shared.js"

export function getMonorepoTemplates(opts: ScaffoldOptions): TemplateFile[] {
  const workspacePkg = {
    name: opts.projectName,
    version: "0.1.0",
    private: true,
    scripts: {
      dev: "flink-reactor dev",
      synth: "flink-reactor synth",
      validate: "flink-reactor validate",
      test: "vitest run",
      "test:watch": "vitest",
      dashboard: "flink-reactor-dashboard start",
      "dashboard:mock": "flink-reactor-dashboard start --mock",
    },
    devDependencies: {
      "@flink-reactor/dashboard": "^0.1.0",
    },
  }

  const schemasPkg = {
    name: `@${opts.projectName}/schemas`,
    version: "0.1.0",
    private: true,
    type: "module",
    main: "index.ts",
    dependencies: {
      "flink-reactor": "^0.1.0",
    },
  }

  const patternsPkg = {
    name: `@${opts.projectName}/patterns`,
    version: "0.1.0",
    private: true,
    type: "module",
    main: "index.ts",
    dependencies: {
      "flink-reactor": "^0.1.0",
      [`@${opts.projectName}/schemas`]: "workspace:*",
    },
  }

  const appPkg = {
    name: `@${opts.projectName}/default-app`,
    version: "0.1.0",
    private: true,
    type: "module",
    dependencies: {
      "flink-reactor": "^0.1.0",
      [`@${opts.projectName}/schemas`]: "workspace:*",
      [`@${opts.projectName}/patterns`]: "workspace:*",
    },
    devDependencies: {
      typescript: "^5.7.0",
      vitest: "^3.0.0",
    },
  }

  const tsconfig = {
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
    },
    include: ["apps/**/*", "packages/**/*"],
  }

  return [
    {
      path: "package.json",
      content: `${JSON.stringify(workspacePkg, null, 2)}\n`,
    },
    {
      path: "pnpm-workspace.yaml",
      content: 'packages:\n  - "packages/*"\n  - "apps/*"\n',
    },
    {
      path: "tsconfig.json",
      content: `${JSON.stringify(tsconfig, null, 2)}\n`,
    },
    { path: "flink-reactor.config.ts", content: makeConfig(opts) },
    { path: ".gitignore", content: makeGitignore() },

    // packages/schemas
    {
      path: "packages/schemas/package.json",
      content: `${JSON.stringify(schemasPkg, null, 2)}\n`,
    },
    {
      path: "packages/schemas/index.ts",
      content: `// Export shared schemas here
// import { Schema, Field } from 'flink-reactor';
`,
    },

    // packages/patterns
    {
      path: "packages/patterns/package.json",
      content: `${JSON.stringify(patternsPkg, null, 2)}\n`,
    },
    {
      path: "packages/patterns/index.ts",
      content: `// Export reusable pipeline patterns here
`,
    },

    // apps/default-app
    {
      path: "apps/default-app/package.json",
      content: `${JSON.stringify(appPkg, null, 2)}\n`,
    },
    {
      path: "apps/default-app/flink-reactor.config.ts",
      content: makeConfig(opts),
    },
    {
      path: "apps/default-app/env/dev.ts",
      content: `import { defineEnvironment } from 'flink-reactor';

export default defineEnvironment({
  name: 'dev',
});
`,
    },
    {
      path: "apps/default-app/pipelines/.gitkeep",
      content: "",
    },
    {
      path: "apps/default-app/tests/.gitkeep",
      content: "",
    },
  ]
}
