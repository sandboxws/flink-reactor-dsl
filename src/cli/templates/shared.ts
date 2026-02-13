import type { ScaffoldOptions, TemplateFile } from '../commands/new.js';

export function makePackageJson(opts: ScaffoldOptions, extra?: Record<string, unknown>): string {
  const pkg: Record<string, unknown> = {
    name: opts.projectName,
    version: '0.1.0',
    private: true,
    type: 'module',
    scripts: {
      dev: 'flink-reactor dev',
      synth: 'flink-reactor synth',
      validate: 'flink-reactor validate',
      test: 'vitest run',
      'test:watch': 'vitest',
    },
    dependencies: {
      'flink-reactor': '^0.1.0',
    },
    devDependencies: {
      typescript: '^5.7.0',
      vitest: '^3.0.0',
    },
    ...extra,
  };
  return JSON.stringify(pkg, null, 2) + '\n';
}

export function makeTsconfig(opts: ScaffoldOptions): string {
  const config = {
    compilerOptions: {
      target: 'ES2022',
      module: 'ESNext',
      moduleResolution: 'bundler',
      lib: ['ES2022'],
      strict: true,
      esModuleInterop: true,
      skipLibCheck: true,
      forceConsistentCasingInFileNames: true,
      resolveJsonModule: true,
      jsx: 'react-jsx',
      jsxImportSource: 'flink-reactor',
    },
    include: ['pipelines/**/*', 'schemas/**/*', 'env/**/*', 'patterns/**/*'],
  };
  return JSON.stringify(config, null, 2) + '\n';
}

export function makeConfig(opts: ScaffoldOptions): string {
  return `import { defineConfig } from 'flink-reactor';

export default defineConfig({
  flink: { version: '${opts.flinkVersion}' },
});
`;
}

export function makeGitignore(): string {
  return `node_modules/
dist/
.flink-reactor/
*.tsbuildinfo
.env
.env.local
`;
}

export function makeDevEnv(opts: ScaffoldOptions): string {
  return `import { defineEnvironment } from 'flink-reactor';

export default defineEnvironment({
  name: 'dev',
  // Override pipeline defaults for local development
});
`;
}

export function makeNpmrc(registry: string): string {
  return `registry=${registry}\n`;
}

export function sharedFiles(opts: ScaffoldOptions): TemplateFile[] {
  const files: TemplateFile[] = [
    { path: 'package.json', content: makePackageJson(opts) },
    { path: 'tsconfig.json', content: makeTsconfig(opts) },
    { path: 'flink-reactor.config.ts', content: makeConfig(opts) },
    { path: '.gitignore', content: makeGitignore() },
    { path: 'env/dev.ts', content: makeDevEnv(opts) },
  ];
  if (opts.registry) {
    files.push({ path: '.npmrc', content: makeNpmrc(opts.registry) });
  }
  return files;
}
