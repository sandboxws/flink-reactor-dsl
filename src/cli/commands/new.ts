import { Command } from 'commander';
import * as clack from '@clack/prompts';
import pc from 'picocolors';
import Handlebars from 'handlebars';
import { existsSync, mkdirSync, writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { execSync } from 'node:child_process';
import { getStarterTemplates } from '../templates/starter.js';
import { getMinimalTemplates } from '../templates/minimal.js';
import { getCdcLakehouseTemplates } from '../templates/cdc-lakehouse.js';
import { getRealtimeAnalyticsTemplates } from '../templates/realtime-analytics.js';
import { getMonorepoTemplates } from '../templates/monorepo.js';

export type TemplateName = 'starter' | 'minimal' | 'cdc-lakehouse' | 'realtime-analytics' | 'monorepo';
export type PackageManager = 'pnpm' | 'npm' | 'yarn';
export type FlinkVersion = '1.20' | '2.0' | '2.1' | '2.2';

export interface ScaffoldOptions {
  projectName: string;
  template: TemplateName;
  pm: PackageManager;
  flinkVersion: FlinkVersion;
  gitInit: boolean;
  installDeps: boolean;
}

export interface TemplateFile {
  path: string;
  content: string;
}

type TemplateFactory = (opts: ScaffoldOptions) => TemplateFile[];

const TEMPLATE_FACTORIES: Record<TemplateName, TemplateFactory> = {
  starter: getStarterTemplates,
  minimal: getMinimalTemplates,
  'cdc-lakehouse': getCdcLakehouseTemplates,
  'realtime-analytics': getRealtimeAnalyticsTemplates,
  monorepo: getMonorepoTemplates,
};

const TEMPLATE_DESCRIPTIONS: Record<TemplateName, string> = {
  starter: 'Kafka source → transform → Kafka sink',
  minimal: 'Empty project structure',
  'cdc-lakehouse': 'Debezium CDC → Paimon/Iceberg lakehouse',
  'realtime-analytics': 'Kafka → windowed aggregation → JDBC',
  monorepo: 'pnpm workspace with packages/ and apps/',
};

export function registerNewCommand(program: Command): void {
  program
    .command('new')
    .argument('<project-name>', 'Name of the new project')
    .option('-t, --template <template>', 'Project template')
    .option('--pm <pm>', 'Package manager (pnpm, npm, yarn)')
    .option('--flink-version <version>', 'Flink version (1.20, 2.0, 2.1, 2.2)')
    .option('-y, --yes', 'Use defaults, skip prompts')
    .option('--no-git', 'Skip git initialization')
    .option('--no-install', 'Skip dependency installation')
    .description('Create a new FlinkReactor project')
    .action(async (projectName: string, opts: Record<string, unknown>) => {
      await runNewCommand(projectName, opts);
    });
}

export async function runNewCommand(
  projectName: string,
  opts: Record<string, unknown>,
): Promise<void> {
  const projectDir = resolve(projectName);

  if (existsSync(projectDir)) {
    console.error(pc.red(`Error: Directory "${projectName}" already exists.`));
    process.exitCode = 1;
    return;
  }

  const nonInteractive = opts.yes === true;

  let options: ScaffoldOptions;

  if (nonInteractive) {
    options = {
      projectName,
      template: validateTemplate(opts.template as string) ?? 'starter',
      pm: validatePm(opts.pm as string) ?? 'pnpm',
      flinkVersion: validateFlinkVersion(opts.flinkVersion as string) ?? '1.20',
      gitInit: opts.git !== false,
      installDeps: opts.install !== false,
    };
  } else {
    const collected = await collectOptions(projectName, opts);
    if (!collected) return;
    options = collected;
  }

  scaffoldProject(projectDir, options);

  if (options.gitInit) {
    try {
      execSync('git init', { cwd: projectDir, stdio: 'ignore' });
    } catch {
      console.warn(pc.yellow('Warning: git init failed. Skipping.'));
    }
  }

  if (options.installDeps) {
    try {
      console.log(pc.dim(`Running ${options.pm} install...`));
      execSync(`${options.pm} install`, { cwd: projectDir, stdio: 'inherit' });
    } catch {
      console.warn(pc.yellow('Warning: dependency installation failed. Run it manually.'));
    }
  }

  console.log('');
  console.log(pc.green('Project created successfully!'));
  console.log('');
  console.log(`  ${pc.dim('cd')} ${projectName}`);
  console.log(`  ${pc.dim(options.pm)} run dev`);
  console.log('');
}

async function collectOptions(
  projectName: string,
  cliOpts: Record<string, unknown>,
): Promise<ScaffoldOptions | null> {
  clack.intro(pc.bgCyan(pc.black(' flink-reactor new ')));

  const template = cliOpts.template
    ? validateTemplate(cliOpts.template as string) ?? 'starter'
    : await promptTemplate();

  if (clack.isCancel(template)) {
    clack.cancel('Operation cancelled.');
    return null;
  }

  const pm = cliOpts.pm
    ? validatePm(cliOpts.pm as string) ?? 'pnpm'
    : await promptPm();

  if (clack.isCancel(pm)) {
    clack.cancel('Operation cancelled.');
    return null;
  }

  const flinkVersion = cliOpts.flinkVersion
    ? validateFlinkVersion(cliOpts.flinkVersion as string) ?? '1.20'
    : await promptFlinkVersion();

  if (clack.isCancel(flinkVersion)) {
    clack.cancel('Operation cancelled.');
    return null;
  }

  const gitInit = await clack.confirm({
    message: 'Initialize a git repository?',
    initialValue: true,
  });

  if (clack.isCancel(gitInit)) {
    clack.cancel('Operation cancelled.');
    return null;
  }

  const installDeps = await clack.confirm({
    message: `Install dependencies with ${pm as string}?`,
    initialValue: true,
  });

  if (clack.isCancel(installDeps)) {
    clack.cancel('Operation cancelled.');
    return null;
  }

  clack.outro(pc.green('Scaffolding project...'));

  return {
    projectName,
    template: template as TemplateName,
    pm: pm as PackageManager,
    flinkVersion: flinkVersion as FlinkVersion,
    gitInit: gitInit as boolean,
    installDeps: installDeps as boolean,
  };
}

async function promptTemplate(): Promise<TemplateName | symbol> {
  return clack.select({
    message: 'Which template would you like to use?',
    options: [
      { value: 'starter', label: 'Starter', hint: TEMPLATE_DESCRIPTIONS.starter },
      { value: 'minimal', label: 'Minimal', hint: TEMPLATE_DESCRIPTIONS.minimal },
      { value: 'cdc-lakehouse', label: 'CDC to Lakehouse', hint: TEMPLATE_DESCRIPTIONS['cdc-lakehouse'] },
      { value: 'realtime-analytics', label: 'Real-time Analytics', hint: TEMPLATE_DESCRIPTIONS['realtime-analytics'] },
      { value: 'monorepo', label: 'Monorepo', hint: TEMPLATE_DESCRIPTIONS.monorepo },
    ],
  }) as Promise<TemplateName | symbol>;
}

async function promptPm(): Promise<PackageManager | symbol> {
  return clack.select({
    message: 'Which package manager?',
    options: [
      { value: 'pnpm', label: 'pnpm', hint: 'recommended' },
      { value: 'npm', label: 'npm' },
      { value: 'yarn', label: 'yarn' },
    ],
  }) as Promise<PackageManager | symbol>;
}

async function promptFlinkVersion(): Promise<FlinkVersion | symbol> {
  return clack.select({
    message: 'Target Flink version?',
    options: [
      { value: '1.20', label: 'Flink 1.20 LTS', hint: 'recommended' },
      { value: '2.0', label: 'Flink 2.0' },
      { value: '2.1', label: 'Flink 2.1' },
      { value: '2.2', label: 'Flink 2.2' },
    ],
  }) as Promise<FlinkVersion | symbol>;
}

function validateTemplate(value: string | undefined): TemplateName | null {
  const valid: TemplateName[] = ['starter', 'minimal', 'cdc-lakehouse', 'realtime-analytics', 'monorepo'];
  return valid.includes(value as TemplateName) ? (value as TemplateName) : null;
}

function validatePm(value: string | undefined): PackageManager | null {
  const valid: PackageManager[] = ['pnpm', 'npm', 'yarn'];
  return valid.includes(value as PackageManager) ? (value as PackageManager) : null;
}

function validateFlinkVersion(value: string | undefined): FlinkVersion | null {
  const valid: FlinkVersion[] = ['1.20', '2.0', '2.1', '2.2'];
  return valid.includes(value as FlinkVersion) ? (value as FlinkVersion) : null;
}

export function scaffoldProject(projectDir: string, options: ScaffoldOptions): void {
  const factory = TEMPLATE_FACTORIES[options.template];
  const files = factory(options);

  for (const file of files) {
    const filePath = join(projectDir, file.path);
    const dir = join(filePath, '..');
    mkdirSync(dir, { recursive: true });
    writeFileSync(filePath, file.content, 'utf-8');
  }
}

export function renderTemplate(templateSource: string, context: Record<string, unknown>): string {
  const compiled = Handlebars.compile(templateSource, { noEscape: true });
  return compiled(context);
}
