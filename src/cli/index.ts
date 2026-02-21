import { Command, Help } from 'commander';
import pc from 'picocolors';
import { registerNewCommand } from './commands/new.js';
import { registerDoctorCommand } from './commands/doctor.js';
import { registerInstallCommand } from './commands/install.js';
import { registerGenerateCommand } from './commands/generate.js';
import { registerSynthCommand } from './commands/synth.js';
import { registerValidateCommand } from './commands/validate.js';
import { registerGraphCommand } from './commands/graph.js';
import { registerDevCommand } from './commands/dev.js';
import { registerDeployCommand } from './commands/deploy.js';
import { registerClusterCommand } from './commands/cluster.js';
import { registerSchemaCommand } from './commands/schema.js';
import { registerUpgradeCommand } from './commands/upgrade.js';
import { registerDashboardCommand } from './commands/dashboard.js';

const VERSION = '0.1.0';

// ── Program setup ───────────────────────────────────────────────────

export function createProgram(): Command {
  const program = new Command();

  program
    .name('flink-reactor')
    .description('React-style TSX DSL that synthesizes to Flink SQL + Kubernetes CRDs')
    .version(VERSION);

  // Styled help output — Commander v13 style* hooks
  program.configureHelp({
    helpWidth: 80,
    showGlobalOptions: false,
    styleTitle: (str: string) => pc.bold(pc.cyan(str)),
    styleUsage: (str: string) => pc.yellow(str),
    styleCommandText: (str: string) => pc.green(str),
    styleCommandDescription: (str: string) => pc.dim(str),
    styleSubcommandTerm: (str: string) => pc.green(str),
    styleSubcommandDescription: (str: string) => pc.dim(str),
    styleOptionTerm: (str: string) => pc.yellow(str),
    styleOptionDescription: (str: string) => pc.dim(str),
    styleArgumentTerm: (str: string) => pc.yellow(str),
    styleArgumentDescription: (str: string) => pc.dim(str),
    styleDescriptionText: (str: string) => pc.white(str),
    formatHelp(cmd: Command, helper: Help): string {
      const output: string = Help.prototype.formatHelp.call(this, cmd, helper);

      // Add branded header for root command
      if (cmd.name() === 'flink-reactor' && !cmd.parent) {
        const header = [
          '',
          `  ${pc.bold(pc.cyan('flink-reactor'))} ${pc.dim(`v${VERSION}`)}`,
          `  ${pc.dim('React-style TSX DSL → Flink SQL + Kubernetes CRDs')}`,
          '',
        ].join('\n');

        const lines = output.split('\n');
        const withoutDesc = lines.filter(
          (l: string) => !l.includes('React-style TSX DSL that synthesizes'),
        );
        return header + '\n' + withoutDesc.join('\n');
      }

      return output;
    },
  });

  // Scaffold commands
  registerNewCommand(program);
  registerDoctorCommand(program);
  registerInstallCommand(program);
  registerGenerateCommand(program);

  // Core commands
  registerSynthCommand(program);
  registerValidateCommand(program);
  registerGraphCommand(program);
  registerDevCommand(program);
  registerDeployCommand(program);
  registerClusterCommand(program);
  registerSchemaCommand(program);
  registerUpgradeCommand(program);
  registerDashboardCommand(program);

  return program;
}

const program = createProgram();
program.parse();
