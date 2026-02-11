import { Command } from 'commander';
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

const VERSION = '0.1.0';

export function createProgram(): Command {
  const program = new Command();

  program
    .name('flink-reactor')
    .description('React-style TSX DSL that synthesizes to Flink SQL + Kubernetes CRDs')
    .version(VERSION);

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

  return program;
}

const program = createProgram();
program.parse();
