import { Command } from 'commander';
import { registerNewCommand } from './commands/new.js';
import { registerDoctorCommand } from './commands/doctor.js';
import { registerInstallCommand } from './commands/install.js';
import { registerGenerateCommand } from './commands/generate.js';

const VERSION = '0.1.0';

export function createProgram(): Command {
  const program = new Command();

  program
    .name('flink-reactor')
    .description('React-style TSX DSL that synthesizes to Flink SQL + Kubernetes CRDs')
    .version(VERSION);

  // Implemented commands
  registerNewCommand(program);
  registerDoctorCommand(program);
  registerInstallCommand(program);
  registerGenerateCommand(program);

  // Stub commands (implemented in change 11)
  program
    .command('synth')
    .description('Synthesize pipelines to Flink SQL and CRDs')
    .action(() => {
      console.log('Not yet implemented. Coming soon.');
      process.exitCode = 1;
    });

  program
    .command('validate')
    .description('Validate pipeline definitions and configuration')
    .action(() => {
      console.log('Not yet implemented. Coming soon.');
      process.exitCode = 1;
    });

  program
    .command('graph')
    .description('Visualize pipeline DAG')
    .action(() => {
      console.log('Not yet implemented. Coming soon.');
      process.exitCode = 1;
    });

  program
    .command('dev')
    .description('Start development mode with file watching')
    .action(() => {
      console.log('Not yet implemented. Coming soon.');
      process.exitCode = 1;
    });

  program
    .command('deploy')
    .description('Deploy pipelines to a Kubernetes cluster')
    .action(() => {
      console.log('Not yet implemented. Coming soon.');
      process.exitCode = 1;
    });

  return program;
}

const program = createProgram();
program.parse();
