import { Command, Help } from "commander"
import pc from "picocolors"
import { registerClusterCommand } from "./commands/cluster.js"
import { registerCompletionCommand } from "./commands/completion.js"
import { registerDeployCommand } from "./commands/deploy.js"
import { registerDevCommand } from "./commands/dev.js"
import { registerDoctorCommand } from "./commands/doctor.js"
import { registerGenerateCommand } from "./commands/generate.js"
import { registerGraphCommand } from "./commands/graph.js"
import { registerInstallCommand } from "./commands/install.js"
import { registerNewCommand } from "./commands/new.js"
import { registerResumeCommand } from "./commands/resume.js"
import { registerSavepointCommand } from "./commands/savepoint.js"
import { registerSchemaCommand } from "./commands/schema.js"
import { registerStatusCommand } from "./commands/status.js"
import { registerStopCommand } from "./commands/stop.js"
import { registerSynthCommand } from "./commands/synth.js"
import { registerUpgradeCommand } from "./commands/upgrade.js"
import { registerValidateCommand } from "./commands/validate.js"
import { DSL_VERSION } from "./templates/shared.js"

const VERSION = DSL_VERSION

// ── Program setup ───────────────────────────────────────────────────

export function createProgram(): Command {
  const program = new Command()

  program
    .name("flink-reactor")
    .description(
      "React-style TSX DSL that synthesizes to Flink SQL + Kubernetes CRDs",
    )
    .version(VERSION)
    .option("--verbose", "Show full error traces for unexpected errors")

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
      const output: string = Help.prototype.formatHelp.call(this, cmd, helper)

      // Add branded header for root command
      if (cmd.name() === "flink-reactor" && !cmd.parent) {
        const header = [
          "",
          `  ${pc.bold(pc.cyan("flink-reactor"))} ${pc.dim(`v${VERSION}`)}`,
          `  ${pc.dim("React-style TSX DSL → Flink SQL + Kubernetes CRDs")}`,
          "",
        ].join("\n")

        const lines = output.split("\n")
        const withoutDesc = lines.filter(
          (l: string) => !l.includes("React-style TSX DSL that synthesizes"),
        )
        return `${header}\n${withoutDesc.join("\n")}`
      }

      return output
    },
  })

  // Scaffold commands
  registerNewCommand(program)
  registerDoctorCommand(program)
  registerInstallCommand(program)
  registerGenerateCommand(program)

  // Core commands
  registerSynthCommand(program)
  registerValidateCommand(program)
  registerGraphCommand(program)
  registerDevCommand(program)
  registerDeployCommand(program)
  registerClusterCommand(program)
  registerSchemaCommand(program)
  registerUpgradeCommand(program)

  // Lifecycle commands
  registerStopCommand(program)
  registerResumeCommand(program)
  registerSavepointCommand(program)
  registerStatusCommand(program)

  // Shell completion
  registerCompletionCommand(program)

  return program
}
