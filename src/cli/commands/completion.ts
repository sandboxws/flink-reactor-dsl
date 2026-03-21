import type { Command } from "commander"
import { introspectProgram } from "@/cli/completion/introspect.js"
import { generateZshCompletion } from "@/cli/completion/zsh.js"

export function registerCompletionCommand(program: Command): void {
  program
    .command("completion")
    .argument("<shell>", "Shell type (zsh)")
    .description("Generate shell completion script")
    .addHelpText(
      "after",
      `
Install completions:
  # Option A: one-time file install
  flink-reactor completion zsh > ~/.zsh/completions/_flink-reactor

  # Option B: eval in .zshrc (always up-to-date)
  eval "$(flink-reactor completion zsh)"
`,
    )
    .action((shell: string) => {
      switch (shell) {
        case "zsh": {
          const tree = introspectProgram(program)
          const script = generateZshCompletion(tree)
          process.stdout.write(`${script}\n`)
          break
        }
        default:
          console.error(`Unsupported shell: ${shell}. Supported shells: zsh`)
          process.exitCode = 1
      }
    })
}
