import type { Command } from "commander"
import { Effect } from "effect"
import pc from "picocolors"
import { resolveProjectContext } from "@/cli/discovery.js"
import { runCommand } from "@/cli/effect-runner.js"
import { selectAdapter } from "@/cli/runtime/index.js"
import type { Runtime } from "@/core/config.js"
import { CliError } from "@/core/errors.js"

export function registerDownCommand(program: Command): void {
  program
    .command("down")
    .description(
      "Stop the runtime for the current environment (docker, minikube, or homebrew)",
    )
    .option("-e, --env <name>", "Environment name (default: auto-select)")
    .option(
      "--runtime <name>",
      "Override the env's runtime (docker | minikube | homebrew | kubernetes)",
    )
    .option("--volumes", "Remove persistent volumes")
    .option("--all", "Full teardown (e.g. stop the minikube VM entirely)")
    .action(
      async (opts: {
        env?: string
        runtime?: string
        volumes?: boolean
        all?: boolean
      }) => {
        await runCommand(
          Effect.tryPromise({
            try: async () => {
              const ctx = await resolveProjectContext(process.cwd(), {
                env: opts.env,
              })
              const adapter = selectAdapter(ctx, opts.runtime as Runtime)
              console.log(pc.dim(`Stopping runtime=${adapter.name}...\n`))
              await adapter.down(ctx, {
                volumes: opts.volumes,
                all: opts.all,
              })
            },
            catch: (err) =>
              new CliError({
                reason: "invalid_args",
                message: (err as Error).message,
              }),
          }),
        )
      },
    )
}
