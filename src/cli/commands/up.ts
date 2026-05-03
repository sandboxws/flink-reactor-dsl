import { type Command, Option } from "commander"
import { Effect } from "effect"
import pc from "picocolors"
import { resolveProjectContext } from "@/cli/discovery.js"
import { runCommand } from "@/cli/effect-runner.js"
import type { ContainerEngineChoice } from "@/cli/runtime/container-engine.js"
import { selectAdapter } from "@/cli/runtime/index.js"
import type { Runtime } from "@/core/config.js"
import { CliError } from "@/core/errors.js"

export function registerUpCommand(program: Command): void {
  program
    .command("up")
    .description(
      "Start the runtime for the current environment (docker, minikube, or homebrew)",
    )
    .option("-e, --env <name>", "Environment name (default: auto-select)")
    .option(
      "--runtime <name>",
      "Override the env's runtime (docker | minikube | homebrew | kubernetes)",
    )
    .addOption(
      new Option(
        "--container-engine <name>",
        "Container engine override for the docker lane (auto | docker | podman)",
      ).choices(["auto", "docker", "podman"]),
    )
    .option("--port <port>", "Flink REST port", "8081")
    .option("--seed", "Submit example pipelines after startup (docker only)")
    .action(
      async (opts: {
        env?: string
        runtime?: string
        containerEngine?: ContainerEngineChoice
        port: string
        seed?: boolean
      }) => {
        await runCommand(
          Effect.tryPromise({
            try: async () => {
              const ctx = await resolveProjectContext(process.cwd(), {
                env: opts.env,
              })
              const adapter = selectAdapter(ctx, opts.runtime as Runtime)
              console.log(
                pc.dim(
                  `Starting runtime=${adapter.name}${
                    ctx.resolvedConfig?.environmentName
                      ? ` (env=${ctx.resolvedConfig.environmentName})`
                      : ""
                  }...\n`,
                ),
              )
              await adapter.up(ctx, {
                port: opts.port,
                seed: opts.seed,
                containerEngine: opts.containerEngine,
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
