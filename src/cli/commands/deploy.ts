import type { Command } from "commander"
import { Effect } from "effect"
import pc from "picocolors"
import { resolveProjectContext } from "@/cli/discovery.js"
import { runCommand } from "@/cli/effect-runner.js"
import { selectAdapter } from "@/cli/runtime/index.js"
import type { Runtime } from "@/core/config.js"
import { CliError } from "@/core/errors.js"

// ── Command registration ────────────────────────────────────────────

export function registerDeployCommand(program: Command): void {
  program
    .command("deploy")
    .description("Deploy pipelines to the runtime for the current environment")
    .option("-p, --pipeline <name>", "Deploy a specific pipeline")
    .option("-e, --env <name>", "Environment name (default: auto-select)")
    .option(
      "--dry-run",
      "Synth only, print generated artifacts without applying",
    )
    .option(
      "--runtime <name>",
      "Override the env's runtime (docker | minikube | homebrew | kubernetes)",
    )
    .option("--context <context>", "kubectl context (overrides env config)")
    .option(
      "--console-url <url>",
      "Push tap manifests to reactor-console at this URL",
    )
    .action(
      async (opts: {
        pipeline?: string
        env?: string
        dryRun?: boolean
        runtime?: string
        context?: string
        consoleUrl?: string
      }) => {
        await runCommand(
          Effect.tryPromise({
            try: () => runDeploy(opts),
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

async function runDeploy(opts: {
  pipeline?: string
  env?: string
  dryRun?: boolean
  runtime?: string
  context?: string
  consoleUrl?: string
  projectDir?: string
}): Promise<void> {
  const projectDir = opts.projectDir ?? process.cwd()
  const ctx = await resolveProjectContext(projectDir, {
    pipeline: opts.pipeline,
    env: opts.env,
  })

  // `--context` flag: inject into resolvedConfig so adapters see it.
  let effectiveCtx = ctx
  if (opts.context && ctx.resolvedConfig) {
    effectiveCtx = {
      ...ctx,
      resolvedConfig: {
        ...ctx.resolvedConfig,
        kubectl: { context: opts.context },
      },
    }
  }

  const adapter = selectAdapter(effectiveCtx, opts.runtime as Runtime)
  console.log(
    pc.dim(
      `Deploying via runtime=${adapter.name}${
        ctx.resolvedConfig?.environmentName
          ? ` (env=${ctx.resolvedConfig.environmentName})`
          : ""
      }\n`,
    ),
  )

  await adapter.deploy(effectiveCtx, {
    pipeline: opts.pipeline,
    dryRun: opts.dryRun,
    consoleUrl: opts.consoleUrl,
  })
}
