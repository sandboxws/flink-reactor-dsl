import { existsSync, readFileSync } from "node:fs"
import { join } from "node:path"
import type { Command } from "commander"
import { Effect } from "effect"
import pc from "picocolors"
import { discoverPipelines } from "@/cli/discovery.js"
import { runCommand } from "@/cli/effect-runner.js"
import { CliError, type DiscoveryError, type FileSystemError } from "@/core/errors.js"
import { ProcessRunner } from "@/core/services.js"
import { runSynth } from "./synth.js"

// ── Command registration ────────────────────────────────────────────

export function registerDeployCommand(program: Command): void {
  program
    .command("deploy")
    .description("Deploy pipelines to a Kubernetes cluster")
    .option("-p, --pipeline <name>", "Deploy a specific pipeline")
    .option("-e, --env <name>", "Environment name")
    .option("--dry-run", "Synth only, print YAML without applying")
    .option("--context <context>", "kubectl context")
    .action(
      async (opts: {
        pipeline?: string
        env?: string
        dryRun?: boolean
        context?: string
      }) => {
        await runCommand(runDeployEffect(opts))
      },
    )
}

// ── Effect variant ──────────────────────────────────────────────────

function runDeployEffect(opts: {
  pipeline?: string
  env?: string
  dryRun?: boolean
  context?: string
  projectDir?: string
}): Effect.Effect<
  void,
  DiscoveryError | FileSystemError | CliError,
  ProcessRunner
> {
  return Effect.gen(function* () {
    const runner = yield* ProcessRunner
    const projectDir = opts.projectDir ?? process.cwd()

    // Step 1: Synthesize (still imperative — complex, internal calls)
    yield* Effect.sync(() => console.log(pc.dim("Running synthesis...\n")))
    const artifacts = yield* Effect.tryPromise({
      try: () =>
        runSynth({
          pipeline: opts.pipeline,
          env: opts.env,
          outdir: "dist",
          projectDir,
        }),
      catch: (err) =>
        new CliError({
          reason: "invalid_args",
          message: `Synthesis failed: ${(err as Error).message}`,
        }),
    })

    if (artifacts.length === 0) {
      yield* Effect.sync(() =>
        console.log(pc.yellow("No pipelines to deploy.")),
      )
      return
    }

    const pipelines = discoverPipelines(projectDir, opts.pipeline)

    if (opts.dryRun) {
      yield* Effect.sync(() => {
        console.log(pc.dim("\n--- Dry run: showing generated CRDs ---\n"))
        for (const p of pipelines) {
          const yamlPath = join(projectDir, "dist", p.name, "deployment.yaml")
          if (!existsSync(yamlPath)) continue
          const yaml = readFileSync(yamlPath, "utf-8")
          console.log(pc.bold(`# ${p.name}`))
          console.log(yaml)
          console.log("---")
        }
      })
      return
    }

    // Step 2: Apply CRDs via kubectl
    const contextArgs = opts.context ? ["--context", opts.context] : []
    let hasFailures = false

    for (const p of pipelines) {
      const yamlPath = join(projectDir, "dist", p.name, "deployment.yaml")
      if (!existsSync(yamlPath)) {
        yield* Effect.sync(() =>
          console.log(
            pc.yellow(`No deployment.yaml for ${p.name}. Skipping.`),
          ),
        )
        continue
      }

      yield* Effect.sync(() =>
        console.log(pc.dim(`\nApplying ${p.name}...`)),
      )

      const result = yield* runner.exec(
        "kubectl",
        ["apply", "-f", yamlPath, ...contextArgs],
        { cwd: projectDir },
      )

      if (result.exitCode !== 0) {
        yield* Effect.sync(() =>
          console.error(
            pc.red(
              `  ${p.name}: Failed to apply — ${result.stderr.trim()}`,
            ),
          ),
        )
        hasFailures = true
      } else {
        yield* Effect.sync(() =>
          console.log(
            pc.green(`  ${p.name}: ${result.stdout.trim()}`),
          ),
        )
      }
    }

    // Step 3: Print status
    yield* Effect.sync(() => {
      console.log("")
      if (!hasFailures) {
        console.log(pc.green("Deployment complete."))
        for (const p of pipelines) {
          console.log(pc.dim(`  ${p.name}: deployed`))
        }
        console.log("")
        const ctxFlag = opts.context ? ` --context ${opts.context}` : ""
        console.log(pc.dim("Check status with:"))
        console.log(pc.dim(`  kubectl get flinkdeployments${ctxFlag}`))
      } else {
        console.log(pc.red("Deployment finished with errors."))
      }
    })

    if (hasFailures) {
      return yield* Effect.fail(
        new CliError({
          reason: "invalid_args",
          message: "Deployment finished with errors.",
        }),
      )
    }
  })
}
