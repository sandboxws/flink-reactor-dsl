import type { Command } from "commander"
import { Effect } from "effect"
import pc from "picocolors"
import { runCommand } from "@/cli/effect-runner.js"
import { CliError } from "@/core/errors.js"
import { ProcessRunner } from "@/core/services.js"

// ── Command registration ────────────────────────────────────────────

export function registerStopCommand(program: Command): void {
  program
    .command("stop [pipeline]")
    .description(
      "Stop pipelines by suspending their FlinkDeployments (triggers savepoint)",
    )
    .option("-a, --all", "Stop all pipelines in the namespace")
    .option("-e, --env <name>", "Environment name")
    .option("-n, --namespace <ns>", "Kubernetes namespace", "flink-demo")
    .option("--context <context>", "kubectl context")
    .action(
      async (
        pipeline: string | undefined,
        opts: {
          all?: boolean
          env?: string
          namespace: string
          context?: string
        },
      ) => {
        if (!pipeline && !opts.all) {
          console.error(
            pc.red(
              "Specify a pipeline name or use --all to stop all pipelines",
            ),
          )
          process.exitCode = 1
          return
        }
        await runCommand(runStopEffect(pipeline, opts))
      },
    )
}

// ── Effect variant ──────────────────────────────────────────────────

function runStopEffect(
  pipeline: string | undefined,
  opts: {
    all?: boolean
    namespace: string
    context?: string
  },
): Effect.Effect<void, CliError, ProcessRunner> {
  return Effect.gen(function* () {
    const runner = yield* ProcessRunner
    const contextArgs = opts.context ? ["--context", opts.context] : []
    const ns = opts.namespace

    const names = opts.all
      ? yield* listFlinkDeployments(runner, ns, contextArgs)
      : [pipeline as string]

    if (names.length === 0) {
      yield* Effect.sync(() =>
        console.log(pc.yellow("No FlinkDeployments found.")),
      )
      return
    }

    for (const name of names) {
      yield* Effect.sync(() => console.log(pc.dim(`Stopping ${name}...`)))

      const result = yield* runner.exec("kubectl", [
        "patch",
        "flinkdeployment",
        name,
        "-n",
        ns,
        "--type",
        "merge",
        "-p",
        '{"spec":{"job":{"state":"suspended"}}}',
        ...contextArgs,
      ])

      if (result.exitCode !== 0) {
        yield* Effect.sync(() =>
          console.error(pc.red(`  ${name}: ${result.stderr.trim()}`)),
        )
      } else {
        yield* Effect.sync(() =>
          console.log(pc.green(`  ${name}: suspended (savepoint triggered)`)),
        )
      }
    }

    yield* Effect.sync(() =>
      console.log(pc.dim("\nAll specified pipelines suspended.")),
    )
  })
}

// ── Helpers ─────────────────────────────────────────────────────────

function listFlinkDeployments(
  runner: ProcessRunner["Type"],
  namespace: string,
  contextArgs: string[],
): Effect.Effect<string[], CliError> {
  return Effect.gen(function* () {
    const result = yield* runner.exec("kubectl", [
      "get",
      "flinkdeployment",
      "-n",
      namespace,
      "-o",
      "jsonpath={.items[*].metadata.name}",
      ...contextArgs,
    ])

    if (result.exitCode !== 0) {
      return yield* Effect.fail(
        new CliError({
          reason: "invalid_args",
          message: `Failed to list FlinkDeployments: ${result.stderr.trim()}`,
        }),
      )
    }

    const output = result.stdout.trim()
    if (!output) return []
    return output.split(/\s+/)
  })
}
