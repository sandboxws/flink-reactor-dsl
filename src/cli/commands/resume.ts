import type { Command } from "commander"
import { Effect } from "effect"
import pc from "picocolors"
import { runCommand } from "@/cli/effect-runner.js"
import { CliError } from "@/core/errors.js"
import { ProcessRunner } from "@/core/services.js"

// ── Command registration ────────────────────────────────────────────

export function registerResumeCommand(program: Command): void {
  program
    .command("resume [pipeline]")
    .description("Resume suspended pipelines (restores from last savepoint)")
    .option("-a, --all", "Resume all suspended pipelines in the namespace")
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
              "Specify a pipeline name or use --all to resume all pipelines",
            ),
          )
          process.exitCode = 1
          return
        }
        await runCommand(runResumeEffect(pipeline, opts))
      },
    )
}

// ── Effect variant ──────────────────────────────────────────────────

function runResumeEffect(
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
      ? yield* listSuspendedDeployments(runner, ns, contextArgs)
      : [pipeline as string]

    if (names.length === 0) {
      yield* Effect.sync(() =>
        console.log(pc.yellow("No suspended FlinkDeployments found.")),
      )
      return
    }

    for (const name of names) {
      yield* Effect.sync(() => console.log(pc.dim(`Resuming ${name}...`)))

      const result = yield* runner.exec("kubectl", [
        "patch",
        "flinkdeployment",
        name,
        "-n",
        ns,
        "--type",
        "merge",
        "-p",
        '{"spec":{"job":{"state":"running"}}}',
        ...contextArgs,
      ])

      if (result.exitCode !== 0) {
        yield* Effect.sync(() =>
          console.error(pc.red(`  ${name}: ${result.stderr.trim()}`)),
        )
      } else {
        yield* Effect.sync(() =>
          console.log(
            pc.green(`  ${name}: running (restoring from savepoint)`),
          ),
        )
      }
    }

    yield* Effect.sync(() =>
      console.log(pc.dim("\nAll specified pipelines resumed.")),
    )
  })
}

// ── Helpers ─────────────────────────────────────────────────────────

function listSuspendedDeployments(
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
      "json",
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

    try {
      const parsed = JSON.parse(result.stdout) as {
        items?: Array<{
          metadata?: { name?: string }
          spec?: { job?: { state?: string } }
          status?: { jobStatus?: { state?: string } }
        }>
      }
      return (parsed.items ?? [])
        .filter(
          (item) =>
            item.spec?.job?.state === "suspended" ||
            item.status?.jobStatus?.state === "SUSPENDED",
        )
        .map((item) => item.metadata?.name ?? "unknown")
    } catch {
      return []
    }
  })
}
