import type { Command } from "commander"
import { Effect } from "effect"
import pc from "picocolors"
import { runCommand } from "@/cli/effect-runner.js"
import { CliError } from "@/core/errors.js"
import { ProcessRunner } from "@/core/services.js"

// ── Command registration ────────────────────────────────────────────

export function registerStatusCommand(program: Command): void {
  program
    .command("status")
    .description("Show the status of all pipelines in the cluster")
    .option("-e, --env <name>", "Environment name")
    .option("-n, --namespace <ns>", "Kubernetes namespace", "flink-demo")
    .option("--context <context>", "kubectl context")
    .action(
      async (opts: { env?: string; namespace: string; context?: string }) => {
        await runCommand(runStatusEffect(opts))
      },
    )
}

// ── Effect variant ──────────────────────────────────────────────────

interface DeploymentStatus {
  name: string
  state: string
  jobState: string
  lastSavepoint: string
}

function runStatusEffect(opts: {
  namespace: string
  context?: string
}): Effect.Effect<void, CliError, ProcessRunner> {
  return Effect.gen(function* () {
    const runner = yield* ProcessRunner
    const contextArgs = opts.context ? ["--context", opts.context] : []
    const ns = opts.namespace

    const result = yield* runner.exec("kubectl", [
      "get",
      "flinkdeployment",
      "-n",
      ns,
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

    type FlinkDeploymentItem = {
      metadata?: { name?: string }
      spec?: { job?: { state?: string } }
      status?: {
        jobStatus?: {
          state?: string
          savepointInfo?: { lastSavepoint?: { location?: string } }
        }
      }
    }

    let items: FlinkDeploymentItem[]
    try {
      items =
        (JSON.parse(result.stdout) as { items?: FlinkDeploymentItem[] })
          .items ?? []
    } catch {
      items = []
    }

    if (items.length === 0) {
      yield* Effect.sync(() =>
        console.log(pc.yellow("No FlinkDeployments found.")),
      )
      return
    }

    const statuses: DeploymentStatus[] = items.map((item) => ({
      name: item.metadata?.name ?? "unknown",
      state: item.spec?.job?.state ?? "unknown",
      jobState: item.status?.jobStatus?.state ?? "—",
      lastSavepoint:
        item.status?.jobStatus?.savepointInfo?.lastSavepoint?.location ?? "—",
    }))

    // Print formatted table.
    yield* Effect.sync(() => {
      const nameWidth = Math.max(8, ...statuses.map((s) => s.name.length))
      const stateWidth = 12
      const jobWidth = 12

      const header = [
        "Pipeline".padEnd(nameWidth),
        "Status".padEnd(stateWidth),
        "Job State".padEnd(jobWidth),
        "Last Savepoint",
      ].join("  ")

      console.log("")
      console.log(pc.bold(header))
      console.log(pc.dim("─".repeat(header.length + 20)))

      for (const s of statuses) {
        const stateColor =
          s.state === "running"
            ? pc.green
            : s.state === "suspended"
              ? pc.yellow
              : pc.dim
        const jobColor =
          s.jobState === "RUNNING"
            ? pc.green
            : s.jobState === "SUSPENDED"
              ? pc.yellow
              : s.jobState === "FAILED"
                ? pc.red
                : pc.dim

        console.log(
          [
            pc.white(s.name.padEnd(nameWidth)),
            stateColor(s.state.padEnd(stateWidth)),
            jobColor(s.jobState.padEnd(jobWidth)),
            pc.dim(
              s.lastSavepoint.length > 50
                ? `${s.lastSavepoint.slice(0, 47)}...`
                : s.lastSavepoint,
            ),
          ].join("  "),
        )
      }
      console.log("")
    })
  })
}
