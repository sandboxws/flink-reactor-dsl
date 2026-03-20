import type { Command } from "commander"
import { Effect } from "effect"
import pc from "picocolors"
import { runCommand } from "@/cli/effect-runner.js"
import { CliError } from "@/core/errors.js"
import { ProcessRunner } from "@/core/services.js"

// ── Command registration ────────────────────────────────────────────

export function registerSavepointCommand(program: Command): void {
  program
    .command("savepoint <pipeline>")
    .description(
      "Trigger a savepoint for a running pipeline without stopping it",
    )
    .option("-e, --env <name>", "Environment name")
    .option("-n, --namespace <ns>", "Kubernetes namespace", "flink-demo")
    .option("--context <context>", "kubectl context")
    .action(
      async (
        pipeline: string,
        opts: {
          env?: string
          namespace: string
          context?: string
        },
      ) => {
        await runCommand(runSavepointEffect(pipeline, opts))
      },
    )
}

// ── Effect variant ──────────────────────────────────────────────────

function runSavepointEffect(
  pipeline: string,
  opts: {
    namespace: string
    context?: string
  },
): Effect.Effect<void, CliError, ProcessRunner> {
  return Effect.gen(function* () {
    const runner = yield* ProcessRunner
    const contextArgs = opts.context ? ["--context", opts.context] : []
    const ns = opts.namespace

    // Step 1: Get the Flink job ID from the FlinkDeployment status.
    yield* Effect.sync(() =>
      console.log(pc.dim(`Resolving job ID for ${pipeline}...`)),
    )

    const statusResult = yield* runner.exec("kubectl", [
      "get",
      "flinkdeployment",
      pipeline,
      "-n",
      ns,
      "-o",
      "jsonpath={.status.jobStatus.jobId}",
      ...contextArgs,
    ])

    if (statusResult.exitCode !== 0 || !statusResult.stdout.trim()) {
      return yield* Effect.fail(
        new CliError({
          reason: "invalid_args",
          message: `Could not resolve job ID for ${pipeline}: ${statusResult.stderr.trim() || "no job ID in status"}`,
        }),
      )
    }

    const jobId = statusResult.stdout.trim()

    // Step 2: Get the Flink REST URL from the FlinkDeployment.
    const restResult = yield* runner.exec("kubectl", [
      "get",
      "flinkdeployment",
      pipeline,
      "-n",
      ns,
      "-o",
      "jsonpath={.status.clusterInfo.address}",
      ...contextArgs,
    ])

    // Fall back to convention-based service URL if clusterInfo not available.
    const flinkUrl = restResult.stdout.trim() || `http://${pipeline}-rest:8081`

    // Step 3: Get the savepoint directory from the FlinkDeployment config.
    const dirResult = yield* runner.exec("kubectl", [
      "get",
      "flinkdeployment",
      pipeline,
      "-n",
      ns,
      "-o",
      "jsonpath={.spec.flinkConfiguration.state\\.savepoints\\.dir}",
      ...contextArgs,
    ])

    const savepointDir = dirResult.stdout.trim() || undefined

    // Step 4: Trigger the savepoint via kubectl exec into the JM pod.
    yield* Effect.sync(() =>
      console.log(
        pc.dim(
          `Triggering savepoint for job ${jobId}${savepointDir ? ` → ${savepointDir}` : ""}...`,
        ),
      ),
    )

    // Use kubectl port-forward + curl to hit Flink REST, or exec into a pod.
    // Simplest approach: use kubectl run to create a temporary curl pod.
    const body = savepointDir
      ? `{"cancel-job":false,"target-directory":"${savepointDir}"}`
      : '{"cancel-job":false}'

    const curlResult = yield* runner.exec("kubectl", [
      "run",
      "savepoint-trigger",
      "--rm",
      "-i",
      "--restart=Never",
      `-n=${ns}`,
      "--image=curlimages/curl:8.11.0",
      "--",
      "curl",
      "-s",
      "-X",
      "POST",
      "-H",
      "Content-Type: application/json",
      "-d",
      body,
      `${flinkUrl}/jobs/${jobId}/savepoints`,
      ...contextArgs,
    ])

    if (curlResult.exitCode !== 0) {
      return yield* Effect.fail(
        new CliError({
          reason: "invalid_args",
          message: `Failed to trigger savepoint: ${curlResult.stderr.trim()}`,
        }),
      )
    }

    yield* Effect.sync(() => {
      console.log(pc.green(`Savepoint triggered for ${pipeline}`))
      const output = curlResult.stdout.trim()
      if (output) {
        try {
          const parsed = JSON.parse(output)
          if (parsed["request-id"]) {
            console.log(pc.dim(`  request-id: ${parsed["request-id"]}`))
          }
        } catch {
          console.log(pc.dim(`  response: ${output}`))
        }
      }
    })
  })
}
