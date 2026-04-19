import { execSync } from "node:child_process"
import { existsSync } from "node:fs"
import { join } from "node:path"
import pc from "picocolors"
import { isClusterRunning } from "@/cli/cluster/health-check.js"
import { runSynth } from "@/cli/commands/synth.js"
import type { ProjectContext } from "@/cli/discovery.js"
import { discoverPipelines } from "@/cli/discovery.js"
import type {
  DeployOptions,
  DownOptions,
  HealthReport,
  RuntimeAdapter,
  UpOptions,
} from "./types.js"

/**
 * Locate a local Flink install. Order:
 *   1. Explicit `flinkHome` in the resolved config
 *   2. `$FLINK_HOME` env var
 *   3. `brew --prefix apache-flink` if Homebrew is on PATH
 *
 * Throws with an actionable message when nothing is found — this adapter
 * intentionally has no "quietly skip" path, since the user asked to run on
 * the homebrew lane.
 */
function resolveFlinkHome(ctx: ProjectContext): string {
  const declared = ctx.resolvedConfig?.flinkHome
  if (declared) return declared

  const envHome = process.env.FLINK_HOME
  if (envHome) return envHome

  try {
    const prefix = execSync("brew --prefix apache-flink", {
      stdio: "pipe",
      encoding: "utf-8",
    }).trim()
    // Homebrew layout: <prefix>/libexec contains the Flink dist.
    const libexec = join(prefix, "libexec")
    if (existsSync(libexec)) return libexec
    if (existsSync(prefix)) return prefix
  } catch {
    // brew not installed or apache-flink not installed
  }

  throw new Error(
    "Could not locate a local Flink install.\n" +
      "  Set `flinkHome` in flink-reactor.config.ts, export $FLINK_HOME,\n" +
      "  or run `brew install apache-flink`.",
  )
}

function runFlinkScript(flinkHome: string, script: string): void {
  const path = join(flinkHome, "bin", script)
  if (!existsSync(path)) {
    throw new Error(`Flink script not found: ${path}`)
  }
  execSync(`"${path}"`, { stdio: "inherit" })
}

export const HomebrewAdapter: RuntimeAdapter = {
  name: "homebrew",

  async up(ctx: ProjectContext, opts: UpOptions): Promise<void> {
    const flinkHome = resolveFlinkHome(ctx)
    console.log(pc.dim(`Starting standalone Flink from ${flinkHome}...`))
    runFlinkScript(flinkHome, "start-cluster.sh")

    const port = parseInt(opts.port ?? "8081", 10)
    const deadline = Date.now() + 30_000
    while (Date.now() < deadline) {
      if (await isClusterRunning(port)) {
        console.log(pc.green(`Flink UI: http://localhost:${port}`))
        return
      }
      await new Promise((r) => setTimeout(r, 1_000))
    }
    throw new Error(
      `Flink did not become ready on :${port} within 30s. ` +
        `Check ${flinkHome}/log/*.log for details.`,
    )
  },

  async down(ctx: ProjectContext, _opts: DownOptions): Promise<void> {
    const flinkHome = resolveFlinkHome(ctx)
    console.log(pc.dim(`Stopping standalone Flink at ${flinkHome}...`))
    runFlinkScript(flinkHome, "stop-cluster.sh")
  },

  async deploy(ctx: ProjectContext, opts: DeployOptions): Promise<void> {
    const outdir = opts.outdir ?? "dist"
    const projectDir = ctx.projectDir
    const flinkHome = resolveFlinkHome(ctx)
    const sqlClient = join(flinkHome, "bin", "sql-client.sh")

    if (!existsSync(sqlClient)) {
      throw new Error(`sql-client.sh not found at ${sqlClient}`)
    }

    console.log(pc.dim("Running synthesis...\n"))
    const artifacts = await runSynth({
      pipeline: opts.pipeline,
      env: ctx.resolvedConfig?.environmentName,
      outdir,
      projectDir,
      consoleUrl: opts.consoleUrl,
    })

    if (artifacts.length === 0) {
      console.log(pc.yellow("No pipelines to deploy."))
      return
    }

    const pipelines = discoverPipelines(projectDir, opts.pipeline)

    if (opts.dryRun) {
      console.log(pc.dim("\n--- Dry run: SQL files ---\n"))
      for (const p of pipelines) {
        const sqlPath = join(projectDir, outdir, p.name, "pipeline.sql")
        console.log(`  ${p.name} → ${sqlPath}`)
      }
      return
    }

    let failed = 0
    for (const p of pipelines) {
      const sqlPath = join(projectDir, outdir, p.name, "pipeline.sql")
      if (!existsSync(sqlPath)) {
        console.log(pc.yellow(`  ${p.name}: no pipeline.sql — skipping`))
        continue
      }

      console.log(pc.dim(`Submitting ${p.name} via sql-client.sh...`))
      try {
        execSync(`"${sqlClient}" -f "${sqlPath}"`, { stdio: "inherit" })
        console.log(pc.green(`  ${p.name}: submitted`))
      } catch {
        console.log(pc.red(`  ${p.name}: submission failed`))
        failed++
      }
    }

    if (failed > 0) {
      throw new Error(`Deployment finished with ${failed} failure(s).`)
    }
  },

  async health(ctx: ProjectContext): Promise<HealthReport> {
    const rawUrl = ctx.resolvedConfig?.cluster.url
    const urlStr = typeof rawUrl === "string" ? rawUrl : undefined
    const port = parseInt(urlStr?.match(/:(\d+)/)?.[1] ?? "8081", 10)
    const running = await isClusterRunning(port)
    return {
      healthy: running,
      details: running
        ? `Local Flink is reachable on :${port}`
        : `Local Flink is not reachable on :${port}`,
    }
  },
}
