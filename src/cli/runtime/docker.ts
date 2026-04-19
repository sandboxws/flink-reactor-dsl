import { existsSync, readFileSync } from "node:fs"
import { join } from "node:path"
import pc from "picocolors"
import { isClusterRunning } from "@/cli/cluster/health-check.js"
import { runClusterDown, runClusterUp } from "@/cli/commands/cluster.js"
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

const DEFAULT_SQL_GATEWAY_URL = "http://localhost:8083"

export const DockerAdapter: RuntimeAdapter = {
  name: "docker",

  async up(_ctx: ProjectContext, opts: UpOptions): Promise<void> {
    await runClusterUp({
      port: opts.port ?? "8081",
      seed: opts.seed ?? false,
      timescaledb: true,
    })
  },

  async down(_ctx: ProjectContext, opts: DownOptions): Promise<void> {
    await runClusterDown({ volumes: opts.volumes ?? false })
  },

  async deploy(ctx: ProjectContext, opts: DeployOptions): Promise<void> {
    const outdir = opts.outdir ?? "dist"
    const projectDir = ctx.projectDir

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

    const gatewayUrl =
      ctx.resolvedConfig?.sqlGateway.url ?? DEFAULT_SQL_GATEWAY_URL
    const pipelines = discoverPipelines(projectDir, opts.pipeline)

    if (opts.dryRun) {
      console.log(pc.dim("\n--- Dry run: showing generated SQL ---\n"))
      for (const p of pipelines) {
        const sqlPath = join(projectDir, outdir, p.name, "pipeline.sql")
        if (!existsSync(sqlPath)) continue
        console.log(pc.bold(`-- ${p.name}`))
        console.log(readFileSync(sqlPath, "utf-8"))
        console.log("---")
      }
      return
    }

    if (!(await isClusterRunning(8081))) {
      throw new Error(
        "Docker Flink cluster is not reachable. Start it with `fr up` first.",
      )
    }

    const { SqlGatewayClient } = await import(
      "@/cli/cluster/sql-gateway-client.js"
    )
    const client = new SqlGatewayClient(gatewayUrl)

    let submitted = 0
    let failed = 0

    for (const p of pipelines) {
      const sqlPath = join(projectDir, outdir, p.name, "pipeline.sql")
      if (!existsSync(sqlPath)) {
        console.log(pc.yellow(`  ${p.name}: no pipeline.sql — skipping`))
        continue
      }

      try {
        const result = await client.submitSqlFile(sqlPath)
        if (result.status === "ERROR") {
          const msg = result.errorMessage ?? "Unknown error"
          console.log(pc.red(`  ${p.name}: ${msg}`))
          failed++
        } else {
          console.log(pc.green(`  ${p.name}: submitted (${result.status})`))
          submitted++
        }
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err)
        console.log(pc.red(`  ${p.name}: ${msg}`))
        failed++
      }
    }

    console.log("")
    console.log(
      `  Submitted: ${pc.green(String(submitted))}  Failed: ${failed > 0 ? pc.red(String(failed)) : pc.dim("0")}`,
    )

    if (failed > 0) {
      throw new Error(`Deployment finished with ${failed} failure(s).`)
    }
  },

  async health(_ctx: ProjectContext): Promise<HealthReport> {
    const running = await isClusterRunning(8081)
    return {
      healthy: running,
      details: running
        ? "Flink cluster is reachable on :8081"
        : "Flink cluster is not reachable on :8081 — run `fr up`",
    }
  },
}
