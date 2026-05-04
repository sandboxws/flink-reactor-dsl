import { execSync } from "node:child_process"
import { existsSync, readFileSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import pc from "picocolors"
import { isClusterRunning } from "@/cli/cluster/health-check.js"
import { runClusterDown, runClusterUp } from "@/cli/commands/cluster.js"
import { runSynth } from "@/cli/commands/synth.js"
import type { ProjectContext } from "@/cli/discovery.js"
import { discoverPipelines } from "@/cli/discovery.js"
import {
  type ContainerEngineChoice,
  detectContainerEngine,
} from "./container-engine.js"
import type {
  DeployOptions,
  DownOptions,
  HealthReport,
  RuntimeAdapter,
  UpOptions,
} from "./types.js"

const DEFAULT_SQL_GATEWAY_URL = "http://localhost:8083"
const JOBMANAGER_CONTAINER = "cluster-jobmanager-1"

export const DockerAdapter: RuntimeAdapter = {
  name: "docker",

  async up(_ctx: ProjectContext, opts: UpOptions): Promise<void> {
    await runClusterUp({
      port: opts.port ?? "8081",
      seed: opts.seed ?? false,
      timescaledb: true,
      containerEngine: opts.containerEngine,
    })
  },

  async down(_ctx: ProjectContext, opts: DownOptions): Promise<void> {
    await runClusterDown({
      volumes: opts.volumes ?? false,
      containerEngine: opts.containerEngine,
    })
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
      const yamlPath = join(projectDir, outdir, p.name, "pipeline.yaml")

      // Pipeline-Connector pipelines (e.g. Flink CDC ingest) emit a
      // pipeline.yaml. The companion pipeline.sql is a placeholder
      // comment file — submitting it would fool the SQL gateway into
      // reporting a no-op success while no Flink job actually runs.
      // Detect the YAML and route through the flink-cdc.sh submission
      // path inside the JobManager container instead.
      if (existsSync(yamlPath)) {
        try {
          submitPipelineYaml(yamlPath, p.name)
          console.log(pc.green(`  ${p.name}: submitted (Pipeline-YAML)`))
          submitted++
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err)
          console.log(pc.red(`  ${p.name}: ${msg}`))
          failed++
        }
        continue
      }

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

// ── Pipeline-YAML submission ────────────────────────────────────────
//
// `flink-cdc.sh` submits Pipeline Connector jobs through the JobManager
// REST API, so the script must run inside the cluster network. Mounting
// pipeline.yaml on the host wouldn't work because flink-cdc.sh resolves
// the file path locally on the JM container. We `cp` the file into a
// transient location inside the JM and then `exec` the launcher there.
//
// The JM container ships `flink-cdc-cli.jar` plus the postgres+fluss
// pipeline connectors via the runtime-image build (Dockerfile.flink).
// Forwarding the host's PG_PRIMARY_PASSWORD into the exec environment
// resolves `${env:...}` placeholders inside pipeline.yaml at parse time.

function submitPipelineYaml(yamlPath: string, pipelineName: string): void {
  // Engine selection. `FR_DOCKER_BIN` is an escape hatch for hybrid
  // setups (e.g. Docker Desktop running the cluster while the user's
  // shell aliases `docker` to a different engine like podman). When
  // unset we fall through to the runtime auto-detect.
  const dockerBin =
    process.env.FR_DOCKER_BIN ??
    detectContainerEngine({ flag: "auto" as ContainerEngineChoice }).bin
  const remotePath = `/tmp/${pipelineName}-pipeline.yaml`

  // 1. Resolve `${env:VAR}` placeholders against the host environment.
  //    Flink CDC 3.6's pipeline.yaml loader does NOT do env substitution
  //    (unlike `flink-conf.yaml`), so a placeholder like `${env:PG_PASS}`
  //    is passed through as a literal — Postgres then rejects it with
  //    "password authentication failed". The synthesised YAML keeps the
  //    placeholder so the same artifact remains valid for Kubernetes
  //    (where envFrom/secretKeyRef inject the value into the JM pod);
  //    here we just inline it before docker-cp.
  const resolvedYaml = resolveEnvPlaceholders(readFileSync(yamlPath, "utf-8"))
  const tmpResolved = join(tmpdir(), `fr-${pipelineName}-resolved.yaml`)
  writeFileSync(tmpResolved, resolvedYaml, "utf-8")

  // 2. Copy resolved pipeline.yaml into the JobManager container.
  execSync(
    `${dockerBin} cp ${quoteShell(tmpResolved)} ${JOBMANAGER_CONTAINER}:${remotePath}`,
    { stdio: "pipe" },
  )

  // 3. Run flink-cdc.sh inside the container.
  const execCmd = [
    dockerBin,
    "exec",
    JOBMANAGER_CONTAINER,
    "/opt/flink-cdc/bin/flink-cdc.sh",
    remotePath,
  ].join(" ")

  execSync(execCmd, { stdio: "pipe" })
}

/**
 * Substitute `${env:VAR}` tokens with the matching host env-var value.
 * Throws when a placeholder has no corresponding env var so deploys fail
 * loudly instead of silently passing an empty password.
 */
function resolveEnvPlaceholders(text: string): string {
  return text.replace(/\$\{env:([A-Z_][A-Z0-9_]*)\}/g, (_match, name) => {
    const value = process.env[name]
    if (value === undefined) {
      throw new Error(
        `pipeline.yaml references \${env:${name}} but ${name} is not set in the host environment`,
      )
    }
    return value
  })
}

function quoteShell(value: string): string {
  // Single-quote and escape any single-quote inside the value. Adequate
  // for paths and password values; we never pass shell metacharacters
  // here intentionally.
  return `'${value.replace(/'/g, `'"'"'`)}'`
}
