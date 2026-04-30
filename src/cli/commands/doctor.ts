import { execSync } from "node:child_process"
import { existsSync, readdirSync, statSync } from "node:fs"
import { join } from "node:path"
import type { Command } from "commander"
import { Effect } from "effect"
import pc from "picocolors"
import { runCommand as runEffectAction } from "@/cli/effect-runner.js"
import { CliError } from "@/core/errors.js"

export interface CheckResult {
  name: string
  status: "pass" | "fail" | "warn"
  version?: string
  message: string
  hint?: string
}

export function registerDoctorCommand(program: Command): void {
  program
    .command("doctor")
    .description("Check your development environment")
    .action(async () => {
      await runEffectAction(
        Effect.tryPromise({
          try: () => runDoctorCommand(),
          catch: (err) =>
            new CliError({
              reason: "invalid_args",
              message: (err as Error).message,
            }),
        }),
      )
    })
}

export async function runDoctorCommand(cwd?: string): Promise<CheckResult[]> {
  const projectDir = cwd ?? process.cwd()
  const results: CheckResult[] = []

  results.push(checkNodeVersion())
  results.push(checkTypeScriptVersion())
  results.push(checkDockerVersion())
  results.push(checkFlinkInstallation())
  results.push(checkKubectl())
  results.push(checkProjectConfig(projectDir))
  results.push(discoverPipelines(projectDir))
  results.push(...checkPostgresCdcReadiness())
  results.push(...(await checkFlussReachability(projectDir)))

  printResults(results)
  return results
}

function runShellCommand(cmd: string): string | null {
  try {
    return execSync(cmd, {
      encoding: "utf-8",
      stdio: ["pipe", "pipe", "pipe"],
    }).trim()
  } catch {
    return null
  }
}

export function checkNodeVersion(): CheckResult {
  const output = runShellCommand("node --version")
  if (!output) {
    return {
      name: "Node.js",
      status: "fail",
      message: "Not found",
      hint: "Install Node.js 18+ from https://nodejs.org",
    }
  }
  const version = output.replace(/^v/, "")
  const major = parseInt(version.split(".")[0], 10)
  if (major < 18) {
    return {
      name: "Node.js",
      status: "warn",
      version,
      message: `v${version} (18+ recommended)`,
      hint: "Upgrade to Node.js 18+ for full compatibility",
    }
  }
  return { name: "Node.js", status: "pass", version, message: `v${version}` }
}

export function checkTypeScriptVersion(): CheckResult {
  const output = runShellCommand("npx tsc --version")
  if (!output) {
    return {
      name: "TypeScript",
      status: "fail",
      message: "Not found",
      hint: "Install TypeScript: npm install -g typescript",
    }
  }
  const version = output.replace(/^Version\s+/, "")
  return { name: "TypeScript", status: "pass", version, message: `v${version}` }
}

export function checkDockerVersion(): CheckResult {
  const output = runShellCommand("docker --version")
  if (!output) {
    return {
      name: "Docker",
      status: "warn",
      message: "Not found",
      hint: "Install Docker from https://docs.docker.com/get-docker/ (needed for local Flink)",
    }
  }
  const match = output.match(/Docker version\s+([\d.]+)/)
  const version = match?.[1] ?? output
  return { name: "Docker", status: "pass", version, message: `v${version}` }
}

export function checkFlinkInstallation(): CheckResult {
  // Check for local Flink binary
  const flinkOutput = runShellCommand("flink --version")
  if (flinkOutput) {
    const match = flinkOutput.match(/Version:\s*([\d.]+)/)
    const version = match?.[1] ?? flinkOutput
    return {
      name: "Apache Flink",
      status: "pass",
      version,
      message: `v${version} (local)`,
    }
  }

  // Check for Flink Docker image
  const dockerOutput = runShellCommand(
    'docker images --format "{{.Repository}}:{{.Tag}}" | grep -i flink | head -1',
  )
  if (dockerOutput) {
    return {
      name: "Apache Flink",
      status: "pass",
      message: `Docker: ${dockerOutput}`,
    }
  }

  return {
    name: "Apache Flink",
    status: "warn",
    message: "Not found",
    hint: 'Run "flink-reactor install flink" to set up a local Flink environment',
  }
}

export function checkKubectl(): CheckResult {
  const output = runShellCommand(
    "kubectl version --client --short 2>/dev/null || kubectl version --client 2>/dev/null",
  )
  if (!output) {
    return {
      name: "kubectl",
      status: "warn",
      message: "Not found",
      hint: "Install kubectl: https://kubernetes.io/docs/tasks/tools/ (needed for deployment)",
    }
  }
  const match = output.match(/v([\d.]+)/)
  const version = match?.[1] ?? output.slice(0, 40)
  return { name: "kubectl", status: "pass", version, message: `v${version}` }
}

export function checkProjectConfig(projectDir: string): CheckResult {
  const configPath = join(projectDir, "flink-reactor.config.ts")
  if (existsSync(configPath)) {
    return {
      name: "Project config",
      status: "pass",
      message: "flink-reactor.config.ts found",
    }
  }
  return {
    name: "Project config",
    status: "warn",
    message: "No flink-reactor.config.ts found",
    hint: 'Run "flink-reactor new" to create a project, or create flink-reactor.config.ts manually',
  }
}

export function discoverPipelines(projectDir: string): CheckResult {
  const pipelinesDir = join(projectDir, "pipelines")
  if (!existsSync(pipelinesDir)) {
    return {
      name: "Pipelines",
      status: "warn",
      message: "No pipelines/ directory",
      hint: 'Run "flink-reactor generate pipeline <name>"',
    }
  }

  const count = countPipelines(pipelinesDir)
  if (count === 0) {
    return {
      name: "Pipelines",
      status: "warn",
      message: "0 pipelines discovered",
      hint: 'Run "flink-reactor generate pipeline <name>"',
    }
  }

  return {
    name: "Pipelines",
    status: "pass",
    message: `${count} pipeline${count !== 1 ? "s" : ""} discovered`,
  }
}

/**
 * Probes a locally-reachable Postgres for CDC readiness:
 *   - wal_level=logical
 *   - flink_cdc role with REPLICATION + LOGIN
 *   - flink_cdc_pub publication
 *
 * Returns an empty array when `psql` is not on PATH (the developer can't
 * have a Postgres without it, and we'd rather skip silently than report a
 * fail for an unrelated reason). When postgres itself is unreachable on
 * the default localhost:5433, returns three "warn" results pointing at
 * `fr cluster up` — the canonical setup path.
 *
 * Wired up by OpenSpec change 53-prepare-postgres-for-cdc.
 */
export function checkPostgresCdcReadiness(): CheckResult[] {
  const psqlAvailable = runShellCommand("psql --version") !== null
  if (!psqlAvailable) return []

  const baseEnv =
    "PGHOST=${PGHOST:-localhost} PGPORT=${PGPORT:-5433} PGUSER=${PGUSER:-reactor} PGPASSWORD=${PGPASSWORD:-reactor}"
  const probe = (db: string, sql: string): string | null =>
    runShellCommand(
      `${baseEnv} psql -d ${db} -tAc "${sql.replace(/"/g, '\\"')}" 2>/dev/null`,
    )

  const reachable = probe("postgres", "SELECT 1") === "1"
  if (!reachable) {
    const hint = "Postgres unreachable on localhost:5433 — run `fr cluster up`"
    return [
      { name: "Postgres wal_level", status: "warn", message: "Skipped", hint },
      {
        name: "Postgres flink_cdc role",
        status: "warn",
        message: "Skipped",
        hint,
      },
      {
        name: "Postgres flink_cdc_pub",
        status: "warn",
        message: "Skipped",
        hint,
      },
    ]
  }

  const results: CheckResult[] = []

  // 1. wal_level
  const wal = probe("postgres", "SHOW wal_level")
  results.push(
    wal === "logical"
      ? {
          name: "Postgres wal_level",
          status: "pass",
          message: "logical",
        }
      : {
          name: "Postgres wal_level",
          status: "warn",
          message: `${wal ?? "unknown"} (CDC requires 'logical')`,
          hint: "Restart postgres with -c wal_level=logical (or re-run `fr cluster up`)",
        },
  )

  // 2. flink_cdc role with REPLICATION + LOGIN
  const role = probe(
    "postgres",
    "SELECT 1 FROM pg_roles WHERE rolname='flink_cdc' AND rolcanlogin AND rolreplication",
  )
  results.push(
    role === "1"
      ? {
          name: "Postgres flink_cdc role",
          status: "pass",
          message: "exists with REPLICATION + LOGIN",
        }
      : {
          name: "Postgres flink_cdc role",
          status: "warn",
          message: "missing or lacks REPLICATION/LOGIN",
          hint: "Re-run `fr cluster up` to apply pg-cdc-bootstrap.sql",
        },
  )

  // 3. flink_cdc_pub publication on tpch
  const pub = probe(
    "tpch",
    "SELECT 1 FROM pg_publication WHERE pubname='flink_cdc_pub'",
  )
  results.push(
    pub === "1"
      ? {
          name: "Postgres flink_cdc_pub",
          status: "pass",
          message: "publication present on tpch",
        }
      : {
          name: "Postgres flink_cdc_pub",
          status: "warn",
          message: "publication missing on tpch",
          hint: "Re-run `fr cluster up` to apply pg-cdc-bootstrap.sql",
        },
  )

  return results
}

/**
 * Probes the Docker-lane Fluss CoordinatorServer on `localhost:9123` when
 * the project's resolved runtime is `docker` and `sim.init.fluss` is set.
 *
 * Wired up by OpenSpec change 52-add-fluss-docker-compose-services. The
 * minikube-lane probe is a separate path (kubectl-based) handled
 * elsewhere in the doctor structure; this check returns an empty array
 * for non-docker runtimes so the two probes don't double-report.
 */
export async function checkFlussReachability(
  projectDir: string,
): Promise<CheckResult[]> {
  const { loadConfig } = await import("@/cli/discovery.js")
  const config = await loadConfig(projectDir).catch(() => null)
  if (!config?.environments) return []

  // Find the first env with sim.init.fluss declared. Mirrors cluster.ts's
  // `development > docker > minikube > first` ordering — the resolved env
  // for the docker lane is the one whose runtime is (or defaults to) docker.
  const envs = Object.entries(config.environments)
  const flussEnv =
    envs.find(([_, e]) => e.runtime === "docker" && e.sim?.init?.fluss) ??
    envs.find(
      ([n, e]) => (n === "development" || !e.runtime) && e.sim?.init?.fluss,
    )

  if (!flussEnv) return []

  const port = 9123
  const ok = runShellCommand(`nc -z localhost ${port}`) !== null
  if (ok) {
    return [
      {
        name: "Fluss coordinator",
        status: "pass",
        message: `reachable on localhost:${port}`,
      },
    ]
  }
  return [
    {
      name: "Fluss coordinator",
      status: "warn",
      message: `Fluss coordinator unreachable on localhost:${port} — run \`fr cluster up\``,
      hint: "Run `fr cluster up` to start the Docker-lane Fluss services",
    },
  ]
}

function countPipelines(pipelinesDir: string): number {
  let count = 0
  try {
    const entries = readdirSync(pipelinesDir)
    for (const entry of entries) {
      const entryPath = join(pipelinesDir, entry)
      if (statSync(entryPath).isDirectory()) {
        const indexPath = join(entryPath, "index.tsx")
        if (existsSync(indexPath)) {
          count++
        }
      }
    }
  } catch {
    // ignore read errors
  }
  return count
}

function printResults(results: CheckResult[]): void {
  console.log("")
  console.log(pc.bold("FlinkReactor Doctor"))
  console.log(pc.dim("─".repeat(40)))
  console.log("")

  for (const result of results) {
    const icon =
      result.status === "pass"
        ? pc.green("✓")
        : result.status === "warn"
          ? pc.yellow("!")
          : pc.red("✗")

    console.log(`  ${icon} ${pc.bold(result.name)}: ${result.message}`)
    if (result.hint) {
      console.log(`    ${pc.dim(result.hint)}`)
    }
  }

  console.log("")

  const failures = results.filter((r) => r.status === "fail")
  if (failures.length > 0) {
    console.log(pc.red(`${failures.length} check(s) failed. See hints above.`))
    process.exitCode = 1
  } else {
    const warnings = results.filter((r) => r.status === "warn")
    if (warnings.length > 0) {
      console.log(
        pc.yellow(`All checks passed with ${warnings.length} warning(s).`),
      )
    } else {
      console.log(pc.green("All checks passed!"))
    }
  }
}
