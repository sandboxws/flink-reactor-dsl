import { execSync, fork } from "node:child_process"
import {
  createWriteStream,
  existsSync,
  readdirSync,
  readFileSync,
  unlinkSync,
  writeFileSync,
} from "node:fs"
import { tmpdir } from "node:os"
import { dirname, join, resolve } from "node:path"
import { pipeline } from "node:stream/promises"
import { fileURLToPath } from "node:url"
import * as clack from "@clack/prompts"
import type { Command } from "commander"
import { Effect } from "effect"
import pc from "picocolors"
import { runCommand } from "@/cli/effect-runner.js"
import { CliError } from "@/core/errors.js"
import type { CdcDomain } from "@/cli/cluster/cdc-publisher.js"

// ── Resource path resolution ─────────────────────────────────────────
// When bundled by tsup → dist/index.js, __dirname = <root>/dist
// When running from source, __dirname = <root>/src/cli/commands
// In both cases we need to find <root>/src/cli/cluster/

const __filename = fileURLToPath(import.meta.url)
const __dirname = dirname(__filename)

function clusterDir(): string {
  // Bundled: dist/ → ../src/cli/cluster
  const fromDist = join(__dirname, "..", "src", "cli", "cluster")
  if (existsSync(join(fromDist, "docker-compose.yml"))) {
    return fromDist
  }
  // Source: src/cli/commands/ → ../cluster
  return join(__dirname, "..", "cluster")
}

function composePath(): string {
  return join(clusterDir(), "docker-compose.yml")
}

function pipelinesDir(): string {
  return join(clusterDir(), "pipelines")
}

function pidFilePath(): string {
  return join(tmpdir(), "flink-reactor-cdc-publisher.pid")
}

// ── Command registration ─────────────────────────────────────────────

export function registerClusterCommand(program: Command): void {
  const cluster = program
    .command("cluster")
    .description("Manage local Flink dev cluster for dashboard development")

  cluster
    .command("up")
    .description(
      "Start local Flink cluster (JM + 2×TM + SQL Gateway + Kafka + PostgreSQL)",
    )
    .option("--port <port>", "Flink REST port", "8081")
    .option("--seed", "Submit example pipelines after startup")
    .option(
      "--domain <domain>",
      "Filter seed data by domain: ecommerce, iot, or all",
      "all",
    )
    .option("--no-timescaledb", "Use plain PostgreSQL instead of TimescaleDB")
    .action(async (opts: { port: string; seed?: boolean; domain: string; timescaledb: boolean }) => {
      await runCommand(
        Effect.tryPromise({
          try: () =>
            runClusterUp({
              port: opts.port,
              seed: opts.seed ?? false,
              domain: opts.domain as CdcDomain,
              timescaledb: opts.timescaledb,
            }),
          catch: (err) =>
            new CliError({ reason: "invalid_args", message: (err as Error).message }),
        }),
      )
    })

  cluster
    .command("down")
    .description("Stop local Flink cluster")
    .option("--volumes", "Remove Docker volumes (checkpoint and state data)")
    .action(async (opts: { volumes?: boolean }) => {
      await runCommand(
        Effect.tryPromise({
          try: () => runClusterDown({ volumes: opts.volumes ?? false }),
          catch: (err) =>
            new CliError({ reason: "invalid_args", message: (err as Error).message }),
        }),
      )
    })

  cluster
    .command("seed")
    .description("Submit example SQL pipelines and publish CDC data")
    .option(
      "--only <category>",
      "Filter by category: streaming, batch, cdc, or cdc-kafka",
    )
    .option(
      "--domain <domain>",
      "Filter by domain: ecommerce, iot, or all",
      "all",
    )
    .action(async (opts: { only?: string; domain: string }) => {
      await runCommand(
        Effect.tryPromise({
          try: () =>
            runClusterSeed({
              only: opts.only as SeedCategory | undefined,
              domain: opts.domain as CdcDomain,
            }),
          catch: (err) =>
            new CliError({ reason: "invalid_args", message: (err as Error).message }),
        }),
      )
    })

  cluster
    .command("status")
    .description("Show cluster health, running jobs, and resource utilization")
    .option("--port <port>", "Flink REST port", "8081")
    .action(async (opts: { port: string }) => {
      await runCommand(
        Effect.tryPromise({
          try: () => runClusterStatus(parseInt(opts.port, 10)),
          catch: (err) =>
            new CliError({ reason: "invalid_args", message: (err as Error).message }),
        }),
      )
    })

  cluster
    .command("submit <sql-file>")
    .description("Submit a single SQL file via SQL Gateway")
    .option("--port <port>", "SQL Gateway port", "8083")
    .action(async (sqlFile: string, opts: { port: string }) => {
      await runCommand(
        Effect.tryPromise({
          try: () => runClusterSubmit(sqlFile, parseInt(opts.port, 10)),
          catch: (err) =>
            new CliError({ reason: "invalid_args", message: (err as Error).message }),
        }),
      )
    })
}

// ── Postgres profile resolution ──────────────────────────────────────

function resolvePostgresProfile(timescaledbFlag: boolean): string {
  // CLI flag takes precedence; if flag is true (default), check env var
  if (!timescaledbFlag) return "postgres-plain"
  const envVal = process.env.TIMESCALEDB_ENABLED?.toLowerCase()
  if (envVal === "false" || envVal === "0" || envVal === "no") return "postgres-plain"
  return "timescaledb"
}

// ── cluster up ───────────────────────────────────────────────────────

export async function runClusterUp(opts: {
  port: string
  seed: boolean
  domain?: CdcDomain
  timescaledb?: boolean
}): Promise<void> {
  clack.intro(pc.bgCyan(pc.black(" flink-reactor cluster up ")))

  // Pre-flight: Docker available?
  if (!dockerAvailable()) {
    console.error(pc.red("Docker is not installed or not running."))
    console.error(pc.dim("Install Docker: https://docs.docker.com/get-docker/"))
    process.exitCode = 1
    return
  }

  const compose = composePath()
  const dataDir = join(clusterDir(), "data")
  const pgProfile = resolvePostgresProfile(opts.timescaledb ?? true)

  // Copy sample data to a Docker-accessible volume path
  const spinner = clack.spinner()
  spinner.start(
    `Building Flink image and starting services (PostgreSQL: ${pgProfile === "timescaledb" ? "TimescaleDB" : "plain"})...`,
  )

  try {
    execSync(`docker compose -f "${compose}" --profile ${pgProfile} up --build -d`, {
      cwd: clusterDir(),
      stdio: "pipe",
      env: { ...process.env, FLINK_PORT: opts.port },
    })

    // Copy sample CSV into the flink-data volume via the jobmanager container
    try {
      execSync(
        `docker compose -f "${compose}" exec -T jobmanager mkdir -p /opt/flink/data`,
        { cwd: clusterDir(), stdio: "pipe" },
      )
      execSync(
        `docker compose -f "${compose}" cp "${join(dataDir, "sample-transactions.csv")}" jobmanager:/opt/flink/data/sample-transactions.csv`,
        { cwd: clusterDir(), stdio: "pipe" },
      )
    } catch {
      // Non-critical: filesystem batch job will fail but others work fine
    }

    spinner.stop(pc.green("Docker services started."))
  } catch (err) {
    spinner.stop(pc.red("Failed to start Docker services."))
    if (err instanceof Error) {
      console.error(pc.dim(err.message))
    }
    process.exitCode = 1
    return
  }

  // Wait for services to be healthy
  const { waitForServices } = await import("@/cli/cluster/health-check.js")
  try {
    await waitForServices({
      flinkPort: parseInt(opts.port, 10),
      sqlGatewayPort: 8083,
      kafkaPort: 9094,
      postgresPort: 5433,
    })
  } catch {
    console.error(pc.red("Services did not become ready in time."))
    console.error(pc.dim("Check docker logs: docker compose -f ... logs"))
    process.exitCode = 1
    return
  }

  // Initialize PostgreSQL databases (idempotent — skips if already exist)
  const pgService = pgProfile === "timescaledb" ? "postgres" : "postgres-plain"
  await initPostgresDatabases(compose, pgService)

  console.log("")
  console.log(
    `  ${pc.green("✓")} Flink UI:     ${pc.dim(`http://localhost:${opts.port}`)}`,
  )
  console.log(
    `  ${pc.green("✓")} SQL Gateway:  ${pc.dim("http://localhost:8083")}`,
  )
  console.log(`  ${pc.green("✓")} Kafka:        ${pc.dim("localhost:9094")}`)
  console.log(
    `  ${pc.green("✓")} PostgreSQL:   ${pc.dim("localhost:5433 (pagila, chinook, employees)")}`,
  )
  console.log("")

  if (opts.seed) {
    await seedPipelines({ domain: opts.domain })
  }

  clack.outro(pc.green("Cluster is ready!"))
}

// ── cluster down ─────────────────────────────────────────────────────

export async function runClusterDown(opts: {
  volumes: boolean
}): Promise<void> {
  clack.intro(pc.bgCyan(pc.black(" flink-reactor cluster down ")))

  // Kill background CDC publisher if running
  killCdcPublisher()

  const compose = composePath()
  const args = ["compose", "-f", compose, "--profile", "timescaledb", "--profile", "postgres-plain", "down"]
  if (opts.volumes) {
    args.push("-v")
  }

  const spinner = clack.spinner()
  spinner.start("Stopping cluster...")

  try {
    execSync(`docker ${args.join(" ")}`, {
      cwd: clusterDir(),
      stdio: "pipe",
    })
    spinner.stop(
      pc.green(
        opts.volumes
          ? "Cluster stopped and volumes removed."
          : "Cluster stopped.",
      ),
    )
  } catch {
    spinner.stop(pc.yellow("Cluster may already be stopped."))
  }

  clack.outro("Done.")
}

// ── cluster seed ─────────────────────────────────────────────────────

type SeedCategory = "streaming" | "batch" | "cdc" | "cdc-kafka"

export async function runClusterSeed(opts: {
  only?: SeedCategory
  domain?: CdcDomain
}): Promise<void> {
  clack.intro(pc.bgCyan(pc.black(" flink-reactor cluster seed ")))

  const { isClusterRunning } = await import("@/cli/cluster/health-check.js")
  if (!(await isClusterRunning(8081))) {
    console.error(pc.red("Cluster is not running."))
    console.error(pc.dim("Start it first: flink-reactor cluster up"))
    process.exitCode = 1
    return
  }

  await seedPipelines(opts)
  clack.outro(pc.green("Seeding complete!"))
}

async function seedPipelines(opts: {
  only?: SeedCategory
  domain?: CdcDomain
}): Promise<void> {
  const { publishCdcMessages } = await import("@/cli/cluster/cdc-publisher.js")

  const category = opts.only
  const domain = opts.domain ?? "all"
  const publishCdc =
    !category ||
    category === "cdc" ||
    category === "cdc-kafka" ||
    category === "streaming"

  // Step 1: Publish CDC batch data (needed before CDC pipelines)
  if (publishCdc) {
    const spinner = clack.spinner()
    spinner.start("Publishing CDC seed data to Kafka...")
    try {
      await publishCdcMessages({
        mode: "batch",
        domain,
        composeFile: composePath(),
      })
      spinner.stop(pc.green("CDC seed data published."))
    } catch (err) {
      spinner.stop(
        pc.yellow("Failed to publish CDC data (Kafka may not be ready)."),
      )
      if (err instanceof Error) {
        console.log(pc.dim(`  ${err.message}`))
      }
    }
  }

  // Step 2: Submit SQL pipelines (skip for cdc-kafka)
  if (category !== "cdc-kafka") {
    const { SqlGatewayClient } = await import(
      "@/cli/cluster/sql-gateway-client.js"
    )
    const client = new SqlGatewayClient("http://localhost:8083")

    const categories = category
      ? [category === "cdc" ? "streaming" : category]
      : ["streaming", "batch"]

    let submitted = 0
    let failed = 0

    for (const cat of categories) {
      const dir = join(pipelinesDir(), cat)
      if (!existsSync(dir)) continue

      const files = readdirSync(dir)
        .filter((f) => f.endsWith(".sql"))
        .sort()

      // When --only cdc, only submit Kafka-based pipelines
      let filtered =
        category === "cdc" ? files.filter((f) => f.includes("kafka")) : files

      // Apply domain filter: iot- prefixed files → iot domain, others → ecommerce
      if (domain !== "all") {
        filtered = filtered.filter((f) => {
          const name = f.replace(/^\d+-/, "") // Strip numeric prefix
          const isIoT = name.startsWith("iot-")
          return domain === "iot" ? isIoT : !isIoT
        })
      }

      for (const file of filtered) {
        const filePath = join(dir, file)
        const name = file.replace(".sql", "")
        const spinner = clack.spinner()
        spinner.start(`Submitting ${pc.dim(`${cat}/${name}`)}...`)

        try {
          const result = await client.submitSqlFile(filePath)
          if (result.status === "ERROR") {
            const errMsg = result.errorMessage ?? "Unknown error"
            spinner.stop(`  ${pc.red("✗")} ${cat}/${name} → ${pc.dim(errMsg)}`)
            failed++
          } else {
            spinner.stop(
              `  ${pc.green("✓")} ${cat}/${name} → ${pc.dim(result.status)}`,
            )
            submitted++
          }
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err)
          spinner.stop(`  ${pc.red("✗")} ${cat}/${name} → ${pc.dim(msg)}`)
          failed++
        }
      }
    }

    console.log("")
    console.log(
      `  Submitted: ${pc.green(String(submitted))}  Failed: ${failed > 0 ? pc.red(String(failed)) : pc.dim("0")}`,
    )
  }

  // Step 3: Start continuous CDC publisher in background
  if (publishCdc) {
    startBackgroundCdcPublisher(domain)
  }
}

// ── cluster status ───────────────────────────────────────────────────

export async function runClusterStatus(
  flinkPort: number = 8081,
): Promise<void> {
  const { isClusterRunning } = await import("@/cli/cluster/health-check.js")

  if (!(await isClusterRunning(flinkPort))) {
    console.log(pc.yellow("Cluster is not reachable."))
    console.log(pc.dim(`Tried http://localhost:${flinkPort}/overview`))
    return
  }

  try {
    const overviewRes = await fetch(`http://localhost:${flinkPort}/overview`)
    const overview = (await overviewRes.json()) as {
      taskmanagers: number
      "slots-total": number
      "slots-available": number
      "jobs-running": number
      "jobs-finished": number
      "jobs-cancelled": number
      "jobs-failed": number
    }

    console.log(pc.bold("\n  Cluster Overview\n"))
    console.log(`  TaskManagers:    ${pc.green(String(overview.taskmanagers))}`)
    console.log(
      `  Slots (total):   ${pc.green(String(overview["slots-total"]))}`,
    )
    console.log(
      `  Slots (free):    ${pc.green(String(overview["slots-available"]))}`,
    )
    console.log("")
    console.log(
      `  Jobs running:    ${pc.green(String(overview["jobs-running"]))}`,
    )
    console.log(
      `  Jobs finished:   ${pc.dim(String(overview["jobs-finished"]))}`,
    )
    console.log(
      `  Jobs cancelled:  ${pc.dim(String(overview["jobs-cancelled"]))}`,
    )
    console.log(
      `  Jobs failed:     ${overview["jobs-failed"] > 0 ? pc.red(String(overview["jobs-failed"])) : pc.dim("0")}`,
    )

    // Fetch job list
    const jobsRes = await fetch(`http://localhost:${flinkPort}/jobs/overview`)
    const jobsData = (await jobsRes.json()) as {
      jobs: Array<{
        jid: string
        name: string
        state: string
        "start-time": number
        duration: number
      }>
    }

    if (jobsData.jobs.length > 0) {
      console.log(pc.bold("\n  Jobs\n"))
      console.log(
        `  ${pc.dim(padRight("Name", 40))} ${pc.dim(padRight("State", 12))} ${pc.dim("Duration")}`,
      )
      console.log(`  ${pc.dim("─".repeat(65))}`)

      for (const job of jobsData.jobs) {
        const stateColor =
          job.state === "RUNNING"
            ? pc.green
            : job.state === "FINISHED"
              ? pc.dim
              : job.state === "FAILED"
                ? pc.red
                : pc.yellow

        console.log(
          `  ${padRight(job.name, 40)} ${stateColor(padRight(job.state, 12))} ${pc.dim(formatDuration(job.duration))}`,
        )
      }
    }

    console.log("")
  } catch (err) {
    console.error(pc.red("Failed to fetch cluster status."))
    if (err instanceof Error) {
      console.error(pc.dim(err.message))
    }
  }
}

// ── cluster submit ───────────────────────────────────────────────────

export async function runClusterSubmit(
  sqlFile: string,
  sqlGatewayPort: number = 8083,
): Promise<void> {
  const filePath = resolve(sqlFile)

  if (!existsSync(filePath)) {
    console.error(pc.red(`File not found: ${sqlFile}`))
    process.exitCode = 1
    return
  }

  const { isClusterRunning } = await import("@/cli/cluster/health-check.js")
  if (!(await isClusterRunning(8081))) {
    console.error(pc.red("Cluster is not running."))
    console.error(pc.dim("Start it first: flink-reactor cluster up"))
    process.exitCode = 1
    return
  }

  const { SqlGatewayClient } = await import(
    "@/cli/cluster/sql-gateway-client.js"
  )
  const client = new SqlGatewayClient(`http://localhost:${sqlGatewayPort}`)

  const spinner = clack.spinner()
  spinner.start(`Submitting ${pc.dim(sqlFile)}...`)

  try {
    const result = await client.submitSqlFile(filePath)

    if (result.status === "ERROR") {
      spinner.stop(pc.red("Statement failed."))
      console.log("")
      console.log(`  Status:    ${pc.red(result.status)}`)
      console.log(`  Session:   ${pc.dim(result.sessionHandle)}`)
      console.log(`  Operation: ${pc.dim(result.operationHandle)}`)
      if (result.errorMessage) {
        console.log("")
        console.log(`  ${pc.red("Error:")} ${result.errorMessage}`)
      }
      console.log("")
      process.exitCode = 1
    } else {
      spinner.stop(pc.green("Statement submitted."))
      console.log("")
      console.log(`  Status:    ${pc.green(result.status)}`)
      console.log(`  Session:   ${pc.dim(result.sessionHandle)}`)
      console.log(`  Operation: ${pc.dim(result.operationHandle)}`)
      console.log("")
    }
  } catch (err) {
    spinner.stop(pc.red("Submission failed."))
    if (err instanceof Error) {
      console.error(pc.dim(err.message))
    }
    process.exitCode = 1
  }
}

// ── Background CDC publisher ─────────────────────────────────────────

function startBackgroundCdcPublisher(domain: CdcDomain = "all"): void {
  // Write a small inline script that imports and runs the continuous publisher
  // Fork it as a detached child process
  const scriptPath = join(tmpdir(), "flink-reactor-cdc-continuous.mjs")
  const cdcModulePath = join(clusterDir(), "cdc-publisher.js")

  const compose = composePath()
  writeFileSync(
    scriptPath,
    `
import { publishCdcMessages } from '${cdcModulePath}';
const ac = new AbortController();
process.on('SIGTERM', () => ac.abort());
process.on('SIGINT', () => ac.abort());
publishCdcMessages({ mode: 'continuous', domain: '${domain}', composeFile: '${compose}', signal: ac.signal }).catch(() => {});
`,
    "utf-8",
  )

  try {
    const child = fork(scriptPath, [], {
      detached: true,
      stdio: "ignore",
    })
    child.unref()

    if (child.pid) {
      writeFileSync(pidFilePath(), String(child.pid), "utf-8")
      console.log(
        `  ${pc.green("✓")} CDC publisher running ${pc.dim(`(PID ${child.pid})`)}`,
      )
    }
  } catch {
    console.log(pc.dim("  Could not start background CDC publisher."))
  }
}

function killCdcPublisher(): void {
  const pidFile = pidFilePath()
  if (!existsSync(pidFile)) return

  try {
    const pid = parseInt(readFileSync(pidFile, "utf-8").trim(), 10)
    process.kill(pid, "SIGTERM")
    console.log(pc.dim(`  Stopped CDC publisher (PID ${pid})`))
  } catch {
    // Process may already be dead
  }

  try {
    unlinkSync(pidFile)
  } catch {
    // Best-effort cleanup
  }
}

// ── PostgreSQL initialization ────────────────────────────────────────

const SAMPLE_DATABASES = ["pagila", "chinook", "employees"] as const

/** Schema to check for table counts — employees uses a custom schema */
const DB_SCHEMA: Record<string, string> = {
  pagila: "public",
  chinook: "public",
  employees: "employees",
}

async function initPostgresDatabases(compose: string, pgService: string = "postgres"): Promise<void> {
  const initDir = join(clusterDir(), "init")

  // Ensure SQL dumps are downloaded before loading
  await ensureSqlDumps(initDir)

  const spinner = clack.spinner()
  spinner.start("Initializing PostgreSQL databases...")

  const cwd = clusterDir()
  const psql = (db: string, sql: string) =>
    execSync(
      `docker compose -f "${compose}" exec -T ${pgService} psql -U reactor -d ${db} -c ${JSON.stringify(sql)}`,
      { cwd, stdio: "pipe" },
    )

  try {
    // Create databases if they don't exist
    for (const db of SAMPLE_DATABASES) {
      try {
        psql("postgres", `CREATE DATABASE ${db}`)
      } catch {
        // Already exists — fine
      }
    }

    // Load data only if tables don't exist yet
    for (const db of SAMPLE_DATABASES) {
      const schema = DB_SCHEMA[db]
      const result = execSync(
        `docker compose -f "${compose}" exec -T ${pgService} psql -U reactor -d ${db} -tAc "SELECT count(*) FROM information_schema.tables WHERE table_schema = '${schema}'"`,
        { cwd, stdio: "pipe" },
      )
      const tableCount = parseInt(result.toString().trim(), 10)

      if (tableCount === 0) {
        const sqlFile = join(initDir, `${db}.sql`)
        if (!existsSync(sqlFile)) {
          spinner.message(`Skipping ${db} (dump not found)...`)
          continue
        }
        execSync(
          `docker compose -f "${compose}" cp "${sqlFile}" ${pgService}:/tmp/${db}.sql`,
          { cwd, stdio: "pipe" },
        )
        execSync(
          `docker compose -f "${compose}" exec -T ${pgService} psql -v ON_ERROR_STOP=1 -U reactor -d ${db} -f /tmp/${db}.sql`,
          { cwd, stdio: "pipe", timeout: 120_000 },
        )
        spinner.message(`Loaded ${db} dataset...`)
      }
    }

    spinner.stop(pc.green("PostgreSQL databases ready."))
  } catch (err) {
    spinner.stop(pc.yellow("PostgreSQL initialization failed (non-critical)."))
    if (err instanceof Error) {
      console.log(pc.dim(`  ${err.message}`))
    }
  }
}

// ── SQL dump download ────────────────────────────────────────────────

interface DumpSource {
  /** Files to download and concatenate (in order) */
  urls: string[]
  /** If true, the last URL is bzip2-compressed and needs decompression */
  bzip2Last?: boolean
  /** Lines matching this pattern are removed (PG compat fixes) */
  removeLine?: RegExp
  /** SQL prepended before the dump (e.g. to disable FK checks) */
  preamble?: string
  /** SQL appended after the dump */
  epilogue?: string
}

const DUMP_SOURCES: Record<string, DumpSource> = {
  pagila: {
    urls: [
      "https://raw.githubusercontent.com/devrimgunduz/pagila/master/pagila-schema.sql",
      "https://raw.githubusercontent.com/devrimgunduz/pagila/master/pagila-data.sql",
    ],
    // The upstream dump has OWNER TO postgres / GRANT / REVOKE statements
    // that fail because our Docker PG runs as user "reactor"
    removeLine: /\bOWNER TO\b|\bGRANT\b|\bREVOKE\b/,
    // Data insert order doesn't respect FK dependencies — disable checks
    preamble: "SET session_replication_role = 'replica';",
    epilogue: "SET session_replication_role = 'origin';",
  },
  chinook: {
    urls: [
      "https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_PostgreSql.sql",
    ],
  },
  employees: {
    urls: [
      "https://raw.githubusercontent.com/h8/employees-database/master/employees_schema.sql",
      "https://raw.githubusercontent.com/h8/employees-database/master/employees_data.sql.bz2",
    ],
    bzip2Last: true,
    removeLine: /SET default_with_oids/,
  },
}

async function ensureSqlDumps(initDir: string): Promise<void> {
  const missing = SAMPLE_DATABASES.filter(
    (db) => !existsSync(join(initDir, `${db}.sql`)),
  )
  if (missing.length === 0) return

  const spinner = clack.spinner()
  spinner.start(`Downloading sample databases: ${missing.join(", ")}...`)

  for (const db of missing) {
    const source = DUMP_SOURCES[db]
    const outPath = join(initDir, `${db}.sql`)

    try {
      spinner.message(`Downloading ${db}...`)
      await downloadDump(source, outPath)
    } catch (err) {
      spinner.stop(pc.yellow(`Failed to download ${db} (non-critical).`))
      if (err instanceof Error) {
        console.log(pc.dim(`  ${err.message}`))
      }
      console.log(
        pc.dim(`  You can manually place the SQL dump at: ${outPath}`),
      )
      return
    }
  }

  spinner.stop(pc.green("Sample database dumps ready."))
}

async function downloadDump(
  source: DumpSource,
  outPath: string,
): Promise<void> {
  const parts: string[] = []

  for (let i = 0; i < source.urls.length; i++) {
    const url = source.urls[i]
    const isBz2 = source.bzip2Last && i === source.urls.length - 1

    if (isBz2) {
      // Download bz2 to temp file, decompress with bunzip2
      const tmpBz2 = `${outPath}.bz2`
      const tmpDecompressed = `${outPath}.tmp`
      try {
        const response = await fetch(url)
        if (!response.ok || !response.body) {
          throw new Error(`HTTP ${response.status} fetching ${url}`)
        }
        const ws = createWriteStream(tmpBz2)
        await pipeline(response.body as never, ws)
        execSync(`bunzip2 -c "${tmpBz2}" > "${tmpDecompressed}"`, {
          stdio: "pipe",
        })
        parts.push(readFileSync(tmpDecompressed, "utf-8"))
      } finally {
        try {
          unlinkSync(tmpBz2)
        } catch {
          /* ignore */
        }
        try {
          unlinkSync(`${outPath}.tmp`)
        } catch {
          /* ignore */
        }
      }
    } else {
      const response = await fetch(url)
      if (!response.ok) {
        throw new Error(`HTTP ${response.status} fetching ${url}`)
      }
      parts.push(await response.text())
    }
  }

  let content = parts.join("\n")

  // Apply line-level fixes
  if (source.removeLine) {
    content = content
      .split("\n")
      .filter((line) => !source.removeLine!.test(line))
      .join("\n")
  }

  // Wrap with preamble/epilogue (e.g. disable FK checks during load)
  if (source.preamble) {
    content = source.preamble + "\n" + content
  }
  if (source.epilogue) {
    content = content + "\n" + source.epilogue + "\n"
  }

  writeFileSync(outPath, content, "utf-8")
}

// ── Utilities ────────────────────────────────────────────────────────

function dockerAvailable(): boolean {
  try {
    execSync("docker info", { stdio: "pipe" })
    return true
  } catch {
    return false
  }
}

function padRight(str: string, len: number): string {
  return str.length >= len
    ? str.substring(0, len)
    : str + " ".repeat(len - str.length)
}

function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`
  const seconds = Math.floor(ms / 1000) % 60
  const minutes = Math.floor(ms / 60_000) % 60
  const hours = Math.floor(ms / 3_600_000)
  if (hours > 0) return `${hours}h ${minutes}m`
  if (minutes > 0) return `${minutes}m ${seconds}s`
  return `${seconds}s`
}
