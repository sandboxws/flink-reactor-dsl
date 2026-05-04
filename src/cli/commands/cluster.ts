import { execFileSync, execSync, fork } from "node:child_process"
import {
  existsSync,
  readdirSync,
  readFileSync,
  unlinkSync,
  writeFileSync,
} from "node:fs"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"
import * as clack from "@clack/prompts"
import { type Command, Option } from "commander"
import { Effect } from "effect"
import pc from "picocolors"
import type { CdcDomain } from "@/cli/cluster/cdc-publisher.js"
import { bundledComposePath, clusterDir } from "@/cli/cluster/paths.js"
import {
  DB_SCHEMA,
  ensureSqlDumps,
  SAMPLE_DATABASES,
} from "@/cli/cluster/pg-samples.js"
import { runCommand } from "@/cli/effect-runner.js"
import {
  type ContainerEngineChoice,
  ContainerEngineNotFoundError,
  detectContainerEngine,
  PodmanVersionTooOldError,
  type ResolvedEngine,
} from "@/cli/runtime/container-engine.js"
import {
  ALL_PROFILES,
  buildComposeEnv,
  type ComposeProfile,
  profilesFromConfig,
} from "@/cli/runtime/service-selection.js"
import type { ResolvedConfig } from "@/core/config-resolver.js"
import { CliError } from "@/core/errors.js"

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
    .description(
      "Docker-runtime shortcut (alias of `fr up --runtime=docker`). Provides Docker-specific extras: seed, submit, status.",
    )

  const engineOption = new Option(
    "--container-engine <name>",
    "Container engine override (auto | docker | podman)",
  ).choices(["auto", "docker", "podman"])

  cluster
    .command("up")
    .description(
      "Start local Flink cluster (JM + 2×TM + SQL Gateway + Kafka + PostgreSQL)",
    )
    .option("--port <port>", "Flink REST port", "8081")
    .option("--seed", "Submit example pipelines after startup")
    .addOption(
      new Option("--domain <domain>", "Filter seed data by domain")
        .choices(["ecommerce", "iot", "all"])
        .default("all"),
    )
    .option("--no-timescaledb", "Use plain PostgreSQL instead of TimescaleDB")
    .addOption(engineOption)
    .action(
      async (opts: {
        port: string
        seed?: boolean
        domain: string
        timescaledb: boolean
        containerEngine?: ContainerEngineChoice
      }) => {
        await runCommand(
          Effect.tryPromise({
            try: () =>
              runClusterUp({
                port: opts.port,
                seed: opts.seed ?? false,
                domain: opts.domain as CdcDomain,
                timescaledb: opts.timescaledb,
                containerEngine: opts.containerEngine,
              }),
            catch: (err) =>
              new CliError({
                reason: "invalid_args",
                message: (err as Error).message,
              }),
          }),
        )
      },
    )

  cluster
    .command("down")
    .description("Stop local Flink cluster")
    .option("--volumes", "Remove Docker volumes (checkpoint and state data)")
    .addOption(engineOption)
    .action(
      async (opts: {
        volumes?: boolean
        containerEngine?: ContainerEngineChoice
      }) => {
        await runCommand(
          Effect.tryPromise({
            try: () =>
              runClusterDown({
                volumes: opts.volumes ?? false,
                containerEngine: opts.containerEngine,
              }),
            catch: (err) =>
              new CliError({
                reason: "invalid_args",
                message: (err as Error).message,
              }),
          }),
        )
      },
    )

  cluster
    .command("seed")
    .description("Submit example SQL pipelines and publish CDC data")
    .addOption(
      new Option("--only <category>", "Filter by category").choices([
        "streaming",
        "batch",
        "cdc",
        "cdc-kafka",
      ]),
    )
    .addOption(
      new Option("--domain <domain>", "Filter by domain")
        .choices(["ecommerce", "iot", "all"])
        .default("all"),
    )
    .addOption(engineOption)
    .action(
      async (opts: {
        only?: string
        domain: string
        containerEngine?: ContainerEngineChoice
      }) => {
        await runCommand(
          Effect.tryPromise({
            try: () =>
              runClusterSeed({
                only: opts.only as SeedCategory | undefined,
                domain: opts.domain as CdcDomain,
                containerEngine: opts.containerEngine,
              }),
            catch: (err) =>
              new CliError({
                reason: "invalid_args",
                message: (err as Error).message,
              }),
          }),
        )
      },
    )

  cluster
    .command("status")
    .description("Show cluster health, running jobs, and resource utilization")
    .option("--port <port>", "Flink REST port", "8081")
    .action(async (opts: { port: string }) => {
      await runCommand(
        Effect.tryPromise({
          try: () => runClusterStatus(parseInt(opts.port, 10)),
          catch: (err) =>
            new CliError({
              reason: "invalid_args",
              message: (err as Error).message,
            }),
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
            new CliError({
              reason: "invalid_args",
              message: (err as Error).message,
            }),
        }),
      )
    })
}

// ── Engine + config resolution ───────────────────────────────────────

/**
 * Resolve the container engine *and* the project's resolved config in a
 * single pass. `cluster up` consumes both — the engine to run compose,
 * the resolved config to decide which compose profiles to activate
 * (driven by `services:` in flink-reactor.config.ts) and which init
 * helpers to run.
 *
 * Engine resolution honors all four inputs: --container-engine flag →
 * FR_CONTAINER_ENGINE env → config (`containerEngine` on the auto-
 * selected environment) → auto-detect (docker, then podman ≥ 4.7).
 *
 * `resolved` is `undefined` when no flink-reactor.config.ts is found or
 * the config is unparseable — callers treat that as "no services
 * declared" and fall back to no-op defaults.
 */
async function resolveClusterContext(opts: {
  containerEngine?: ContainerEngineChoice
}): Promise<{ engine: ResolvedEngine; resolved: ResolvedConfig | undefined }> {
  let resolved: ResolvedConfig | undefined
  try {
    const { loadConfig } = await import("@/cli/discovery.js")
    const { resolveConfig } = await import("@/core/config-resolver.js")
    const config = await loadConfig(process.cwd())
    if (config?.environments && Object.keys(config.environments).length > 0) {
      const envName = config.environments.development
        ? "development"
        : config.environments.docker
          ? "docker"
          : Object.keys(config.environments).sort()[0]
      if (envName) {
        resolved = resolveConfig(config, envName)
      }
    }
  } catch {
    // Config absent or unparseable — fall through.
  }
  const engine = await detectContainerEngine({
    flag: opts.containerEngine,
    configValue: resolved?.containerEngine,
  })
  return { engine, resolved }
}

function reportEngineError(err: unknown): void {
  if (
    err instanceof ContainerEngineNotFoundError ||
    err instanceof PodmanVersionTooOldError
  ) {
    console.error(pc.red(err.message))
    process.exitCode = 1
    return
  }
  throw err
}

// ── cluster up ───────────────────────────────────────────────────────

/**
 * Apply the `--no-timescaledb` flag (deprecated) on top of the
 * services-driven profile list. The flag wins over config because it's
 * an explicit user override; we emit a one-time warning so users migrate
 * to `services: { postgres: { flavor: 'plain' } }`.
 */
function applyTimescaledbFlag(
  profiles: ComposeProfile[],
  timescaledbFlag: boolean,
): ComposeProfile[] {
  if (timescaledbFlag) return profiles
  console.log(
    pc.yellow(
      "  Warning: --no-timescaledb is deprecated. " +
        "Set `services: { postgres: { flavor: 'plain' } }` in flink-reactor.config.ts.",
    ),
  )
  const out = profiles.filter((p) => p !== "timescaledb")
  if (!out.includes("postgres-plain")) out.push("postgres-plain")
  return out
}

export async function runClusterUp(opts: {
  port: string
  seed: boolean
  domain?: CdcDomain
  timescaledb?: boolean
  containerEngine?: ContainerEngineChoice
}): Promise<void> {
  clack.intro(pc.bgCyan(pc.black(" flink-reactor cluster up ")))

  // Pre-flight: resolve the container engine and project config in one pass.
  let engine: ResolvedEngine
  let resolved: ResolvedConfig | undefined
  try {
    ;({ engine, resolved } = await resolveClusterContext({
      containerEngine: opts.containerEngine,
    }))
  } catch (err) {
    reportEngineError(err)
    return
  }
  const engineSourceLabel =
    engine.source === "auto" ? "auto-detected" : `from ${engine.source}`
  const dockerHost = engine.composeEnv().DOCKER_HOST
  console.log(
    pc.dim(
      `  Container engine: ${engine.name} (${engineSourceLabel})${
        dockerHost ? ` · ${dockerHost}` : ""
      }`,
    ),
  )

  const compose = bundledComposePath()
  const dataDir = join(clusterDir(), "data")

  // Profiles come from `services:` in flink-reactor.config.ts. Empty
  // when no config or no services declared — only always-on services
  // (Flink core, SeaweedFS, SQL Gateway) start.
  let profiles: ComposeProfile[] = resolved ? profilesFromConfig(resolved) : []
  profiles = applyTimescaledbFlag(profiles, opts.timescaledb ?? true)

  const composeEnv = buildComposeEnv(resolved?.services)
  const profileArgs = profiles.flatMap((p) => ["--profile", p])
  const profileSummary =
    profiles.length > 0
      ? `profiles: ${profiles.join(", ")}`
      : "always-on services only"

  const spinner = clack.spinner()
  spinner.start(
    `Building Flink image and starting services (${profileSummary})...`,
  )

  try {
    execFileSync(
      engine.bin,
      [
        ...engine.composeArgv([
          "-f",
          compose,
          ...profileArgs,
          "up",
          "--build",
          "-d",
        ]),
      ],
      {
        cwd: clusterDir(),
        stdio: "pipe",
        env: {
          ...process.env,
          ...engine.composeEnv(),
          ...composeEnv,
          FLINK_PORT: opts.port,
          FR_CONTAINER_ENGINE: engine.name,
        },
      },
    )

    // Copy sample CSV into the flink-data volume via the jobmanager container
    try {
      const composeChildEnv = { ...process.env, ...engine.composeEnv() }
      execFileSync(
        engine.bin,
        [
          ...engine.composeArgv([
            "-f",
            compose,
            "exec",
            "-T",
            "jobmanager",
            "mkdir",
            "-p",
            "/opt/flink/data",
          ]),
        ],
        { cwd: clusterDir(), stdio: "pipe", env: composeChildEnv },
      )
      execFileSync(
        engine.bin,
        [
          ...engine.composeArgv([
            "-f",
            compose,
            "cp",
            join(dataDir, "sample-transactions.csv"),
            "jobmanager:/opt/flink/data/sample-transactions.csv",
          ]),
        ],
        { cwd: clusterDir(), stdio: "pipe", env: composeChildEnv },
      )
    } catch {
      // Non-critical: filesystem batch job will fail but others work fine
    }

    spinner.stop(pc.green(`${engine.name} services started.`))
  } catch (err) {
    spinner.stop(pc.red(`Failed to start ${engine.name} services.`))
    if (err instanceof Error) {
      console.error(pc.dim(err.message))
      // Container-name collisions across engines are a common gotcha when
      // switching between docker and podman: each engine maintains its own
      // state, but the named containers conflict.
      if (/already in use|name.*already|conflict/i.test(err.message)) {
        console.error(
          pc.dim(
            "Hint: if switching between docker and podman, run `fr cluster down` with the previous engine to remove orphaned containers.",
          ),
        )
      }
    }
    process.exitCode = 1
    return
  }

  // Wait for services to be healthy. Only check ports for services we
  // actually started — checking Kafka when its profile is dormant would
  // timeout because the container isn't running.
  const hasPostgres =
    profiles.includes("timescaledb") || profiles.includes("postgres-plain")
  const { waitForServices } = await import("@/cli/cluster/health-check.js")
  try {
    await waitForServices({
      flinkPort: parseInt(opts.port, 10),
      sqlGatewayPort: 8083,
      kafkaPort: profiles.includes("kafka") ? 9094 : undefined,
      postgresPort: hasPostgres ? 5433 : undefined,
      seaweedfsPort: 8333,
      icebergRestPort: profiles.includes("iceberg") ? 8181 : undefined,
      flussPort: profiles.includes("fluss") ? 9123 : undefined,
    })
  } catch {
    console.error(pc.red("Services did not become ready in time."))
    console.error(pc.dim("Check docker logs: docker compose -f ... logs"))
    process.exitCode = 1
    return
  }

  // Initialize PostgreSQL databases — only when a postgres profile is
  // active. Idempotent — skips if databases already exist.
  if (hasPostgres) {
    const pgService = profiles.includes("timescaledb")
      ? "postgres"
      : "postgres-plain"
    await initPostgresDatabases(
      engine,
      compose,
      pgService,
      profiles.includes("timescaledb"),
    )
  }

  // Initialize Flink SQL catalogs and tables from project config. The
  // helper itself further gates Iceberg/Fluss/Paimon DDL on whether
  // those profiles are active.
  await initFlinkCatalogs(parseInt(opts.port, 10), {
    iceberg: profiles.includes("iceberg"),
    fluss: profiles.includes("fluss"),
  })

  console.log("")
  console.log(
    `  ${pc.green("✓")} Flink UI:       ${pc.dim(`http://localhost:${opts.port}`)}`,
  )
  console.log(
    `  ${pc.green("✓")} SQL Gateway:    ${pc.dim("http://localhost:8083")}`,
  )
  if (profiles.includes("kafka")) {
    console.log(
      `  ${pc.green("✓")} Kafka:          ${pc.dim("localhost:9094")}`,
    )
  }
  if (hasPostgres) {
    console.log(
      `  ${pc.green("✓")} PostgreSQL:     ${pc.dim("localhost:5433 (pagila, chinook, employees, flink_sink)")}`,
    )
  }
  console.log(
    `  ${pc.green("✓")} SeaweedFS (S3): ${pc.dim("http://localhost:8333 (admin/admin, bucket: flink-state)")}`,
  )
  console.log(
    `  ${pc.green("✓")} SeaweedFS UI:   ${pc.dim("http://localhost:9333 (master) · http://localhost:8888 (filer)")}`,
  )
  if (profiles.includes("iceberg")) {
    console.log(
      `  ${pc.green("✓")} Iceberg UI:     ${pc.dim("http://lakekeeper.localtest.me:8181/ui (Lakekeeper) · API: /catalog/v1")}`,
    )
  }
  if (profiles.includes("fluss")) {
    console.log(
      `  ${pc.green("✓")} Fluss:          ${pc.dim("localhost:9123 (coordinator)")}`,
    )
  }
  console.log("")

  if (opts.seed) {
    await seedPipelines({ domain: opts.domain }, engine, resolved)
  }

  clack.outro(pc.green("Cluster is ready!"))
}

// ── cluster down ─────────────────────────────────────────────────────

export async function runClusterDown(opts: {
  volumes: boolean
  containerEngine?: ContainerEngineChoice
}): Promise<void> {
  clack.intro(pc.bgCyan(pc.black(" flink-reactor cluster down ")))

  // Kill background CDC publisher if running
  killCdcPublisher()

  let engine: ResolvedEngine
  try {
    ;({ engine } = await resolveClusterContext({
      containerEngine: opts.containerEngine,
    }))
  } catch (err) {
    reportEngineError(err)
    return
  }

  const compose = bundledComposePath()
  // Always pass *every* profile here — `down` must be exhaustive so
  // dormant containers (started under a different config that we've
  // since edited away) get cleaned up too. Sourced from the single
  // `ALL_PROFILES` constant so adding a new profile elsewhere
  // automatically threads through here.
  const allProfileArgs = ALL_PROFILES.flatMap((p) => ["--profile", p])
  const composeArgs: string[] = ["-f", compose, ...allProfileArgs, "down"]
  if (opts.volumes) {
    composeArgs.push("-v")
  }

  const spinner = clack.spinner()
  spinner.start("Stopping cluster...")

  try {
    execFileSync(engine.bin, [...engine.composeArgv(composeArgs)], {
      cwd: clusterDir(),
      stdio: "pipe",
      env: { ...process.env, ...engine.composeEnv() },
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
  containerEngine?: ContainerEngineChoice
}): Promise<void> {
  clack.intro(pc.bgCyan(pc.black(" flink-reactor cluster seed ")))

  let engine: ResolvedEngine
  let resolved: ResolvedConfig | undefined
  try {
    ;({ engine, resolved } = await resolveClusterContext({
      containerEngine: opts.containerEngine,
    }))
  } catch (err) {
    reportEngineError(err)
    return
  }

  const { isClusterRunning } = await import("@/cli/cluster/health-check.js")
  if (!(await isClusterRunning(8081))) {
    console.error(pc.red("Cluster is not running."))
    console.error(pc.dim("Start it first: flink-reactor cluster up"))
    process.exitCode = 1
    return
  }

  await seedPipelines(opts, engine, resolved)
  clack.outro(pc.green("Seeding complete!"))
}

async function seedPipelines(
  opts: {
    only?: SeedCategory
    domain?: CdcDomain
  },
  engine: ResolvedEngine,
  resolved: ResolvedConfig | undefined,
): Promise<void> {
  const { publishCdcMessages } = await import("@/cli/cluster/cdc-publisher.js")

  const category = opts.only
  const domain = opts.domain ?? "all"
  // Kafka-publishing CDC steps require `services.kafka` to be declared.
  // When it's not, `cluster up` never started Kafka — silently skip
  // rather than hammer a non-existent container with retries.
  const kafkaEnabled = resolved
    ? !!profilesFromConfig(resolved).includes("kafka")
    : true
  const publishCdc =
    kafkaEnabled &&
    (!category ||
      category === "cdc" ||
      category === "cdc-kafka" ||
      category === "streaming")

  // Step 1: Publish CDC batch data (needed before CDC pipelines)
  if (publishCdc) {
    const spinner = clack.spinner()
    spinner.start("Publishing CDC seed data to Kafka...")
    try {
      await publishCdcMessages({
        mode: "batch",
        domain,
        composeFile: bundledComposePath(),
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
    startBackgroundCdcPublisher(domain, engine)
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
    // Auto-prepend init SQL by writing a combined temp file
    // This ensures catalogs are created in the same session as the user's SQL
    let submitPath = filePath
    const cachedInitPath = initSqlPath()
    if (existsSync(cachedInitPath)) {
      const initSql = readFileSync(cachedInitPath, "utf-8")
      const userSql = readFileSync(filePath, "utf-8")
      const combinedPath = join(tmpdir(), "flink-reactor-combined-submit.sql")
      writeFileSync(combinedPath, `${initSql}\n\n${userSql}`, "utf-8")
      submitPath = combinedPath
    }

    const result = await client.submitSqlFile(submitPath)

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

function startBackgroundCdcPublisher(
  domain: CdcDomain = "all",
  engine: ResolvedEngine,
): void {
  // Write a small inline script that imports and runs the continuous publisher
  // Fork it as a detached child process
  const scriptPath = join(tmpdir(), "flink-reactor-cdc-continuous.mjs")
  const cdcModulePath = join(clusterDir(), "cdc-publisher.js")

  const compose = bundledComposePath()
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
    // Pin the child process to the same engine via env var so it doesn't
    // re-detect (and potentially pick a different one if both are installed).
    // Also propagate DOCKER_HOST so the docker-compose provider talks to the
    // podman socket — the child wouldn't get this otherwise.
    const child = fork(scriptPath, [], {
      detached: true,
      stdio: "ignore",
      env: {
        ...process.env,
        ...engine.composeEnv(),
        FR_CONTAINER_ENGINE: engine.name,
      },
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

// ── Flink SQL catalog initialization ─────────────────────────────────

function initSqlPath(): string {
  return join(tmpdir(), "flink-reactor-init.sql")
}

async function initFlinkCatalogs(
  _flinkPort: number,
  enabledServices: { iceberg: boolean; fluss: boolean } = {
    iceberg: true,
    fluss: true,
  },
): Promise<void> {
  const { loadConfig } = await import("@/cli/discovery.js")
  const { generateInitSql } = await import("@/cli/cluster/init-sql.js")

  const spinner = clack.spinner()
  spinner.start("Registering Flink SQL catalogs and tables...")

  try {
    // Load project config if available. `development` is the privileged name
    // in the scaffolded template; legacy `docker` / `minikube` fall back for
    // projects that predate the four-env template.
    const config = await loadConfig(process.cwd())
    let initConfig =
      config?.environments?.development?.sim?.init ??
      config?.environments?.docker?.sim?.init ??
      config?.environments?.minikube?.sim?.init

    // Fall back to first environment with sim.init
    if (!initConfig && config?.environments) {
      for (const env of Object.values(config.environments)) {
        if (env.sim?.init) {
          initConfig = env.sim.init
          break
        }
      }
    }

    const ctx = {
      kafkaBootstrapServers: "kafka:9092",
      postgresHost: "postgres",
      postgresPort: 5432,
      postgresUser: "reactor",
      postgresPassword: "reactor",
    }

    const result = generateInitSql(initConfig, ctx, {
      includeBuiltinJdbc: true,
    })

    // Iceberg/Fluss/Paimon DDL is skipped when their compose profiles
    // weren't activated — no point creating CREATE CATALOG statements
    // pointing at services that aren't running. Paimon piggybacks on
    // Fluss because `pg-fluss-paimon` always runs them together; if we
    // ever split them, plumb a separate flag.
    const { icebergInitStatements } = await import(
      "@/cli/cluster/iceberg-init.js"
    )
    const icebergDdl = enabledServices.iceberg
      ? icebergInitStatements(initConfig?.iceberg?.databases ?? [])
      : []

    const { flussInitStatements } = await import("@/cli/cluster/fluss-init.js")
    const flussDdl = enabledServices.fluss
      ? flussInitStatements(
          initConfig?.fluss?.databases ?? [],
          initConfig?.fluss?.bootstrapServers ?? "fluss-coordinator:9123",
        )
      : []

    const { paimonInitStatements } = await import(
      "@/cli/cluster/paimon-init.js"
    )
    const paimonDdl = enabledServices.fluss
      ? paimonInitStatements(
          initConfig?.paimon?.databases ?? [],
          initConfig?.paimon?.warehouse,
        )
      : []

    if (
      result.ddl.length === 0 &&
      result.seeding.length === 0 &&
      icebergDdl.length === 0 &&
      flussDdl.length === 0 &&
      paimonDdl.length === 0
    ) {
      spinner.stop(pc.dim("No catalogs to register."))
      return
    }

    // Cache DDL for auto-prepend in cluster submit
    if (result.ddl.length > 0) {
      writeFileSync(initSqlPath(), result.ddl.join("\n\n"), "utf-8")
    }

    // Submit DDL + seeding via SQL Gateway
    const { SqlGatewayClient } = await import(
      "@/cli/cluster/sql-gateway-client.js"
    )
    const client = new SqlGatewayClient("http://localhost:8083")
    const sessionHandle = await client.openSession()

    let catalogCount = 0
    let tableCount = 0
    let icebergDbCount = 0
    let flussDbCount = 0
    let paimonDbCount = 0

    const runDdl = async (stmt: string): Promise<void> => {
      const opHandle = await client.submitStatement(sessionHandle, stmt)
      for (let i = 0; i < 30; i++) {
        const status = await client.getOperationStatus(sessionHandle, opHandle)
        if (
          status !== "RUNNING" &&
          status !== "INITIALIZED" &&
          status !== "PENDING"
        )
          break
        await new Promise((r) => setTimeout(r, 500))
      }
    }

    // Iceberg catalog + databases first — Kafka/JDBC table DDL below may
    // reference them, and the catalog must exist before `USE CATALOG`.
    for (const stmt of icebergDdl) {
      await runDdl(stmt)
      if (stmt.includes("CREATE CATALOG")) catalogCount++
      if (stmt.includes("CREATE DATABASE")) icebergDbCount++
    }

    // Fluss catalog + databases — same ordering rationale as Iceberg.
    for (const stmt of flussDdl) {
      await runDdl(stmt)
      if (stmt.includes("CREATE CATALOG")) catalogCount++
      if (stmt.includes("CREATE DATABASE")) flussDbCount++
    }

    // Paimon catalog + databases — same ordering rationale as Iceberg.
    for (const stmt of paimonDdl) {
      await runDdl(stmt)
      if (stmt.includes("CREATE CATALOG")) catalogCount++
      if (stmt.includes("CREATE DATABASE")) paimonDbCount++
    }

    // Submit DDL statements (CREATE CATALOG, CREATE TABLE)
    for (const stmt of result.ddl) {
      await runDdl(stmt)
      if (stmt.includes("CREATE CATALOG")) catalogCount++
      if (stmt.includes("CREATE TABLE")) tableCount++
    }

    // Submit DataGen seeding statements (streaming — session stays open)
    let seedingCount = 0
    for (const stmt of result.seeding) {
      await client.submitStatement(sessionHandle, stmt)
      if (stmt.includes("INSERT INTO")) {
        seedingCount++
        // Brief pause to let the job start
        await new Promise((r) => setTimeout(r, 500))
      }
    }

    // Don't close the session — streaming DataGen jobs would be cancelled
    const parts = []
    if (catalogCount > 0) parts.push(`${catalogCount} catalogs`)
    if (tableCount > 0) parts.push(`${tableCount} tables`)
    if (icebergDbCount > 0) parts.push(`${icebergDbCount} Iceberg databases`)
    if (flussDbCount > 0) parts.push(`${flussDbCount} Fluss databases`)
    if (paimonDbCount > 0) parts.push(`${paimonDbCount} Paimon databases`)
    if (seedingCount > 0) parts.push(`${seedingCount} DataGen jobs`)

    spinner.stop(pc.green(`Registered ${parts.join(", ")}.`))
  } catch (err) {
    spinner.stop(pc.yellow("Catalog initialization failed (non-critical)."))
    if (err instanceof Error) {
      console.log(pc.dim(`  ${err.message}`))
    }
  }
}

// ── PostgreSQL initialization ────────────────────────────────────────

/** Databases created without data dumps (empty, used as pipeline sink targets) */
const EMPTY_DATABASES = ["flink_sink"] as const

async function initPostgresDatabases(
  engine: ResolvedEngine,
  compose: string,
  pgService: string = "postgres",
  timescaledb: boolean = false,
): Promise<void> {
  const initDir = join(clusterDir(), "init")

  // Ensure SQL dumps are downloaded before loading
  await ensureSqlDumps(initDir)

  const spinner = clack.spinner()
  spinner.start("Initializing PostgreSQL databases...")

  const cwd = clusterDir()
  const composeChildEnv = { ...process.env, ...engine.composeEnv() }
  const psql = (db: string, sql: string) =>
    execSync(
      engine.composeCommand(compose, [
        "exec",
        "-T",
        pgService,
        "psql",
        "-U",
        "reactor",
        "-d",
        db,
        "-c",
        JSON.stringify(sql),
      ]),
      { cwd, stdio: "pipe", env: composeChildEnv },
    )

  try {
    // Enable TimescaleDB extension on the default database
    if (timescaledb) {
      psql("postgres", "CREATE EXTENSION IF NOT EXISTS timescaledb")
    }

    // Create databases if they don't exist
    for (const db of [...SAMPLE_DATABASES, ...EMPTY_DATABASES]) {
      try {
        psql("postgres", `CREATE DATABASE ${db}`)
      } catch {
        // Already exists — fine
      }
    }

    // Enable TimescaleDB on each sample database
    if (timescaledb) {
      for (const db of SAMPLE_DATABASES) {
        psql(db, "CREATE EXTENSION IF NOT EXISTS timescaledb")
      }
    }

    // Load data only if tables don't exist yet
    for (const db of SAMPLE_DATABASES) {
      const schema = DB_SCHEMA[db]
      const result = execSync(
        engine.composeCommand(compose, [
          "exec",
          "-T",
          pgService,
          "psql",
          "-U",
          "reactor",
          "-d",
          db,
          "-tAc",
          `"SELECT count(*) FROM information_schema.tables WHERE table_schema = '${schema}'"`,
        ]),
        { cwd, stdio: "pipe", env: composeChildEnv },
      )
      const tableCount = parseInt(result.toString().trim(), 10)

      if (tableCount === 0) {
        const sqlFile = join(initDir, `${db}.sql`)
        if (!existsSync(sqlFile)) {
          spinner.message(`Skipping ${db} (dump not found)...`)
          continue
        }

        // Partial-state recovery: a prior interrupted load can commit
        // CREATE SCHEMA before failing on a later CREATE TABLE, leaving
        // the schema present with zero tables. tableCount=0 then green-
        // lights the load again, which crashes on `CREATE SCHEMA … already
        // exists`. Drop + recreate the *database* — it's empty by the
        // tableCount=0 definition (sample DBs hold dump data only), so
        // nothing of value is at risk. Uses WITH (FORCE) (PG 13+) to
        // evict any lingering connections from prior psql attempts.
        psql("postgres", `DROP DATABASE IF EXISTS ${db} WITH (FORCE)`)
        psql("postgres", `CREATE DATABASE ${db}`)
        if (timescaledb) {
          psql(db, "CREATE EXTENSION IF NOT EXISTS timescaledb")
        }

        execFileSync(
          engine.bin,
          [
            ...engine.composeArgv([
              "-f",
              compose,
              "cp",
              sqlFile,
              `${pgService}:/tmp/${db}.sql`,
            ]),
          ],
          { cwd, stdio: "pipe", env: composeChildEnv },
        )
        // `--single-transaction` makes the dump load atomic: any error
        // (CREATE SCHEMA conflict, COPY failure, etc.) rolls back the
        // entire transaction, so we can never end up with the partial
        // state the recovery logic above is paying off. Combined with
        // `ON_ERROR_STOP=1`, the first error short-circuits the script.
        execFileSync(
          engine.bin,
          [
            ...engine.composeArgv([
              "-f",
              compose,
              "exec",
              "-T",
              pgService,
              "psql",
              "-v",
              "ON_ERROR_STOP=1",
              "--single-transaction",
              "-U",
              "reactor",
              "-d",
              db,
              "-f",
              `/tmp/${db}.sql`,
            ]),
          ],
          { cwd, stdio: "pipe", timeout: 120_000, env: composeChildEnv },
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

// ── Utilities ────────────────────────────────────────────────────────

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
