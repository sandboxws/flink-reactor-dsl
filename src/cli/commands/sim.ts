import { execSync, spawn } from "node:child_process"
import { existsSync, readdirSync } from "node:fs"
import { dirname, join } from "node:path"
import { fileURLToPath } from "node:url"
import * as clack from "@clack/prompts"
import type { Command } from "commander"
import { Effect } from "effect"
import pc from "picocolors"
import { loadConfig } from "@/cli/discovery.js"
import { runCommand } from "@/cli/effect-runner.js"
import type { SimInitConfig } from "@/core/config.js"
import { CliError } from "@/core/errors.js"

// ── Resource path resolution ─────────────────────────────────────────

const __filename = fileURLToPath(import.meta.url)
const __dirname = dirname(__filename)

function simDir(): string {
  // Bundled: dist/ → ../src/cli/sim
  const fromDist = join(__dirname, "..", "src", "cli", "sim")
  if (existsSync(join(fromDist, "manifests"))) {
    return fromDist
  }
  // Source: src/cli/commands/ → ../sim
  return join(__dirname, "..", "sim")
}

function manifestsDir(): string {
  return join(simDir(), "manifests")
}

// ── Defaults ─────────────────────────────────────────────────────────

const DEFAULT_CPUS = "12"
const DEFAULT_MEMORY = "65536"
const DEFAULT_DISK = "100g"
const DEFAULT_K8S_VERSION = "v1.31.0"
const DEFAULT_NAMESPACE = "flink-demo"
const OPERATOR_NAMESPACE = "flink-system"

/** Pod statuses that indicate a non-recoverable failure. */
const TERMINAL_POD_STATUSES = new Set([
  "ImagePullBackOff",
  "ErrImagePull",
  "ErrImageNeverPull",
  "InvalidImageName",
  "CrashLoopBackOff",
  "CreateContainerConfigError",
  "RunContainerError",
])

const IMAGE_PULL_STATUSES = new Set([
  "ImagePullBackOff",
  "ErrImagePull",
  "ErrImageNeverPull",
  "InvalidImageName",
])

// ── Command registration ─────────────────────────────────────────────

export function registerSimCommand(program: Command): void {
  const sim = program
    .command("sim")
    .description(
      "Manage minikube simulation stack (full Flink platform on a single PC)",
    )

  sim
    .command("up")
    .description(
      "Bootstrap minikube cluster and deploy simulation infrastructure",
    )
    .option("--cpus <cpus>", "CPU cores for minikube", DEFAULT_CPUS)
    .option("--memory <mb>", "Memory in MB for minikube", DEFAULT_MEMORY)
    .option("--disk <size>", "Disk size for minikube", DEFAULT_DISK)
    .option(
      "--k8s-version <version>",
      "Kubernetes version",
      DEFAULT_K8S_VERSION,
    )
    .option("--namespace <ns>", "Target namespace", DEFAULT_NAMESPACE)
    .option("--no-operator", "Skip Flink Operator installation")
    .option(
      "-e, --env <name>",
      "Environment name from flink-reactor.config.ts (reads sim.init)",
    )
    .option("--no-init", "Skip resource initialization even if config exists")
    .action(
      async (opts: {
        cpus: string
        memory: string
        disk: string
        k8sVersion: string
        namespace: string
        operator: boolean
        env?: string
        init: boolean
      }) => {
        await runCommand(
          Effect.tryPromise({
            try: () => runSimUp(opts),
            catch: (err) =>
              new CliError({
                reason: "invalid_args",
                message: (err as Error).message,
              }),
          }),
        )
      },
    )

  sim
    .command("down")
    .description("Tear down simulation infrastructure")
    .option(
      "--volumes",
      "Delete persistent volume claims (checkpoint and state data)",
    )
    .option("--all", "Also stop minikube entirely")
    .option("--namespace <ns>", "Target namespace", DEFAULT_NAMESPACE)
    .action(
      async (opts: { volumes?: boolean; all?: boolean; namespace: string }) => {
        await runCommand(
          Effect.tryPromise({
            try: () => runSimDown(opts),
            catch: (err) =>
              new CliError({
                reason: "invalid_args",
                message: (err as Error).message,
              }),
          }),
        )
      },
    )

  sim
    .command("status")
    .description("Show simulation pod status and resource usage")
    .option("--namespace <ns>", "Target namespace", DEFAULT_NAMESPACE)
    .action(async (opts: { namespace: string }) => {
      await runCommand(
        Effect.tryPromise({
          try: () => runSimStatus(opts),
          catch: (err) =>
            new CliError({
              reason: "invalid_args",
              message: (err as Error).message,
            }),
        }),
      )
    })
}

// ── Preflight checks ─────────────────────────────────────────────────

interface PreflightResult {
  minikube: boolean
  kubectl: boolean
  helm: boolean
}

function checkPrerequisites(): PreflightResult {
  return {
    minikube: commandExists("minikube"),
    kubectl: commandExists("kubectl"),
    helm: commandExists("helm"),
  }
}

function commandExists(cmd: string): boolean {
  try {
    execSync(`command -v ${cmd}`, { stdio: "pipe" })
    return true
  } catch {
    return false
  }
}

function isNamespaceTerminating(ns: string): boolean {
  try {
    const phase = execSync(
      `kubectl get namespace ${ns} -o jsonpath='{.status.phase}'`,
      {
        stdio: "pipe",
        encoding: "utf-8",
      },
    ).trim()
    return phase === "Terminating"
  } catch {
    return false
  }
}

function isMinikubeRunning(): boolean {
  try {
    const status = execSync("minikube status --format={{.Host}}", {
      stdio: "pipe",
      encoding: "utf-8",
    }).trim()
    return status === "Running"
  } catch {
    return false
  }
}

function isOperatorInstalled(): boolean {
  try {
    execSync(`helm status flink-operator -n ${OPERATOR_NAMESPACE}`, {
      stdio: "pipe",
    })
    return true
  } catch {
    return false
  }
}

// ── sim up ────────────────────────────────────────────────────────────

async function runSimUp(opts: {
  cpus: string
  memory: string
  disk: string
  k8sVersion: string
  namespace: string
  operator: boolean
  env?: string
  init: boolean
}): Promise<void> {
  clack.intro(pc.bgCyan(pc.black(" flink-reactor sim up ")))

  // ── Step 1: Check prerequisites ──────────────────────────────────
  const prereqs = checkPrerequisites()
  const missing: string[] = []

  if (!prereqs.minikube) missing.push("minikube")
  if (!prereqs.kubectl) missing.push("kubectl")
  if (!prereqs.helm) missing.push("helm")

  if (missing.length > 0) {
    console.error(pc.red(`Missing required tools: ${missing.join(", ")}`))
    console.error("")
    if (!prereqs.minikube)
      console.error(
        pc.dim("  minikube → https://minikube.sigs.k8s.io/docs/start/"),
      )
    if (!prereqs.kubectl)
      console.error(
        pc.dim("  kubectl  → https://kubernetes.io/docs/tasks/tools/"),
      )
    if (!prereqs.helm)
      console.error(pc.dim("  helm     → https://helm.sh/docs/intro/install/"))
    console.error("")
    process.exitCode = 1
    return
  }

  console.log(`  ${pc.green("✓")} Prerequisites: minikube, kubectl, helm`)

  // ── Step 2: Start minikube ───────────────────────────────────────
  if (isMinikubeRunning()) {
    console.log(`  ${pc.green("✓")} Minikube already running`)
  } else {
    const spinner = clack.spinner()
    spinner.start("Starting minikube...")

    try {
      execSync(
        [
          "minikube start",
          "--driver=docker",
          `--cpus=${opts.cpus}`,
          `--memory=${opts.memory}`,
          `--disk-size=${opts.disk}`,
          `--kubernetes-version=${opts.k8sVersion}`,
        ].join(" "),
        { stdio: "pipe" },
      )
      spinner.stop(pc.green("Minikube started."))
    } catch (err) {
      spinner.stop(pc.red("Failed to start minikube."))
      if (err instanceof Error) console.error(pc.dim(err.message))
      process.exitCode = 1
      return
    }
  }

  // Enable metrics-server addon
  try {
    execSync("minikube addons enable metrics-server", { stdio: "pipe" })
  } catch {
    // Non-critical
  }

  // ── Step 3: Install Flink Operator ───────────────────────────────
  if (!opts.operator) {
    console.log(`  ${pc.dim("⊘")} Flink Operator skipped (--no-operator)`)
  } else if (isOperatorInstalled()) {
    console.log(`  ${pc.green("✓")} Flink Operator already installed`)
  } else {
    const spinner = clack.spinner()
    spinner.start("Installing Flink Kubernetes Operator...")

    try {
      execSync(
        "helm repo add flink-operator https://downloads.apache.org/flink/flink-kubernetes-operator-1.14.0/",
        { stdio: "pipe" },
      )
      execSync(
        [
          "helm install flink-operator flink-operator/flink-kubernetes-operator",
          `--namespace ${OPERATOR_NAMESPACE}`,
          "--create-namespace",
          "--set webhook.create=false",
        ].join(" "),
        { stdio: "pipe" },
      )
      spinner.stop(pc.green("Flink Operator installed."))
    } catch (err) {
      spinner.stop(pc.red("Failed to install Flink Operator."))
      if (err instanceof Error) console.error(pc.dim(err.message))
      process.exitCode = 1
      return
    }
  }

  // ── Step 4: Build pg-init image for sample database seeding ─────
  if (opts.init) {
    const { ensureSqlDumps, clusterInitDir } = await import(
      "@/cli/cluster/pg-samples.js"
    )
    const initDir = clusterInitDir()
    await ensureSqlDumps(initDir)

    // Delete stale chinook.sql if it still contains DROP DATABASE
    const chinookPath = join(initDir, "chinook.sql")
    if (existsSync(chinookPath)) {
      const { readFileSync } = await import("node:fs")
      const head = readFileSync(chinookPath, "utf-8").slice(0, 2000)
      if (/^DROP DATABASE\b/m.test(head)) {
        const { unlinkSync } = await import("node:fs")
        unlinkSync(chinookPath)
        await ensureSqlDumps(initDir)
      }
    }

    const buildSpinner = clack.spinner()
    buildSpinner.start("Building pg-init image...")
    try {
      const dockerEnvOut = execSync("minikube docker-env --shell bash", {
        stdio: "pipe",
        encoding: "utf-8",
      })
      const envVars: Record<string, string> = {}
      for (const match of dockerEnvOut.matchAll(/export (\w+)="([^"]+)"/g)) {
        envVars[match[1]] = match[2]
      }

      execSync(
        `docker build -t pg-init:latest -f "${join(simDir(), "Dockerfile.pg-init")}" "${initDir}"`,
        {
          stdio: "pipe",
          env: { ...process.env, ...envVars },
          timeout: 120_000,
        },
      )
      buildSpinner.stop(pc.green("pg-init image built."))
    } catch (err) {
      buildSpinner.stop(pc.yellow("pg-init image build failed (non-critical)."))
      if (err instanceof Error) console.log(pc.dim(`  ${err.message}`))
    }
  }

  // ── Step 5: Generate init SQL ConfigMap (before manifests) ──────
  let initConfig: SimInitConfig | undefined
  if (opts.init) {
    const config = await loadConfig(process.cwd())
    if (config) {
      const resolvedEnv = opts.env ?? "minikube"
      const envEntry = config.environments?.[resolvedEnv]
      initConfig = envEntry?.sim?.init
    }

    if (initConfig?.kafka?.catalogs || initConfig?.jdbc?.catalogs) {
      const { generateInitSql } = await import("@/cli/cluster/init-sql.js")
      const initResult = generateInitSql(initConfig, {
        kafkaBootstrapServers: "kafka:9092",
        postgresHost: `postgres.${opts.namespace}.svc`,
        postgresPort: 5432,
        postgresUser: "reactor",
        postgresPassword: "reactor",
      })

      if (initResult.ddl.length > 0) {
        const initSqlContent = initResult.ddl.join("\n\n")
        const cmSpinner = clack.spinner()
        cmSpinner.start("Creating init SQL ConfigMap...")
        try {
          // Escape single quotes for shell
          const escaped = initSqlContent.replace(/'/g, "'\\''")
          execSync(
            `kubectl create configmap flink-init-sql --from-literal=init-catalogs.sql='${escaped}' -n ${opts.namespace} --dry-run=client -o yaml | kubectl apply -f -`,
            { stdio: "pipe" },
          )
          cmSpinner.stop(pc.green("Init SQL ConfigMap created."))
        } catch (err) {
          cmSpinner.stop(
            pc.yellow("Failed to create init SQL ConfigMap (non-critical)."),
          )
          if (err instanceof Error) console.log(pc.dim(`  ${err.message}`))
        }
      }
    }
  }

  // ── Step 5: Wait for terminating namespace, then apply manifests ─
  const manifests = manifestsDir()
  if (!existsSync(manifests)) {
    console.error(pc.red(`Manifests directory not found: ${manifests}`))
    process.exitCode = 1
    return
  }

  // If the namespace is being deleted from a previous sim down, wait for it.
  if (isNamespaceTerminating(opts.namespace)) {
    const nsSpinner = clack.spinner()
    nsSpinner.start(
      `Waiting for namespace ${opts.namespace} to finish deleting...`,
    )
    const nsDeadline = Date.now() + 120_000
    while (Date.now() < nsDeadline && isNamespaceTerminating(opts.namespace)) {
      await new Promise<void>((resolve) => setTimeout(resolve, 2_000))
    }
    if (isNamespaceTerminating(opts.namespace)) {
      nsSpinner.stop(pc.red("Namespace still terminating after 2 minutes."))
      process.exitCode = 1
      return
    }
    nsSpinner.stop(pc.green("Namespace deleted."))
  }

  // Delete completed init Jobs so they re-run on each sim up
  try {
    execSync(
      `kubectl delete job pg-init -n ${opts.namespace} --ignore-not-found`,
      { stdio: "pipe", timeout: 10_000 },
    )
  } catch {
    // Namespace may not exist yet — fine
  }

  const yamlFiles = readdirSync(manifests)
    .filter((f) => f.endsWith(".yaml"))
    .sort()

  const spinner = clack.spinner()
  spinner.start(`Applying ${yamlFiles.length} manifests...`)

  try {
    execSync(`kubectl apply -f "${manifests}"`, { stdio: "pipe" })
    spinner.stop(pc.green(`Applied ${yamlFiles.length} manifests.`))
  } catch (err) {
    spinner.stop(pc.red("Failed to apply manifests."))
    if (err instanceof Error) console.error(pc.dim(err.message))
    process.exitCode = 1
    return
  }

  // ── Step 6: Wait for pods ────────────────────────────────────────
  const waitSpinner = clack.spinner()
  waitSpinner.start("Waiting for pods to be ready...")

  const POLL_INTERVAL_MS = 3_000
  const TIMEOUT_MS = 300_000
  const deadline = Date.now() + TIMEOUT_MS
  let allReady = false
  let failed = false

  while (Date.now() < deadline) {
    try {
      const pods = getPodStatuses(opts.namespace)
      const tracked = pods.filter(
        (p) => p.status !== "Completed" && p.status !== "Succeeded",
      )
      const readyCount = tracked.filter((p) => p.isReady).length
      const failing = tracked.filter((p) => TERMINAL_POD_STATUSES.has(p.status))

      if (tracked.length > 0) {
        if (readyCount === tracked.length) {
          allReady = true
          break
        }
        if (failing.length > 0) {
          failed = true
          break
        }
        waitSpinner.message(`${readyCount}/${tracked.length} pods ready`)
      }
    } catch {
      // kubectl may not be responsive yet
    }

    await new Promise<void>((resolve) => setTimeout(resolve, POLL_INTERVAL_MS))
  }

  if (allReady) {
    waitSpinner.stop(pc.green("All pods ready."))
  } else if (failed) {
    waitSpinner.stop(pc.red("Some pods failed to start:"))
    printPodTable(opts.namespace)
    const pods = safeGetPodStatuses(opts.namespace)
    const hasImageErrors = pods.some((p) => IMAGE_PULL_STATUSES.has(p.status))
    if (hasImageErrors) {
      console.log("")
      console.log(pc.bold("  Images must be built inside minikube's Docker:"))
      console.log(pc.dim("    eval $(minikube docker-env)"))
      console.log(
        pc.dim(
          "    docker build -t flink-reactor:2.0.1 -f src/cli/cluster/Dockerfile.flink .",
        ),
      )
      console.log(
        pc.dim(
          "    docker build -t flink-reactor-s3:2.0.1 -f src/cli/sim/Dockerfile.flink-s3 .",
        ),
      )
      console.log(
        pc.dim(
          "    docker build -t pg-init:latest -f src/cli/sim/Dockerfile.pg-init src/cli/cluster/init",
        ),
      )
      console.log("")
      console.log(pc.dim("  Then re-run: flink-reactor sim up"))
    }
    console.log("")
    clack.outro(pc.red("Cluster started but pods are not healthy."))
    process.exitCode = 1
    return
  } else {
    waitSpinner.stop(pc.yellow("Timed out waiting for pods:"))
    printPodTable(opts.namespace)
    console.log("")
    clack.outro(pc.yellow("Cluster started but some pods are not ready."))
    process.exitCode = 1
    return
  }

  // ── Step 7: Config-driven init ──────────────────────────────────
  if (opts.init) {
    await runInitFromConfig(opts.env, opts.namespace)
  } else {
    console.log(`  ${pc.dim("⊘")} Resource init skipped (--no-init)`)
  }

  // ── Step 8: Port-forward services ─────────────────────────────────
  const portForwards = [
    {
      svc: "svc/reactor-server",
      local: 8080,
      remote: 8080,
      label: "Dashboard",
    },
    { svc: "svc/postgres", local: 5433, remote: 5432, label: "Postgres" },
  ]

  for (const pf of portForwards) {
    const proc = spawn(
      "kubectl",
      [
        "port-forward",
        pf.svc,
        `${pf.local}:${pf.remote}`,
        "-n",
        opts.namespace,
      ],
      { stdio: "ignore", detached: true },
    )
    proc.unref()
    console.log(
      `  ${pc.green("✓")} Port-forward ${pf.label}: ${pc.cyan(`localhost:${pf.local}`)}`,
    )
  }

  clack.outro(pc.green("Simulation infrastructure deployed!"))
}

// ── Config-driven init ────────────────────────────────────────────────

async function runInitFromConfig(
  envName: string | undefined,
  namespace: string,
): Promise<void> {
  const config = await loadConfig(process.cwd())
  if (!config) {
    console.log(
      `  ${pc.dim("⊘")} No flink-reactor.config.ts found, skipping init`,
    )
    return
  }

  // Resolve environment — prefer explicit --env, fall back to "minikube"
  const resolvedEnv = envName ?? "minikube"
  const envEntry = config.environments?.[resolvedEnv]

  if (!envEntry?.sim?.init) {
    console.log(
      `  ${pc.dim("⊘")} No sim.init in environment "${resolvedEnv}", skipping init`,
    )
    return
  }

  const initConfig = envEntry.sim.init
  await runInit(initConfig, namespace)
}

async function runInit(init: SimInitConfig, namespace: string): Promise<void> {
  const databases = init.iceberg?.databases ?? []
  const topics = init.kafka?.topics ?? []
  const kafkaCatalogs = init.kafka?.catalogs ?? []
  const jdbcCatalogs = init.jdbc?.catalogs ?? []

  if (
    databases.length === 0 &&
    topics.length === 0 &&
    kafkaCatalogs.length === 0 &&
    jdbcCatalogs.length === 0
  )
    return

  console.log("")

  // ── Create Kafka topics ──────────────────────────────────────────
  if (topics.length > 0) {
    const spinner = clack.spinner()
    spinner.start(`Creating ${topics.length} Kafka topic(s)...`)

    let created = 0
    let skipped = 0

    for (const topic of topics) {
      try {
        execSync(
          [
            `kubectl exec -n ${namespace} deploy/kafka --`,
            `/opt/kafka/bin/kafka-topics.sh`,
            `--create`,
            `--if-not-exists`,
            `--topic ${topic}`,
            `--bootstrap-server localhost:9092`,
            `--partitions 3`,
            `--replication-factor 1`,
          ].join(" "),
          { stdio: "pipe", timeout: 15_000 },
        )
        created++
      } catch {
        skipped++
      }
    }

    spinner.stop(
      pc.green(
        `Kafka topics: ${created} created${skipped > 0 ? `, ${skipped} skipped` : ""}`,
      ),
    )
  }

  // ── Create Iceberg databases ─────────────────────────────────────
  if (databases.length > 0) {
    const { icebergInitStatements } = await import(
      "@/cli/cluster/iceberg-init.js"
    )
    const stmts = icebergInitStatements(databases)

    const spinner = clack.spinner()
    spinner.start(`Creating ${databases.length} Iceberg database(s)...`)

    try {
      execSqlViaGateway(stmts.join("; "), namespace)
      spinner.stop(pc.green(`Iceberg databases: ${databases.length} created`))
    } catch {
      spinner.stop(pc.yellow("Iceberg database creation failed."))
    }
  }

  // ── Register Flink SQL catalogs and tables ────────────────────────
  if (kafkaCatalogs.length > 0 || jdbcCatalogs.length > 0) {
    const { generateInitSql } = await import("@/cli/cluster/init-sql.js")
    const initResult = generateInitSql(init, {
      kafkaBootstrapServers: "kafka:9092",
      postgresHost: `postgres.${namespace}.svc`,
      postgresPort: 5432,
      postgresUser: "reactor",
      postgresPassword: "reactor",
    })

    // Submit DDL (CREATE CATALOG, CREATE TABLE)
    if (initResult.ddl.length > 0) {
      const ddlSpinner = clack.spinner()
      ddlSpinner.start("Registering Flink SQL catalogs and tables...")

      let catalogCount = 0
      let tableCount = 0

      try {
        const allDdl = initResult.ddl.join("; ")
        execSqlViaGateway(allDdl, namespace)

        for (const stmt of initResult.ddl) {
          if (stmt.includes("CREATE CATALOG")) catalogCount++
          if (stmt.includes("CREATE TABLE")) tableCount++
        }

        const parts = []
        if (catalogCount > 0) parts.push(`${catalogCount} catalogs`)
        if (tableCount > 0) parts.push(`${tableCount} tables`)
        ddlSpinner.stop(pc.green(`Registered ${parts.join(", ")}.`))
      } catch (err) {
        ddlSpinner.stop(
          pc.yellow("Catalog registration failed (non-critical)."),
        )
        if (err instanceof Error) console.log(pc.dim(`  ${err.message}`))
      }
    }

    // Submit DataGen seeding (streaming INSERT jobs)
    if (initResult.seeding.length > 0) {
      const seedSpinner = clack.spinner()
      const insertCount = initResult.seeding.filter((s) =>
        s.includes("INSERT INTO"),
      ).length
      seedSpinner.start(`Starting ${insertCount} DataGen seeding job(s)...`)

      try {
        const allSeeding = initResult.seeding.join("; ")
        execSqlViaGateway(allSeeding, namespace)
        seedSpinner.stop(
          pc.green(`Started ${insertCount} DataGen seeding jobs.`),
        )
      } catch (err) {
        seedSpinner.stop(pc.yellow("DataGen seeding failed (non-critical)."))
        if (err instanceof Error) console.log(pc.dim(`  ${err.message}`))
      }
    }
  }
}

/**
 * Execute SQL statements via the SQL Gateway REST API using kubectl exec + curl.
 * Runs from within the cluster so no port-forward is needed.
 */
function execSqlViaGateway(sql: string, namespace: string): void {
  // Open a session
  const openResult = execSync(
    `kubectl exec -n ${namespace} deploy/flink-sql-gateway -- curl -s -X POST http://localhost:8083/v1/sessions -H 'Content-Type: application/json' -d '{}'`,
    { stdio: "pipe", encoding: "utf-8", timeout: 15_000 },
  )
  const sessionHandle = JSON.parse(openResult).sessionHandle as string

  try {
    // Split on semicolons and execute each statement
    const statements = sql
      .split(";")
      .map((s) => s.trim())
      .filter(Boolean)

    for (const stmt of statements) {
      const payload = JSON.stringify({ statement: `${stmt};` })
      execSync(
        `kubectl exec -n ${namespace} deploy/flink-sql-gateway -- curl -s -X POST http://localhost:8083/v1/sessions/${sessionHandle}/statements -H 'Content-Type: application/json' -d '${payload.replace(/'/g, "'\\''")}'`,
        { stdio: "pipe", timeout: 15_000 },
      )
      // Brief pause to let the statement execute
      execSync("sleep 1", { stdio: "pipe" })
    }
  } finally {
    // Close session (best-effort)
    try {
      execSync(
        `kubectl exec -n ${namespace} deploy/flink-sql-gateway -- curl -s -X DELETE http://localhost:8083/v1/sessions/${sessionHandle}`,
        { stdio: "pipe", timeout: 5_000 },
      )
    } catch {
      // Best-effort cleanup
    }
  }
}

// ── sim down ──────────────────────────────────────────────────────────

async function runSimDown(opts: {
  volumes?: boolean
  all?: boolean
  namespace: string
}): Promise<void> {
  clack.intro(pc.bgCyan(pc.black(" flink-reactor sim down ")))

  if (!commandExists("kubectl")) {
    console.error(pc.red("kubectl not found."))
    process.exitCode = 1
    return
  }

  const spinner = clack.spinner()

  // Delete manifests (reverse of apply)
  const manifests = manifestsDir()
  if (existsSync(manifests)) {
    // Delete FlinkDeployments first — their operator finalizer blocks
    // namespace deletion. Strip finalizers and delete in one shot so the
    // operator can't re-add the finalizer between the two calls.
    try {
      execSync(`kubectl get flinkdeployments -n ${opts.namespace} -o name`, {
        stdio: "pipe",
        encoding: "utf-8",
        timeout: 10_000,
      })
        .trim()
        .split("\n")
        .filter(Boolean)
        .forEach((fd) => {
          try {
            execSync(
              `kubectl patch ${fd} -n ${opts.namespace} -p '{"metadata":{"finalizers":null}}' --type=merge`,
              { stdio: "pipe", timeout: 10_000 },
            )
            execSync(
              `kubectl delete ${fd} -n ${opts.namespace} --ignore-not-found --wait=false`,
              { stdio: "pipe", timeout: 10_000 },
            )
          } catch {
            // Best effort
          }
        })
    } catch {
      // No FlinkDeployments or CRD not installed
    }

    spinner.start("Deleting simulation resources...")
    try {
      // --wait=false so kubectl returns immediately instead of blocking
      // on finalizers. --timeout as a safety net.
      execSync(
        `kubectl delete -f "${manifests}" --ignore-not-found --wait=false --timeout=60s`,
        { stdio: "pipe", timeout: 90_000 },
      )
    } catch {
      // Some resources may already be deleted
    }

    // Poll until pods are gone
    const POLL_INTERVAL_MS = 3_000
    const TIMEOUT_MS = 120_000
    const deadline = Date.now() + TIMEOUT_MS

    while (Date.now() < deadline) {
      const pods = safeGetPodStatuses(opts.namespace)
      const remaining = pods.filter(
        (p) => p.status !== "Completed" && p.status !== "Succeeded",
      )

      if (remaining.length === 0) break

      spinner.message(`Waiting for ${remaining.length} pod(s) to terminate...`)
      await new Promise<void>((resolve) =>
        setTimeout(resolve, POLL_INTERVAL_MS),
      )
    }

    const leftover = safeGetPodStatuses(opts.namespace).filter(
      (p) => p.status !== "Completed" && p.status !== "Succeeded",
    )
    if (leftover.length === 0) {
      spinner.stop(pc.green("All pods terminated."))
    } else {
      spinner.stop(pc.yellow(`${leftover.length} pod(s) still terminating.`))
    }
  }

  // Delete PVCs if requested
  if (opts.volumes) {
    const pvcSpinner = clack.spinner()
    pvcSpinner.start("Deleting persistent volume claims...")
    try {
      execSync(
        `kubectl delete pvc --all -n ${opts.namespace} --ignore-not-found`,
        { stdio: "pipe" },
      )
      pvcSpinner.stop(pc.green("PVCs deleted."))
    } catch {
      pvcSpinner.stop(pc.yellow("Failed to delete PVCs."))
    }
  }

  // Stop minikube entirely if requested
  if (opts.all) {
    if (!commandExists("minikube")) {
      console.log(pc.yellow("minikube not found, skipping shutdown."))
    } else {
      const mkSpinner = clack.spinner()
      mkSpinner.start("Stopping minikube...")
      try {
        execSync("minikube stop", { stdio: "pipe" })
        mkSpinner.stop(pc.green("Minikube stopped."))
      } catch {
        mkSpinner.stop(pc.yellow("Minikube may already be stopped."))
      }
    }
  }

  clack.outro("Done.")
}

// ── sim status ────────────────────────────────────────────────────────

async function runSimStatus(opts: { namespace: string }): Promise<void> {
  if (!commandExists("kubectl")) {
    console.error(pc.red("kubectl not found."))
    process.exitCode = 1
    return
  }

  // Check minikube
  if (commandExists("minikube")) {
    if (isMinikubeRunning()) {
      console.log(`\n  ${pc.green("✓")} Minikube: ${pc.green("running")}`)
    } else {
      console.log(`\n  ${pc.red("✗")} Minikube: ${pc.red("stopped")}`)
      console.log(pc.dim("    Start with: flink-reactor sim up"))
      return
    }
  }

  // Pod status
  console.log(pc.bold(`\n  Pods (${opts.namespace})\n`))

  try {
    const output = execSync(
      `kubectl get pods -n ${opts.namespace} -o wide --no-headers`,
      { stdio: "pipe", encoding: "utf-8" },
    ).trim()

    if (!output) {
      console.log(pc.dim("  No pods found."))
    } else {
      // Parse and pretty-print pod status
      for (const line of output.split("\n")) {
        const parts = line.trim().split(/\s+/)
        const name = parts[0]
        const ready = parts[1]
        const status = parts[2]

        const statusColor =
          status === "Running"
            ? pc.green
            : status === "Completed"
              ? pc.dim
              : status === "Pending" || status === "ContainerCreating"
                ? pc.yellow
                : pc.red

        console.log(
          `  ${statusColor(status === "Running" ? "✓" : status === "Completed" ? "○" : "✗")} ${padRight(name, 30)} ${padRight(ready, 8)} ${statusColor(status)}`,
        )
      }
    }
  } catch {
    console.log(pc.dim("  Could not fetch pod status."))
  }

  // Resource usage (if metrics-server is available)
  console.log(pc.bold(`\n  Resource Usage\n`))

  try {
    const topOutput = execSync(
      `kubectl top pods -n ${opts.namespace} --no-headers`,
      { stdio: "pipe", encoding: "utf-8" },
    ).trim()

    if (topOutput) {
      console.log(
        `  ${pc.dim(padRight("Pod", 30))} ${pc.dim(padRight("CPU", 10))} ${pc.dim("Memory")}`,
      )
      console.log(`  ${pc.dim("─".repeat(55))}`)

      for (const line of topOutput.split("\n")) {
        const parts = line.trim().split(/\s+/)
        const name = parts[0]
        const cpu = parts[1]
        const memory = parts[2]
        console.log(`  ${padRight(name, 30)} ${padRight(cpu, 10)} ${memory}`)
      }
    }
  } catch {
    console.log(
      pc.dim("  Metrics not available (metrics-server may not be ready)."),
    )
  }

  console.log("")
}

// ── Pod polling ──────────────────────────────────────────────────────

/** Strip replica-set / job hash suffixes from a K8s pod name. */
function podShortName(name: string): string {
  // Deployment pods: <name>-<rs-hash>-<pod-hash>
  const withoutDeploy = name.replace(/-[a-z0-9]{6,10}-[a-z0-9]{5}$/, "")
  if (withoutDeploy !== name) return withoutDeploy
  // Job pods: <name>-<hash>
  return name.replace(/-[a-z0-9]{5}$/, "")
}

interface PodInfo {
  shortName: string
  ready: string
  status: string
  isReady: boolean
}

function getPodStatuses(namespace: string): PodInfo[] {
  const output = execSync(`kubectl get pods -n ${namespace} --no-headers`, {
    stdio: "pipe",
    encoding: "utf-8",
  }).trim()

  if (!output) return []

  return output
    .split("\n")
    .filter(Boolean)
    .map((line) => {
      const parts = line.trim().split(/\s+/)
      const [readyN, totalN] = parts[1].split("/").map(Number)
      return {
        shortName: podShortName(parts[0]),
        ready: parts[1],
        status: parts[2],
        isReady: parts[2] === "Running" && readyN === totalN,
      }
    })
}

function safeGetPodStatuses(namespace: string): PodInfo[] {
  try {
    return getPodStatuses(namespace)
  } catch {
    return []
  }
}

function printPodTable(namespace: string): void {
  try {
    const pods = getPodStatuses(namespace)
    const tracked = pods.filter(
      (p) => p.status !== "Completed" && p.status !== "Succeeded",
    )
    for (const pod of tracked) {
      const isFailing = TERMINAL_POD_STATUSES.has(pod.status)
      const icon = pod.isReady
        ? pc.green("✓")
        : isFailing
          ? pc.red("✗")
          : pc.yellow("●")
      const color = pod.isReady ? pc.green : isFailing ? pc.red : pc.yellow
      console.log(
        `  ${icon} ${padRight(pod.shortName, 24)} ${color(pod.status)}`,
      )
    }
  } catch {
    console.log(pc.dim(`  kubectl get pods -n ${namespace}`))
  }
}

// ── Utilities ────────────────────────────────────────────────────────

function padRight(str: string, len: number): string {
  return str.length >= len
    ? str.substring(0, len)
    : str + " ".repeat(len - str.length)
}
