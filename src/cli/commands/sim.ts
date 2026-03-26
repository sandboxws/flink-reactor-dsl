import { execSync } from "node:child_process"
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
        "helm repo add flink-operator https://downloads.apache.org/flink/flink-kubernetes-operator-1.11.0/",
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

  // ── Step 4: Generate init SQL ConfigMap (before manifests) ──────
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

  // ── Step 5: Apply manifests ──────────────────────────────────────
  const manifests = manifestsDir()
  if (!existsSync(manifests)) {
    console.error(pc.red(`Manifests directory not found: ${manifests}`))
    process.exitCode = 1
    return
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
  waitSpinner.start(
    "Waiting for pods to be ready (this may take a few minutes)...",
  )

  try {
    // Wait up to 5 minutes for all deployments in the namespace
    execSync(
      `kubectl wait --for=condition=available deployment --all -n ${opts.namespace} --timeout=300s`,
      { stdio: "pipe" },
    )
    waitSpinner.stop(pc.green("All pods ready."))
  } catch {
    waitSpinner.stop(pc.yellow("Some pods may not be ready yet."))
    console.log(pc.dim(`  Check with: kubectl get pods -n ${opts.namespace}`))
  }

  // ── Step 7: Config-driven init ──────────────────────────────────
  if (opts.init) {
    await runInitFromConfig(opts.env, opts.namespace)
  } else {
    console.log(`  ${pc.dim("⊘")} Resource init skipped (--no-init)`)
  }

  // ── Summary ──────────────────────────────────────────────────────
  console.log("")
  console.log(pc.bold("  Next steps:"))
  console.log("")
  console.log(pc.dim("  1. Build Flink image (in minikube's Docker):"))
  console.log(pc.dim("     eval $(minikube docker-env)"))
  console.log(
    pc.dim(
      "     docker build -t flink-reactor:2.0.1 -f src/cli/cluster/Dockerfile.flink .",
    ),
  )
  console.log(
    pc.dim(
      "     docker build -t flink-reactor-s3:2.0.1 -f src/cli/sim/Dockerfile.flink-s3 .",
    ),
  )
  console.log("")
  console.log(
    pc.dim("  2. Build reactor-server image (from flink-reactor-console):"),
  )
  console.log(
    pc.dim("     pnpm build && cp -r dashboard/out/ server/dashboard/"),
  )
  console.log(
    pc.dim("     cd server && docker build -t reactor-server:latest ."),
  )
  console.log("")
  console.log(pc.dim("  3. Access the console:"))
  console.log(
    pc.dim(
      `     kubectl port-forward svc/reactor-server 8080:8080 -n ${opts.namespace}`,
    ),
  )
  console.log(pc.dim("     open http://localhost:8080"))
  console.log("")

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
    const spinner = clack.spinner()
    spinner.start(`Creating ${databases.length} Iceberg database(s)...`)

    let created = 0
    let skipped = 0

    for (const db of databases) {
      try {
        // Use kubectl exec to curl the SQL Gateway REST API from within the cluster
        const sql = `CREATE CATALOG IF NOT EXISTS lakehouse WITH ('type' = 'iceberg', 'catalog-type' = 'rest', 'uri' = 'http://iceberg-rest:8181', 'warehouse' = 's3://flink-state/warehouse', 's3.endpoint' = 'http://seaweedfs.flink-demo.svc:8333', 's3.path-style-access' = 'true', 's3.access-key' = 'admin', 's3.secret-key' = 'admin'); USE CATALOG lakehouse; CREATE DATABASE IF NOT EXISTS ${db};`
        execSqlViaGateway(sql, namespace)
        created++
      } catch {
        skipped++
      }
    }

    spinner.stop(
      pc.green(
        `Iceberg databases: ${created} created${skipped > 0 ? `, ${skipped} skipped` : ""}`,
      ),
    )
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
    spinner.start("Deleting simulation resources...")
    try {
      execSync(`kubectl delete -f "${manifests}" --ignore-not-found`, {
        stdio: "pipe",
      })
      spinner.stop(pc.green("Simulation resources deleted."))
    } catch {
      spinner.stop(pc.yellow("Some resources may already be deleted."))
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

// ── Utilities ────────────────────────────────────────────────────────

function padRight(str: string, len: number): string {
  return str.length >= len
    ? str.substring(0, len)
    : str + " ".repeat(len - str.length)
}
