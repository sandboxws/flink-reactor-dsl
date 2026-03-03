import { execSync } from "node:child_process"
import { existsSync, readdirSync, statSync } from "node:fs"
import { join } from "node:path"
import type { Command } from "commander"
import pc from "picocolors"

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
      await runDoctorCommand()
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

  printResults(results)
  return results
}

function runCommand(cmd: string): string | null {
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
  const output = runCommand("node --version")
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
  const output = runCommand("npx tsc --version")
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
  const output = runCommand("docker --version")
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
  const flinkOutput = runCommand("flink --version")
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
  const dockerOutput = runCommand(
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
  const output = runCommand(
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
