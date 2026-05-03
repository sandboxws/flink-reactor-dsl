import { execFileSync, execSync } from "node:child_process"
import { writeFileSync } from "node:fs"
import { arch, platform } from "node:os"
import { join } from "node:path"
import * as clack from "@clack/prompts"
import { type Command, Option } from "commander"
import { Effect } from "effect"
import pc from "picocolors"
import { runCommand } from "@/cli/effect-runner.js"
import {
  type ContainerEngineName,
  detectContainerEngine,
  dockerVersion,
  podmanVersion,
} from "@/cli/runtime/container-engine.js"
import { CliError } from "@/core/errors.js"

export type InstallMethod = "docker" | "homebrew" | "binary"

export interface PlatformInfo {
  os: "macos" | "linux" | "windows" | "unknown"
  arch: string
  hasDocker: boolean
  hasPodman: boolean
  /** Resolved engine (auto-detected) — null when neither docker nor podman is available. */
  containerEngine: ContainerEngineName | null
  hasHomebrew: boolean
  existingFlink: string | null
}

export function registerInstallCommand(program: Command): void {
  const install = program
    .command("install")
    .description("Install tools for FlinkReactor development")

  install
    .command("flink")
    .description("Install Apache Flink locally")
    .addOption(
      new Option("--method <method>", "Installation method").choices([
        "docker",
        "homebrew",
        "binary",
      ]),
    )
    .option("--flink-version <version>", "Flink version (default: 1.20)")
    .action(async (opts: Record<string, unknown>) => {
      await runCommand(
        Effect.tryPromise({
          try: () => runInstallFlink(opts),
          catch: (err) =>
            new CliError({
              reason: "invalid_args",
              message: (err as Error).message,
            }),
        }),
      )
    })
}

export function detectPlatform(): PlatformInfo {
  const os = mapPlatform(platform())
  const archName = arch()
  const hasDocker = dockerVersion() !== null
  const hasPodman = podmanVersion() !== null
  let containerEngine: ContainerEngineName | null = null
  try {
    containerEngine = detectContainerEngine().name
  } catch {
    containerEngine = null
  }
  const hasHomebrew = os === "macos" && commandExists("brew")
  const existingFlink = detectExistingFlink(containerEngine)

  return {
    os,
    arch: archName,
    hasDocker,
    hasPodman,
    containerEngine,
    hasHomebrew,
    existingFlink,
  }
}

function mapPlatform(p: string): PlatformInfo["os"] {
  switch (p) {
    case "darwin":
      return "macos"
    case "linux":
      return "linux"
    case "win32":
      return "windows"
    default:
      return "unknown"
  }
}

function commandExists(cmd: string): boolean {
  try {
    execSync(`which ${cmd}`, { stdio: "pipe" })
    return true
  } catch {
    return false
  }
}

function detectExistingFlink(
  engine: ContainerEngineName | null,
): string | null {
  try {
    const output = execSync("flink --version 2>/dev/null", {
      encoding: "utf-8",
    }).trim()
    const match = output.match(/Version:\s*([\d.]+)/)
    return match?.[1] ?? output
  } catch {
    if (!engine) return null
    try {
      const output = execFileSync(
        engine,
        ["images", "--format", "{{.Repository}}:{{.Tag}}"],
        { encoding: "utf-8", stdio: ["ignore", "pipe", "pipe"] },
      ).trim()
      const flinkImage = output
        .split("\n")
        .find((line) => line.includes("flink"))
      return flinkImage ? `${engine}: ${flinkImage}` : null
    } catch {
      return null
    }
  }
}

export async function runInstallFlink(
  opts: Record<string, unknown>,
): Promise<void> {
  const info = detectPlatform()
  const flinkVersion = (opts.flinkVersion as string) ?? "1.20"

  clack.intro(pc.bgCyan(pc.black(" flink-reactor install flink ")))

  console.log(`  ${pc.dim("OS:")} ${info.os} (${info.arch})`)
  console.log(
    `  ${pc.dim("Docker:")} ${info.hasDocker ? "available" : "not found"}`,
  )
  console.log(
    `  ${pc.dim("Podman:")} ${info.hasPodman ? "available" : "not found"}`,
  )
  if (info.os === "macos") {
    console.log(
      `  ${pc.dim("Homebrew:")} ${info.hasHomebrew ? "available" : "not found"}`,
    )
  }
  console.log("")

  if (info.existingFlink) {
    const proceed = await clack.confirm({
      message: `Flink already detected: ${info.existingFlink}. Continue anyway?`,
      initialValue: false,
    })

    if (clack.isCancel(proceed) || !proceed) {
      clack.cancel("Installation cancelled.")
      return
    }
  }

  let method: InstallMethod

  if (opts.method) {
    method = opts.method as InstallMethod
  } else {
    const options: Array<{
      value: InstallMethod
      label: string
      hint?: string
    }> = []

    if (info.containerEngine) {
      options.push({
        value: "docker",
        label:
          info.containerEngine === "docker"
            ? "Docker (recommended)"
            : "Podman (recommended)",
        hint: "docker-compose.flink.yml",
      })
    }
    if (info.hasHomebrew) {
      options.push({
        value: "homebrew",
        label: "Homebrew",
        hint: "brew install apache-flink",
      })
    }
    options.push({
      value: "binary",
      label: "Binary download",
      hint: "Download from Apache mirrors",
    })

    if (options.length === 0) {
      console.error(
        pc.red(
          "No installation methods available. Install Docker or Podman ≥ 4.7 first.",
        ),
      )
      process.exitCode = 1
      return
    }

    const selected = await clack.select({
      message: "How would you like to install Flink?",
      options,
    })

    if (clack.isCancel(selected)) {
      clack.cancel("Installation cancelled.")
      return
    }

    method = selected as InstallMethod
  }

  switch (method) {
    case "docker":
      await installViaDocker(flinkVersion)
      break
    case "homebrew":
      await installViaHomebrew()
      break
    case "binary":
      await installViaBinary(flinkVersion)
      break
  }
}

async function installViaDocker(flinkVersion: string): Promise<void> {
  const spinner = clack.spinner()

  let engine: ContainerEngineName
  try {
    engine = detectContainerEngine().name
  } catch (err) {
    spinner.stop(pc.red((err as Error).message))
    process.exitCode = 1
    return
  }

  const composeContent = generateDockerCompose(flinkVersion)
  const composePath = join(process.cwd(), "docker-compose.flink.yml")

  writeFileSync(composePath, composeContent, "utf-8")
  console.log(
    `  ${pc.green("✓")} Created ${pc.dim("docker-compose.flink.yml")}`,
  )

  spinner.start(`Pulling Flink image via ${engine}...`)
  try {
    execFileSync(engine, ["pull", `flink:${flinkVersion}`], { stdio: "pipe" })
    spinner.stop(`Flink image pulled (via ${engine}).`)
  } catch {
    spinner.stop(
      pc.yellow("Failed to pull image. You can pull it manually later."),
    )
    return
  }

  // Verify
  spinner.start("Verifying installation...")
  try {
    const output = execFileSync(
      engine,
      ["run", "--rm", `flink:${flinkVersion}`, "flink", "--version"],
      { encoding: "utf-8", stdio: ["pipe", "pipe", "pipe"] },
    ).trim()
    spinner.stop(`Verified: ${output}`)
  } catch {
    spinner.stop(pc.yellow("Image pulled but verification failed."))
  }

  clack.outro(pc.green(`Flink installed via ${engine}!`))
  console.log("")
  console.log(
    `  Start the cluster: ${pc.dim(`${engine} compose -f docker-compose.flink.yml up -d`)}`,
  )
  console.log(`  Flink UI: ${pc.dim("http://localhost:8081")}`)
  console.log("")
}

async function installViaHomebrew(): Promise<void> {
  const spinner = clack.spinner()
  spinner.start("Installing Apache Flink via Homebrew...")

  try {
    execSync("brew install apache-flink", { stdio: "pipe" })
    spinner.stop("Apache Flink installed via Homebrew.")
  } catch {
    spinner.stop(pc.red("Homebrew installation failed."))
    console.log(`  ${pc.dim("Try manually:")} brew install apache-flink`)
    process.exitCode = 1
    return
  }

  clack.outro(pc.green("Flink installed!"))
}

async function installViaBinary(flinkVersion: string): Promise<void> {
  const url = `https://dlcdn.apache.org/flink/flink-${flinkVersion}.1/flink-${flinkVersion}.1-bin-scala_2.12.tgz`

  clack.outro("Download Flink manually:")
  console.log("")
  console.log(`  ${pc.dim("URL:")} ${url}`)
  console.log(
    `  ${pc.dim("Extract:")} tar -xzf flink-${flinkVersion}.1-bin-scala_2.12.tgz`,
  )
  console.log(
    `  ${pc.dim("Add to PATH:")} export PATH=$PATH:$(pwd)/flink-${flinkVersion}.1/bin`,
  )
  console.log("")
}

export function generateDockerCompose(flinkVersion: string): string {
  return `# Generated by flink-reactor install flink
# Start: docker compose -f docker-compose.flink.yml up -d
#    or: podman compose -f docker-compose.flink.yml up -d
# Stop:  docker compose -f docker-compose.flink.yml down
#    or: podman compose -f docker-compose.flink.yml down

services:
  jobmanager:
    image: flink:${flinkVersion}
    ports:
      - "8081:8081"
    command: jobmanager
    environment:
      FLINK_PROPERTIES: |
        jobmanager.rpc.address: jobmanager

  taskmanager:
    image: flink:${flinkVersion}
    depends_on:
      - jobmanager
    command: taskmanager
    environment:
      FLINK_PROPERTIES: |
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 4
    deploy:
      replicas: 1
`
}
