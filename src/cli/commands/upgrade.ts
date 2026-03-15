import { execSync } from "node:child_process"
import { existsSync, readFileSync } from "node:fs"
import { join } from "node:path"
import type { Command } from "commander"
import { Effect } from "effect"
import pc from "picocolors"
import { runCommand } from "@/cli/effect-runner.js"
import { CliError } from "@/core/errors.js"
import type { PackageManager } from "./new.js"

// ── Shorthand mapping ────────────────────────────────────────────────

const SHORTHANDS: Record<string, string> = {}

// ── Command registration ────────────────────────────────────────────

export function registerUpgradeCommand(program: Command): void {
  program
    .command("upgrade")
    .argument("[package]", "Package to upgrade")
    .option("--dry-run", "Show what would be upgraded without installing")
    .description("Upgrade FlinkReactor packages to their latest versions")
    .action(async (pkg: string | undefined, opts: { dryRun?: boolean }) => {
      await runCommand(
        Effect.tryPromise({
          try: () => runUpgrade(pkg, opts),
          catch: (err) =>
            new CliError({
              reason: "invalid_args",
              message: (err as Error).message,
            }),
        }),
      )
    })
}

// ── Package manager detection ───────────────────────────────────────

export function detectPackageManager(projectDir: string): PackageManager {
  if (existsSync(join(projectDir, "pnpm-lock.yaml"))) return "pnpm"
  if (existsSync(join(projectDir, "yarn.lock"))) return "yarn"
  return "npm"
}

// ── Shorthand resolution ────────────────────────────────────────────

export function resolvePackageName(input: string): string {
  return SHORTHANDS[input] ?? input
}

// ── Version lookup ──────────────────────────────────────────────────

function getInstalledVersion(projectDir: string, pkg: string): string | null {
  const pkgJsonPath = join(projectDir, "node_modules", pkg, "package.json")
  if (!existsSync(pkgJsonPath)) return null

  try {
    const content = JSON.parse(readFileSync(pkgJsonPath, "utf-8"))
    return content.version ?? null
  } catch {
    return null
  }
}

function getLatestVersion(pkg: string): string | null {
  try {
    return execSync(`npm view ${pkg} version`, {
      stdio: "pipe",
      encoding: "utf-8",
    }).trim()
  } catch {
    return null
  }
}

// ── Discover FlinkReactor packages ──────────────────────────────────

function discoverFlinkReactorPackages(projectDir: string): string[] {
  const pkgJsonPath = join(projectDir, "package.json")
  if (!existsSync(pkgJsonPath)) return []

  try {
    const content = JSON.parse(readFileSync(pkgJsonPath, "utf-8"))
    const allDeps = {
      ...content.dependencies,
      ...content.devDependencies,
    }

    return Object.keys(allDeps).filter(
      (name) => name === "flink-reactor" || name.startsWith("@flink-reactor/"),
    )
  } catch {
    return []
  }
}

// ── Upgrade logic ───────────────────────────────────────────────────

function getUpdateCommand(pm: PackageManager, packages: string[]): string {
  const pkgList = packages.join(" ")
  switch (pm) {
    case "pnpm":
      return `pnpm update ${pkgList}`
    case "yarn":
      return `yarn upgrade ${pkgList}`
    case "npm":
      return `npm update ${pkgList}`
  }
}

export async function runUpgrade(
  packageArg: string | undefined,
  opts: { dryRun?: boolean; projectDir?: string },
): Promise<void> {
  const projectDir = opts.projectDir ?? process.cwd()
  const pm = detectPackageManager(projectDir)

  // Resolve packages to upgrade
  let packages: string[]
  if (packageArg) {
    packages = [resolvePackageName(packageArg)]
  } else {
    packages = discoverFlinkReactorPackages(projectDir)
    if (packages.length === 0) {
      console.log(pc.yellow("No FlinkReactor packages found in package.json."))
      return
    }
  }

  console.log(pc.bold("\n  flink-reactor upgrade\n"))
  console.log(pc.dim(`  Package manager: ${pm}\n`))

  // Show version info
  let hasUpgrades = false
  for (const pkg of packages) {
    const installed = getInstalledVersion(projectDir, pkg)
    const latest = getLatestVersion(pkg)

    if (!latest) {
      console.log(`  ${pc.yellow("?")} ${pkg} — not found in registry`)
      continue
    }

    if (!installed) {
      console.log(
        `  ${pc.green("+")} ${pkg} ${pc.dim("not installed")} → ${pc.green(latest)}`,
      )
      hasUpgrades = true
    } else if (installed !== latest) {
      console.log(
        `  ${pc.yellow("↑")} ${pkg} ${pc.dim(installed)} → ${pc.green(latest)}`,
      )
      hasUpgrades = true
    } else {
      console.log(
        `  ${pc.green("✓")} ${pkg} ${pc.dim(installed)} ${pc.dim("(up to date)")}`,
      )
    }
  }

  console.log("")

  if (!hasUpgrades) {
    console.log(pc.green("All packages are up to date."))
    return
  }

  if (opts.dryRun) {
    const cmd = getUpdateCommand(pm, packages)
    console.log(pc.dim(`Would run: ${cmd}`))
    return
  }

  // Run the update
  const cmd = getUpdateCommand(pm, packages)
  console.log(pc.dim(`Running: ${cmd}\n`))

  try {
    execSync(cmd, { cwd: projectDir, stdio: "inherit" })
    console.log(pc.green("\nPackages upgraded successfully."))
  } catch {
    console.error(
      pc.red("\nUpgrade failed. Check the output above for details."),
    )
    process.exitCode = 1
  }
}
