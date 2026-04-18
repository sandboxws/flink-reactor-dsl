import { spawn } from "node:child_process"
import {
  existsSync,
  readdirSync,
  readFileSync,
  rmSync,
  statSync,
} from "node:fs"
import { join } from "node:path"
import type { Paths } from "./paths.js"
import * as ui from "./ui.js"
import { ensureVerdaccio } from "./verdaccio.js"

export type CleanMode = "unpublish" | "wipe" | "none"

export interface PublishOpts {
  paths: Paths
  registry: string
  distTag: string | undefined
  cleanMode: CleanMode
  build: boolean
  verbose: boolean
  port?: number
}

export interface PackageRef {
  name: string
  dir: string
  kind: "root" | "workspace"
}

export function discoverPackages(projectRoot: string): PackageRef[] {
  const packages: PackageRef[] = []
  const root = JSON.parse(
    readFileSync(join(projectRoot, "package.json"), "utf8"),
  ) as {
    name: string
    private?: boolean
  }
  if (!root.private) {
    packages.push({ name: root.name, dir: projectRoot, kind: "root" })
  }

  const packagesDir = join(projectRoot, "packages")
  if (existsSync(packagesDir)) {
    for (const entry of readdirSync(packagesDir)) {
      const dir = join(packagesDir, entry)
      const pkgPath = join(dir, "package.json")
      if (!statSync(dir).isDirectory() || !existsSync(pkgPath)) continue
      const pkg = JSON.parse(readFileSync(pkgPath, "utf8")) as {
        name?: string
        private?: boolean
      }
      if (!pkg.name || pkg.private) continue
      packages.push({ name: pkg.name, dir, kind: "workspace" })
    }
  }
  return packages
}

function run(
  command: string,
  args: string[],
  options: { cwd: string; verbose?: boolean },
): Promise<{ code: number; stdout: string; stderr: string }> {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd: options.cwd,
      stdio: options.verbose ? "inherit" : ["ignore", "pipe", "pipe"],
      env: process.env,
    })
    let stdout = ""
    let stderr = ""
    if (!options.verbose) {
      child.stdout?.on("data", (chunk) => {
        stdout += chunk.toString()
      })
      child.stderr?.on("data", (chunk) => {
        stderr += chunk.toString()
      })
    }
    child.on("error", reject)
    child.on("close", (code) => {
      resolve({ code: code ?? -1, stdout, stderr })
    })
  })
}

function failureMessage(
  label: string,
  code: number,
  stdout: string,
  stderr: string,
): string {
  const parts = [`${label} failed (exit ${code})`]
  const tail = (s: string, n = 40): string => {
    const lines = s.trim().split("\n")
    return lines.slice(-n).join("\n")
  }
  if (stderr.trim()) parts.push(`\n--- stderr ---\n${tail(stderr)}`)
  else if (stdout.trim()) parts.push(`\n--- stdout ---\n${tail(stdout)}`)
  return parts.join("")
}

export async function cleanPackage(
  pkg: PackageRef,
  opts: PublishOpts,
): Promise<void> {
  if (opts.cleanMode === "none") return
  if (opts.cleanMode === "wipe") {
    // Wipe the storage directory for this package scope.
    const [scope] = pkg.name.split("/")
    const scopeDir = join(opts.paths.storageDir, scope)
    if (existsSync(scopeDir)) {
      rmSync(scopeDir, { recursive: true, force: true })
    }
    return
  }

  // unpublish mode
  const { code, stdout, stderr } = await run(
    "npm",
    ["unpublish", pkg.name, "--registry", opts.registry, "--force"],
    { cwd: opts.paths.projectRoot, verbose: opts.verbose },
  )
  if (code === 0) return

  // Tolerate "not found" — first publish will hit this.
  const combined = stderr.toLowerCase()
  if (
    combined.includes("404") ||
    combined.includes("not found") ||
    combined.includes("no such package")
  ) {
    return
  }
  throw new Error(
    failureMessage(`npm unpublish ${pkg.name}`, code, stdout, stderr),
  )
}

export async function publishPackage(
  pkg: PackageRef,
  opts: PublishOpts,
): Promise<void> {
  const args = ["publish", "--registry", opts.registry, "--provenance=false"]
  if (opts.distTag) args.push("--tag", opts.distTag)
  const { code, stdout, stderr } = await run("npm", args, {
    cwd: pkg.dir,
    verbose: opts.verbose,
  })
  if (code !== 0) {
    throw new Error(
      failureMessage(`npm publish ${pkg.name}`, code, stdout, stderr),
    )
  }
}

export async function runPublishFlow(
  opts: PublishOpts,
): Promise<{ startedByUs: boolean }> {
  if (opts.build) {
    const s = ui.spinner()
    s.start("Building packages")
    const { code, stdout, stderr } = await run("pnpm", ["build"], {
      cwd: opts.paths.projectRoot,
      verbose: opts.verbose,
    })
    if (code !== 0) {
      s.stop("Build failed", 1)
      throw new Error(failureMessage("pnpm build", code, stdout, stderr))
    }
    s.stop("Build complete")
  }

  const s = ui.spinner()
  s.start("Ensuring Verdaccio is running")
  const { pid, startedByUs, port } = await ensureVerdaccio({
    paths: opts.paths,
    port: opts.port,
  })
  s.stop(
    startedByUs
      ? `Verdaccio started (pid=${pid}, port=${port})`
      : `Verdaccio already running (pid=${pid || "?"}, port=${port})`,
  )

  const packages = discoverPackages(opts.paths.projectRoot)
  // workspaces first, root last (matches current script's ordering)
  packages.sort((a, b) =>
    a.kind === b.kind ? 0 : a.kind === "workspace" ? -1 : 1,
  )

  for (const pkg of packages) {
    const ps = ui.spinner()
    ps.start(`Publishing ${pkg.name}`)
    try {
      if (opts.cleanMode !== "none") {
        await cleanPackage(pkg, opts)
      }
      await publishPackage(pkg, opts)
      ps.stop(
        `Published ${pkg.name}${opts.distTag ? ` (tag: ${opts.distTag})` : ""}`,
      )
    } catch (err) {
      ps.stop(`Failed to publish ${pkg.name}`, 1)
      throw err
    }
  }

  return { startedByUs }
}
