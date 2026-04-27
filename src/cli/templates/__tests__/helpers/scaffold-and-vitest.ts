import { spawn } from "node:child_process"
import { existsSync, mkdtempSync, symlinkSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import type { ScaffoldOptions, TemplateName } from "@/cli/commands/new.js"
import { scaffoldProject } from "@/cli/commands/new.js"
import {
  ensureBuilt,
  linkDslPackage,
  REPO_ROOT_PATH,
} from "./scaffold-and-synth.js"

export interface VitestRunResult {
  readonly exitCode: number
  readonly stdout: string
  readonly stderr: string
  readonly tempRoot: string
  readonly projectDir: string
}

/**
 * Scaffolds `template` into a fresh temp dir, links the built dsl plus all
 * other repo deps (vitest, vite, typescript, …) so the scaffolded project's
 * `vitest.config.ts` can resolve `vitest/config`, then spawns the repo's
 * vitest binary in the temp dir as a subprocess and captures the result.
 *
 * Subprocess (not `startVitest()`) for isolation: the outer test is itself
 * running under vitest, and a child Vitest instance in the same Node VM
 * would share the module cache with the parent — including the built dsl,
 * which is supposed to be an *external* package from the scaffolded
 * project's perspective.
 */
export async function scaffoldAndRunVitest(
  template: TemplateName,
  overrides?: Partial<ScaffoldOptions>,
): Promise<VitestRunResult> {
  ensureBuilt()

  const tempRoot = mkdtempSync(join(tmpdir(), "fr-scaffold-vitest-"))
  const projectDir = join(tempRoot, "app")

  const opts: ScaffoldOptions = {
    projectName: "scaffold-vitest-test",
    template,
    pm: "pnpm",
    flinkVersion: "2.0",
    gitInit: false,
    installDeps: false,
    ...overrides,
  }

  scaffoldProject(projectDir, opts)
  linkDslPackage(projectDir)
  linkRepoDeps(projectDir)

  const { exitCode, stdout, stderr } = await runVitest(projectDir, 45_000)
  return { exitCode, stdout, stderr, tempRoot, projectDir }
}

/**
 * Symlinks the minimum set of repo deps the scaffolded project needs at
 * runtime: vitest itself, plus `.pnpm` so vitest's transitive imports
 * resolve through pnpm's flat store. Anything vitest pulls in transitively
 * (vite, esbuild, @vitest/*) lives under `.pnpm` and resolves through the
 * pnpm symlink chain — so we don't need to fan out every top-level entry.
 *
 * Kept minimal to avoid filesystem contention when this test runs in
 * parallel with the rest of the suite; the previous fan-out approach
 * pushed sibling scaffold-synth tests over their default 5s timeout.
 */
function linkRepoDeps(projectDir: string): void {
  const projNm = join(projectDir, "node_modules")
  const repoNm = join(REPO_ROOT_PATH, "node_modules")

  const required = ["vitest", ".pnpm"]
  for (const entry of required) {
    const target = join(projNm, entry)
    if (existsSync(target)) continue
    symlinkSync(join(repoNm, entry), target)
  }
}

function runVitest(
  cwd: string,
  timeoutMs: number,
): Promise<{ exitCode: number; stdout: string; stderr: string }> {
  const vitestBin = join(REPO_ROOT_PATH, "node_modules", ".bin", "vitest")

  return new Promise((resolveP, rejectP) => {
    const child = spawn(vitestBin, ["run", "--reporter=basic"], {
      cwd,
      env: {
        ...process.env,
        NODE_ENV: "test",
        NO_COLOR: "1",
        FORCE_COLOR: "0",
      },
      stdio: ["ignore", "pipe", "pipe"],
    })

    let stdout = ""
    let stderr = ""
    child.stdout?.on("data", (chunk) => {
      stdout += String(chunk)
    })
    child.stderr?.on("data", (chunk) => {
      stderr += String(chunk)
    })

    const timer = setTimeout(() => {
      child.kill("SIGKILL")
      rejectP(
        new Error(
          `vitest run timed out after ${timeoutMs}ms in ${cwd}\n--- stdout ---\n${stdout}\n--- stderr ---\n${stderr}`,
        ),
      )
    }, timeoutMs)

    child.on("close", (code) => {
      clearTimeout(timer)
      resolveP({ exitCode: code ?? -1, stdout, stderr })
    })

    child.on("error", (err) => {
      clearTimeout(timer)
      rejectP(err)
    })
  })
}
