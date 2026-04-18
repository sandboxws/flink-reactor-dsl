import { spawn } from "node:child_process"
import {
  existsSync,
  mkdirSync,
  openSync,
  readFileSync,
  unlinkSync,
  writeFileSync,
} from "node:fs"
import { setTimeout as sleep } from "node:timers/promises"
import type { Paths } from "./paths.js"

export interface VerdaccioOpts {
  paths: Paths
  port?: number
  readinessTimeoutMs?: number
}

export interface VerdaccioStatus {
  running: boolean
  pid?: number
  port: number
  pinged: boolean
}

const DEFAULT_PORT = 4873
const DEFAULT_READINESS_TIMEOUT_MS = 30_000

function isProcessAlive(pid: number): boolean {
  try {
    process.kill(pid, 0)
    return true
  } catch {
    return false
  }
}

function readPidFile(pidFile: string): number | undefined {
  if (!existsSync(pidFile)) return undefined
  const raw = readFileSync(pidFile, "utf8").trim()
  const pid = Number.parseInt(raw, 10)
  return Number.isFinite(pid) && pid > 0 ? pid : undefined
}

async function probePing(port: number, timeoutMs = 2000): Promise<boolean> {
  try {
    const res = await fetch(`http://localhost:${port}/-/ping`, {
      signal: AbortSignal.timeout(timeoutMs),
    })
    return res.ok
  } catch {
    return false
  }
}

export async function waitUntilReady(
  port: number,
  timeoutMs: number,
): Promise<void> {
  const deadline = Date.now() + timeoutMs
  let delay = 250
  while (Date.now() < deadline) {
    if (await probePing(port, 1000)) return
    await sleep(delay)
    delay = Math.min(delay * 2, 1000)
  }
  throw new Error(
    `Verdaccio /-/ping on port ${port} did not respond within ${timeoutMs}ms`,
  )
}

export async function isVerdaccioRunning(
  opts: VerdaccioOpts,
): Promise<VerdaccioStatus> {
  const port = opts.port ?? DEFAULT_PORT
  const pid = readPidFile(opts.paths.pidFile)
  const running = pid !== undefined && isProcessAlive(pid)
  const pinged = await probePing(port, 500)
  return { running, pid: running ? pid : undefined, port, pinged }
}

export async function ensureVerdaccio(
  opts: VerdaccioOpts,
): Promise<{ pid: number; startedByUs: boolean; port: number }> {
  const port = opts.port ?? DEFAULT_PORT
  const readinessTimeoutMs =
    opts.readinessTimeoutMs ?? DEFAULT_READINESS_TIMEOUT_MS

  const existingPid = readPidFile(opts.paths.pidFile)
  if (existingPid !== undefined) {
    if (isProcessAlive(existingPid) && (await probePing(port))) {
      return { pid: existingPid, startedByUs: false, port }
    }
    // stale PID file
    try {
      unlinkSync(opts.paths.pidFile)
    } catch {
      /* ignore */
    }
  }

  // Another Verdaccio (not managed by us) may already own the port — reuse it.
  if (await probePing(port)) {
    return { pid: 0, startedByUs: false, port }
  }

  mkdirSync(opts.paths.verdaccioDir, { recursive: true })
  const logFd = openSync(opts.paths.logFile, "a")

  const child = spawn(
    "node_modules/.bin/verdaccio",
    ["--config", opts.paths.verdaccioConfig, "--listen", String(port)],
    {
      cwd: opts.paths.projectRoot,
      detached: true,
      stdio: ["ignore", logFd, logFd],
      env: { ...process.env },
    },
  )

  if (child.pid === undefined) {
    throw new Error("Failed to spawn Verdaccio (no PID assigned)")
  }

  const pid = child.pid
  writeFileSync(opts.paths.pidFile, String(pid))
  child.unref()

  try {
    await waitUntilReady(port, readinessTimeoutMs)
  } catch (err) {
    throw new Error(`${(err as Error).message}. See log: ${opts.paths.logFile}`)
  }

  return { pid, startedByUs: true, port }
}

export async function stopVerdaccio(
  opts: VerdaccioOpts,
): Promise<{ stopped: boolean; pid?: number }> {
  const pid = readPidFile(opts.paths.pidFile)
  if (pid === undefined) return { stopped: false }
  if (!isProcessAlive(pid)) {
    try {
      unlinkSync(opts.paths.pidFile)
    } catch {
      /* ignore */
    }
    return { stopped: false, pid }
  }

  try {
    process.kill(pid, "SIGTERM")
  } catch {
    /* ignore — may have died between checks */
  }

  const deadline = Date.now() + 5_000
  while (Date.now() < deadline) {
    if (!isProcessAlive(pid)) break
    await sleep(200)
  }

  if (isProcessAlive(pid)) {
    try {
      process.kill(pid, "SIGKILL")
    } catch {
      /* ignore */
    }
  }

  try {
    unlinkSync(opts.paths.pidFile)
  } catch {
    /* ignore */
  }

  return { stopped: true, pid }
}
