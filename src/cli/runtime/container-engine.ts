// ── Container engine abstraction ─────────────────────────────────────
//
// Selects between docker and podman as the binary that drives compose
// and raw container subcommands. Lives one altitude below RuntimeAdapter:
// adapters decide WHICH orchestrator to drive (compose / kubectl / flink),
// this module decides WHICH BINARY speaks compose for the docker lane.
//
// Resolution precedence (high → low):
//   1. opts.flag — `--container-engine <name>` (passed by command actions)
//   2. FR_CONTAINER_ENGINE env var
//   3. opts.configValue — `EnvironmentEntry.containerEngine`
//   4. auto-detect: try `docker info`, then `podman info`
//
// Podman ≥ 4.7 is required (modern `podman compose` subcommand). Older
// podman and the standalone `podman-compose` Python script are rejected
// with an upgrade message — their compose semantics around `service_healthy`,
// profiles, and `host-gateway` diverge from docker's.

import { execFileSync } from "node:child_process"

export type ContainerEngineName = "docker" | "podman"
export type ContainerEngineChoice = "auto" | ContainerEngineName

export const SUPPORTED_CONTAINER_ENGINES: readonly ContainerEngineName[] = [
  "docker",
  "podman",
]

const DOCKER_INSTALL_URL = "https://docs.docker.com/get-docker/"
const PODMAN_INSTALL_URL = "https://podman.io/docs/installation"
const PODMAN_MIN_MAJOR = 4
const PODMAN_MIN_MINOR = 7

export interface ContainerEngine {
  readonly name: ContainerEngineName
  readonly bin: string
  /** argv for `<bin> compose <args>`; pass to execFileSync. */
  composeArgv(args: readonly string[]): readonly string[]
  /**
   * Build a quoted shell command string for `<bin> compose -f <file> <args>`.
   * Use with execSync where shell quoting matters (e.g. `psql -c "..."`).
   * If `file` is undefined, the `-f` flag is omitted.
   */
  composeCommand(file: string | undefined, args: readonly string[]): string
  /** argv for raw `<bin> <args>` (info, images, pull, run, build, --version). */
  argv(args: readonly string[]): readonly string[]
  /**
   * Env vars to merge into the subprocess env when invoking compose.
   *
   * For podman on macOS, this returns `DOCKER_HOST=unix://<podman-socket>` so
   * the docker-compose plugin (which podman shells out to as the compose
   * provider) talks to the podman API instead of looking for a Docker
   * Desktop socket. For docker, returns an empty object.
   */
  composeEnv(): Record<string, string>
}

export interface DetectOptions {
  readonly flag?: ContainerEngineChoice
  readonly configValue?: ContainerEngineChoice
}

export type EngineSource = "flag" | "env" | "config" | "auto"

export interface ResolvedEngine extends ContainerEngine {
  /** Where the choice came from, for logging / doctor output. */
  readonly source: EngineSource
  /** Engines whose binary responded to a probe. Used by doctor. */
  readonly available: readonly ContainerEngineName[]
}

export class ContainerEngineNotFoundError extends Error {
  readonly tried: readonly ContainerEngineName[]
  constructor(tried: readonly ContainerEngineName[]) {
    const triedList = tried.join(", ")
    const lines = [
      `No container engine is available. Tried: ${triedList}.`,
      `Install Docker (${DOCKER_INSTALL_URL}) or Podman ≥ ${PODMAN_MIN_MAJOR}.${PODMAN_MIN_MINOR} (${PODMAN_INSTALL_URL}).`,
    ]
    super(lines.join("\n"))
    this.name = "ContainerEngineNotFoundError"
    this.tried = tried
  }
}

export class PodmanVersionTooOldError extends Error {
  readonly version: string
  constructor(version: string, detail?: string) {
    const tail = detail ? ` ${detail}` : ""
    super(
      `Podman ${version} detected, but ≥ ${PODMAN_MIN_MAJOR}.${PODMAN_MIN_MINOR} is required ` +
        `for the modern \`podman compose\` subcommand.${tail} ` +
        `Upgrade: ${PODMAN_INSTALL_URL}`,
    )
    this.name = "PodmanVersionTooOldError"
    this.version = version
  }
}

export class PodmanMachineNotRunningError extends Error {
  readonly version: string
  constructor(version: string) {
    super(
      `Podman ${version} is installed, but the podman machine isn't running. ` +
        `Start it with \`podman machine start\` (run \`podman machine init\` first if you haven't created one yet).`,
    )
    this.name = "PodmanMachineNotRunningError"
    this.version = version
  }
}

// ── Probes ──────────────────────────────────────────────────────────

function probeBinary(bin: string, args: readonly string[]): string | null {
  try {
    return execFileSync(bin, args as string[], {
      stdio: ["ignore", "pipe", "pipe"],
      encoding: "utf-8",
      timeout: 5_000,
    }).trim()
  } catch {
    return null
  }
}

function dockerInfoOk(): boolean {
  return probeBinary("docker", ["info"]) !== null
}

function podmanInfoOk(): boolean {
  return probeBinary("podman", ["info"]) !== null
}

/** Returns the version string ("4.9.0", "5.0.0-rc1") or null if podman is absent. */
export function podmanVersion(): string | null {
  const out = probeBinary("podman", ["--version"])
  if (!out) return null
  // e.g. "podman version 4.9.0"
  const match = out.match(/podman\s+version\s+([0-9][0-9.\-a-zA-Z]*)/i)
  return match?.[1] ?? out
}

/** Returns the docker version string or null if docker is absent. */
export function dockerVersion(): string | null {
  const out = probeBinary("docker", ["--version"])
  if (!out) return null
  // e.g. "Docker version 27.2.0, build abc"
  const match = out.match(/Docker\s+version\s+([0-9][0-9.]*)/)
  return match?.[1] ?? out
}

function parseSemver(v: string): readonly [number, number, number] | null {
  const match = v.match(/(\d+)\.(\d+)(?:\.(\d+))?/)
  if (!match) return null
  return [
    parseInt(match[1], 10),
    parseInt(match[2], 10),
    parseInt(match[3] ?? "0", 10),
  ]
}

/**
 * Returns the host-visible docker-API-compatible socket path the podman
 * daemon exposes. We pass this to compose subprocesses via `DOCKER_HOST` so
 * the docker-compose provider (which `podman compose` shells out to on macOS
 * by default) talks to podman instead of looking for a Docker Desktop socket.
 *
 * Two strategies, in order:
 *   1. `podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}'`
 *      — On macOS, podman runs in a Linux VM and `podman info` returns the
 *      VM-internal path (e.g. `/run/user/501/podman/podman.sock`), which
 *      isn't reachable from the host. `machine inspect` returns the
 *      host-visible path (e.g.
 *      `~/.local/share/containers/podman/machine/podman-machine-default/podman.sock`).
 *   2. `podman info -f '{{.Host.RemoteSocket.Path}}'` — On Linux native podman
 *      there's no VM and no machine, so `machine inspect` errors out; `info`
 *      returns the actual local socket path (`$XDG_RUNTIME_DIR/podman/podman.sock`).
 *
 * If a machine has multiple entries (multi-machine setups), we use the first.
 */
function podmanSocketPath(): string | null {
  const fromMachine = probeBinary("podman", [
    "machine",
    "inspect",
    "--format",
    "{{.ConnectionInfo.PodmanSocket.Path}}",
  ])
  if (fromMachine) {
    const firstLine = fromMachine
      .split("\n")
      .map((s) => s.trim())
      .find((s) => s.length > 0)
    if (firstLine) return firstLine
  }
  const fromInfo = probeBinary("podman", [
    "info",
    "-f",
    "{{.Host.RemoteSocket.Path}}",
  ])
  return fromInfo && fromInfo.length > 0 ? fromInfo : null
}

/**
 * Verify podman is usable: binary present, version ≥ 4.7, daemon responds.
 * Returns the resolved version + (best-effort) socket path on success, or
 * throws one of the specific errors so the caller can render a targeted
 * message.
 *
 * The socket path is best-effort: when present it gets baked into
 * `composeEnv()` as `DOCKER_HOST`. If we can't determine it (unusual setup),
 * we still proceed — the user may have configured podman differently.
 */
function checkPodmanOrThrow(): { version: string; socketPath: string | null } {
  const version = podmanVersion()
  if (!version) {
    throw new ContainerEngineNotFoundError(["podman"])
  }
  const parts = parseSemver(version)
  if (parts) {
    const [major, minor] = parts
    if (
      major < PODMAN_MIN_MAJOR ||
      (major === PODMAN_MIN_MAJOR && minor < PODMAN_MIN_MINOR)
    ) {
      throw new PodmanVersionTooOldError(version)
    }
  }
  if (!podmanInfoOk()) {
    throw new PodmanMachineNotRunningError(version)
  }
  return { version, socketPath: podmanSocketPath() }
}

// ── Engine builder ──────────────────────────────────────────────────

function buildEngine(
  name: ContainerEngineName,
  opts?: { socketPath?: string },
): ContainerEngine {
  const env: Record<string, string> =
    name === "podman" && opts?.socketPath
      ? { DOCKER_HOST: `unix://${opts.socketPath}` }
      : {}
  return {
    name,
    bin: name,
    composeArgv: (args) => ["compose", ...args],
    composeCommand: (file, args) => {
      const parts = [name, "compose"]
      if (file !== undefined) {
        parts.push("-f", `"${file}"`)
      }
      parts.push(...args)
      return parts.join(" ")
    },
    argv: (args) => [...args],
    composeEnv: () => ({ ...env }),
  }
}

// ── Choice resolution ───────────────────────────────────────────────

interface ResolvedChoice {
  readonly value: ContainerEngineChoice
  readonly source: EngineSource
}

function isValidChoice(value: string): value is ContainerEngineChoice {
  return value === "auto" || value === "docker" || value === "podman"
}

function readEnvVar(): ContainerEngineChoice | null {
  const raw = process.env.FR_CONTAINER_ENGINE?.trim()
  if (!raw) return null
  if (!isValidChoice(raw)) {
    throw new Error(
      `Invalid FR_CONTAINER_ENGINE='${raw}'. Expected one of: auto, docker, podman.`,
    )
  }
  return raw
}

function resolveChoice(opts?: DetectOptions): ResolvedChoice {
  if (opts?.flag) {
    if (!isValidChoice(opts.flag)) {
      throw new Error(
        `Invalid --container-engine='${opts.flag}'. Expected: auto, docker, or podman.`,
      )
    }
    if (opts.flag !== "auto") {
      return { value: opts.flag, source: "flag" }
    }
  }
  const envVal = readEnvVar()
  if (envVal && envVal !== "auto") {
    return { value: envVal, source: "env" }
  }
  if (opts?.configValue && opts.configValue !== "auto") {
    return { value: opts.configValue, source: "config" }
  }
  return { value: "auto", source: "auto" }
}

// ── Main entry point ────────────────────────────────────────────────

let cached: ResolvedEngine | null = null

/**
 * Resolve the container engine according to the precedence chain.
 *
 * @throws ContainerEngineNotFoundError when no engine is available.
 * @throws PodmanVersionTooOldError when podman is selected but < 4.7.
 * @throws Error for invalid flag/env values.
 */
export function detectContainerEngine(opts?: DetectOptions): ResolvedEngine {
  const useCache = !opts?.flag && !opts?.configValue
  if (useCache && cached) return cached

  const choice = resolveChoice(opts)

  // Probe state once. "Available" requires the engine's daemon to actually
  // respond — for podman that means the machine is running, not just that
  // the binary is on PATH.
  const dockerOk = dockerInfoOk()
  const podmanBinaryVersion = podmanVersion()
  const podmanRunning = podmanBinaryVersion ? podmanInfoOk() : false
  const available: ContainerEngineName[] = []
  if (dockerOk) available.push("docker")
  if (podmanRunning) available.push("podman")

  let engine: ContainerEngine
  if (choice.value === "docker") {
    if (!dockerOk) throw new ContainerEngineNotFoundError(["docker"])
    engine = buildEngine("docker")
  } else if (choice.value === "podman") {
    const { socketPath } = checkPodmanOrThrow()
    engine = buildEngine("podman", { socketPath: socketPath ?? undefined })
  } else {
    if (dockerOk) {
      engine = buildEngine("docker")
    } else if (podmanRunning) {
      // Verify version while we're here.
      const parts = podmanBinaryVersion
        ? parseSemver(podmanBinaryVersion)
        : null
      if (parts) {
        const [major, minor] = parts
        if (
          major < PODMAN_MIN_MAJOR ||
          (major === PODMAN_MIN_MAJOR && minor < PODMAN_MIN_MINOR)
        ) {
          throw new PodmanVersionTooOldError(podmanBinaryVersion ?? "(unknown)")
        }
      }
      // Compute the host-visible socket only when we know the daemon
      // responds — saves a `podman machine inspect` call on the docker path.
      const socketPath = podmanSocketPath()
      engine = buildEngine("podman", {
        socketPath: socketPath ?? undefined,
      })
    } else if (podmanBinaryVersion) {
      // Binary is here but the machine isn't responding — give a targeted
      // error rather than the generic "neither available" message.
      throw new PodmanMachineNotRunningError(podmanBinaryVersion)
    } else {
      throw new ContainerEngineNotFoundError(["docker", "podman"])
    }
  }

  const resolved: ResolvedEngine = {
    ...engine,
    source: choice.source,
    available,
  }
  if (useCache) cached = resolved
  return resolved
}

/** Reset the module-local cache. Primarily for tests. */
export function resetContainerEngineCache(): void {
  cached = null
}
