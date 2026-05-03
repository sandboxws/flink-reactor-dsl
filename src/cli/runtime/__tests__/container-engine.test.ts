import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"

// Mock node:child_process before any module that imports it.
// We capture invocations and let each test program responses per-call.
const calls: Array<{ bin: string; args: readonly string[] }> = []
let responses: Map<string, string | Error> = new Map()

vi.mock("node:child_process", () => ({
  execFileSync: (bin: string, args: readonly string[]) => {
    calls.push({ bin, args })
    const key = `${bin} ${args.join(" ")}`
    const r = responses.get(key)
    if (r instanceof Error) throw r
    if (r === undefined) {
      const err = new Error(`spawn ${bin} ENOENT`) as NodeJS.ErrnoException
      err.code = "ENOENT"
      throw err
    }
    return r
  },
}))

// Imported after vi.mock so the module under test sees the mocked version.
import {
  ContainerEngineNotFoundError,
  detectContainerEngine,
  PodmanMachineNotRunningError,
  PodmanVersionTooOldError,
  resetContainerEngineCache,
} from "../container-engine.js"

function setEngineResponses(cfg: {
  docker?: { info?: boolean; version?: string }
  podman?: {
    version?: string
    machineRunning?: boolean
    socketPath?: string
    /**
     * Where the socket path comes from:
     *   "machine" (default, macOS): `podman machine inspect --format ...`
     *   "info"    (Linux native):   `podman info -f ...`
     *   "none":   neither responds (rare misconfiguration)
     */
    socketSource?: "machine" | "info" | "none"
  }
}) {
  responses = new Map()
  if (cfg.docker?.info) responses.set("docker info", "Server: Docker")
  if (cfg.docker?.version)
    responses.set("docker --version", `Docker version ${cfg.docker.version}`)
  if (cfg.podman?.version)
    responses.set("podman --version", `podman version ${cfg.podman.version}`)
  if (cfg.podman?.machineRunning) {
    responses.set("podman info", "Server:\n  Version: 5.0.0")
    if (cfg.podman.socketSource !== "none") {
      const path = cfg.podman.socketPath ?? "/run/podman/podman.sock"
      const source = cfg.podman.socketSource ?? "machine"
      if (source === "info") {
        responses.set("podman info -f {{.Host.RemoteSocket.Path}}", path)
      } else {
        responses.set(
          "podman machine inspect --format {{.ConnectionInfo.PodmanSocket.Path}}",
          path,
        )
      }
    }
  }
}

beforeEach(() => {
  calls.length = 0
  responses = new Map()
  resetContainerEngineCache()
  delete process.env.FR_CONTAINER_ENGINE
})

afterEach(() => {
  delete process.env.FR_CONTAINER_ENGINE
})

describe("detectContainerEngine() — auto-detect", () => {
  it("picks docker when available", () => {
    setEngineResponses({ docker: { info: true } })
    const engine = detectContainerEngine()
    expect(engine.name).toBe("docker")
    expect(engine.bin).toBe("docker")
    expect(engine.source).toBe("auto")
  })

  it("falls back to podman when docker is absent and podman ≥ 4.7", () => {
    setEngineResponses({
      podman: { version: "4.9.0", machineRunning: true },
    })
    const engine = detectContainerEngine()
    expect(engine.name).toBe("podman")
    expect(engine.source).toBe("auto")
  })

  it("prefers docker when both are installed", () => {
    setEngineResponses({
      docker: { info: true },
      podman: { version: "5.0.0", machineRunning: true },
    })
    const engine = detectContainerEngine()
    expect(engine.name).toBe("docker")
    expect(engine.available).toEqual(["docker", "podman"])
  })

  it("throws ContainerEngineNotFoundError when neither is available", () => {
    setEngineResponses({})
    expect(() => detectContainerEngine()).toThrow(ContainerEngineNotFoundError)
    expect(() => detectContainerEngine()).toThrow(/Tried: docker, podman/)
  })

  it("rejects podman < 4.7 with PodmanVersionTooOldError", () => {
    setEngineResponses({
      podman: { version: "4.5.0", machineRunning: true },
    })
    expect(() => detectContainerEngine()).toThrow(PodmanVersionTooOldError)
    expect(() => detectContainerEngine()).toThrow(/4\.5\.0/)
  })

  it("throws PodmanMachineNotRunningError when binary is present but machine is off", () => {
    // Podman binary responds to --version, but `podman info` fails — the
    // machine isn't running. We surface a targeted error instead of "too old".
    setEngineResponses({ podman: { version: "5.8.2" } })
    expect(() => detectContainerEngine()).toThrow(PodmanMachineNotRunningError)
    expect(() => detectContainerEngine()).toThrow(/podman machine start/)
  })
})

describe("detectContainerEngine() — explicit choice", () => {
  it("honors flag=docker even if podman is also available", () => {
    setEngineResponses({
      docker: { info: true },
      podman: { version: "5.0.0", machineRunning: true },
    })
    const engine = detectContainerEngine({ flag: "docker" })
    expect(engine.name).toBe("docker")
    expect(engine.source).toBe("flag")
  })

  it("honors flag=podman even if docker is also available", () => {
    setEngineResponses({
      docker: { info: true },
      podman: { version: "5.0.0", machineRunning: true },
    })
    const engine = detectContainerEngine({ flag: "podman" })
    expect(engine.name).toBe("podman")
    expect(engine.source).toBe("flag")
  })

  it("flag=docker fails hard if docker is missing", () => {
    setEngineResponses({
      podman: { version: "5.0.0", machineRunning: true },
    })
    expect(() => detectContainerEngine({ flag: "docker" })).toThrow(
      ContainerEngineNotFoundError,
    )
  })

  it("flag=auto falls through to env/config/auto-detect", () => {
    setEngineResponses({ docker: { info: true } })
    const engine = detectContainerEngine({ flag: "auto" })
    expect(engine.name).toBe("docker")
    expect(engine.source).toBe("auto")
  })

  it("rejects an unknown flag value", () => {
    setEngineResponses({ docker: { info: true } })
    expect(() =>
      detectContainerEngine({
        flag: "containerd" as unknown as "auto",
      }),
    ).toThrow(/Invalid --container-engine='containerd'/)
  })
})

describe("detectContainerEngine() — precedence", () => {
  it("flag beats env beats config beats auto", () => {
    setEngineResponses({
      docker: { info: true },
      podman: { version: "5.0.0", machineRunning: true },
    })

    // flag wins
    process.env.FR_CONTAINER_ENGINE = "docker"
    const e1 = detectContainerEngine({
      flag: "podman",
      configValue: "docker",
    })
    expect(e1.name).toBe("podman")
    expect(e1.source).toBe("flag")
    delete process.env.FR_CONTAINER_ENGINE

    // env wins over config
    resetContainerEngineCache()
    process.env.FR_CONTAINER_ENGINE = "podman"
    const e2 = detectContainerEngine({ configValue: "docker" })
    expect(e2.name).toBe("podman")
    expect(e2.source).toBe("env")
    delete process.env.FR_CONTAINER_ENGINE

    // config wins over auto
    resetContainerEngineCache()
    const e3 = detectContainerEngine({ configValue: "podman" })
    expect(e3.name).toBe("podman")
    expect(e3.source).toBe("config")
  })

  it("rejects invalid FR_CONTAINER_ENGINE values", () => {
    setEngineResponses({ docker: { info: true } })
    process.env.FR_CONTAINER_ENGINE = "lxc"
    expect(() => detectContainerEngine()).toThrow(
      /Invalid FR_CONTAINER_ENGINE='lxc'/,
    )
  })
})

describe("ContainerEngine helpers — argv assembly", () => {
  it("composeArgv prepends 'compose' for both engines", () => {
    setEngineResponses({ docker: { info: true } })
    const docker = detectContainerEngine()
    expect(docker.composeArgv(["up", "-d"])).toEqual(["compose", "up", "-d"])

    resetContainerEngineCache()
    setEngineResponses({
      podman: { version: "4.9.0", machineRunning: true },
    })
    const podman = detectContainerEngine()
    expect(podman.composeArgv(["up", "-d"])).toEqual(["compose", "up", "-d"])
  })

  it("composeCommand quotes the file path", () => {
    setEngineResponses({ docker: { info: true } })
    const docker = detectContainerEngine()
    expect(
      docker.composeCommand("/x/y.yml", ["exec", "-T", "pg", "psql"]),
    ).toBe('docker compose -f "/x/y.yml" exec -T pg psql')

    resetContainerEngineCache()
    setEngineResponses({
      podman: { version: "4.9.0", machineRunning: true },
    })
    const podman = detectContainerEngine()
    expect(
      podman.composeCommand("/x/y.yml", ["exec", "-T", "pg", "psql"]),
    ).toBe('podman compose -f "/x/y.yml" exec -T pg psql')
  })

  it("composeCommand with undefined file omits -f", () => {
    setEngineResponses({ docker: { info: true } })
    const docker = detectContainerEngine()
    expect(docker.composeCommand(undefined, ["ps", "-a"])).toBe(
      "docker compose ps -a",
    )
  })

  it("argv passes args through unchanged", () => {
    setEngineResponses({ docker: { info: true } })
    const engine = detectContainerEngine()
    expect(engine.argv(["images", "--format", "{{.Repository}}"])).toEqual([
      "images",
      "--format",
      "{{.Repository}}",
    ])
  })
})

describe("ContainerEngine.composeEnv() — DOCKER_HOST routing", () => {
  it("docker engine returns empty env", () => {
    setEngineResponses({ docker: { info: true } })
    const engine = detectContainerEngine()
    expect(engine.composeEnv()).toEqual({})
  })

  it("podman engine sets DOCKER_HOST to the host-visible socket on macOS (machine inspect)", () => {
    // On macOS, `podman machine inspect` returns the host path; `podman info`
    // would return the VM-internal path which is wrong. We must prefer
    // machine inspect.
    setEngineResponses({
      podman: {
        version: "5.0.0",
        machineRunning: true,
        socketPath:
          "/Users/ahmed/.local/share/containers/podman/machine/podman-machine-default/podman.sock",
        socketSource: "machine",
      },
    })
    const engine = detectContainerEngine()
    expect(engine.composeEnv()).toEqual({
      DOCKER_HOST:
        "unix:///Users/ahmed/.local/share/containers/podman/machine/podman-machine-default/podman.sock",
    })
  })

  it("falls back to `podman info` socket on Linux native (no machine)", () => {
    setEngineResponses({
      podman: {
        version: "5.0.0",
        machineRunning: true,
        socketPath: "/run/user/1000/podman/podman.sock",
        socketSource: "info",
      },
    })
    const engine = detectContainerEngine()
    expect(engine.composeEnv()).toEqual({
      DOCKER_HOST: "unix:///run/user/1000/podman/podman.sock",
    })
  })

  it("podman engine works with no detectable socket (no DOCKER_HOST set)", () => {
    setEngineResponses({
      podman: {
        version: "5.0.0",
        machineRunning: true,
        socketSource: "none",
      },
    })
    const engine = detectContainerEngine()
    expect(engine.name).toBe("podman")
    expect(engine.composeEnv()).toEqual({})
  })
})

describe("ContainerEngineNotFoundError formatting", () => {
  it("mentions both install URLs", () => {
    setEngineResponses({})
    try {
      detectContainerEngine()
    } catch (err) {
      expect(err).toBeInstanceOf(ContainerEngineNotFoundError)
      const e = err as ContainerEngineNotFoundError
      expect(e.message).toContain("docs.docker.com")
      expect(e.message).toContain("podman.io")
      expect(e.tried).toEqual(["docker", "podman"])
    }
  })
})
