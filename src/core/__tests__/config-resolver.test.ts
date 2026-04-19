import { afterEach, beforeEach, describe, expect, it } from "vitest"
import { defineConfig } from "../config.js"
import { resolveConfig, toInfraConfigFromResolved } from "../config-resolver.js"
import { env } from "../env-var.js"

describe("resolveConfig()", () => {
  const saved: Record<string, string | undefined> = {}

  beforeEach(() => {
    for (const key of [
      "FLINK_REST_URL",
      "FLINK_AUTH_PASSWORD",
      "FLINK_AUTH_USERNAME",
    ]) {
      saved[key] = process.env[key]
    }
  })

  afterEach(() => {
    for (const [key, val] of Object.entries(saved)) {
      if (val === undefined) delete process.env[key]
      else process.env[key] = val
    }
  })

  it("applies defaults when no environment is specified", () => {
    const config = defineConfig({
      flink: { version: "2.0" },
    })
    const resolved = resolveConfig(config)
    expect(resolved.flink.version).toBe("2.0")
    expect(resolved.kubernetes.namespace).toBe("default")
    expect(resolved.pipelines).toEqual({})
  })

  it("merges common + environment overrides", () => {
    const config = defineConfig({
      flink: { version: "2.0" },
      dashboard: { pollIntervalMs: 5000 },
      environments: {
        development: {
          cluster: { url: "http://localhost:8081" },
          dashboard: { mockMode: true },
          pipelines: { "*": { parallelism: 1 } },
        },
      },
    })

    const resolved = resolveConfig(config, "development")
    expect(resolved.cluster.url).toBe("http://localhost:8081")
    expect(resolved.dashboard.mockMode).toBe(true)
    expect(resolved.dashboard.pollIntervalMs).toBe(5000)
    expect(resolved.pipelines["*"]).toEqual({ parallelism: 1 })
    expect(resolved.environmentName).toBe("development")
  })

  it("resolves env() markers from process.env", () => {
    process.env.FLINK_REST_URL = "https://prod.example.com:8081"
    process.env.FLINK_AUTH_PASSWORD = "s3cret"
    process.env.FLINK_AUTH_USERNAME = "admin"

    const config = defineConfig({
      flink: { version: "2.0" },
      environments: {
        production: {
          cluster: { url: env("FLINK_REST_URL") },
          dashboard: {
            auth: {
              type: "basic",
              username: env("FLINK_AUTH_USERNAME"),
              password: env("FLINK_AUTH_PASSWORD"),
            },
          },
        },
      },
    })

    const resolved = resolveConfig(config, "production")
    expect(resolved.cluster.url).toBe("https://prod.example.com:8081")
    expect(resolved.dashboard.auth?.username).toBe("admin")
    expect(resolved.dashboard.auth?.password).toBe("s3cret")
  })

  it("throws on unknown environment name", () => {
    const config = defineConfig({
      environments: {
        development: { cluster: { url: "http://localhost:8081" } },
      },
    })

    expect(() => resolveConfig(config, "staging")).toThrow(
      "Unknown environment 'staging'. Available: development",
    )
  })

  it("throws on missing required env var", () => {
    delete process.env.FLINK_REST_URL
    const config = defineConfig({
      environments: {
        production: {
          cluster: { url: env("FLINK_REST_URL") },
        },
      },
    })

    expect(() => resolveConfig(config, "production")).toThrow(
      "Missing required environment variable 'FLINK_REST_URL'",
    )
  })

  it("deep-merges nested dashboard settings", () => {
    const config = defineConfig({
      dashboard: {
        pollIntervalMs: 5000,
        ssl: { enabled: false },
      },
      environments: {
        production: {
          dashboard: {
            ssl: { enabled: true },
          },
        },
      },
    })

    const resolved = resolveConfig(config, "production")
    expect(resolved.dashboard.ssl?.enabled).toBe(true)
    expect(resolved.dashboard.pollIntervalMs).toBe(5000)
  })

  it("works without environments block (legacy mode)", () => {
    const config = defineConfig({
      flink: { version: "1.20" },
      kubernetes: { namespace: "flink-legacy" },
    })

    const resolved = resolveConfig(config)
    expect(resolved.flink.version).toBe("1.20")
    expect(resolved.kubernetes.namespace).toBe("flink-legacy")
  })
})

describe("resolveConfig() runtime defaulting", () => {
  it("defaults `development` env to docker runtime", () => {
    const config = defineConfig({
      environments: { development: {} },
    })
    const resolved = resolveConfig(config, "development")
    expect(resolved.runtime).toBe("docker")
    expect(resolved.supportedRuntimes).toEqual(["docker"])
  })

  it("defaults `local` env to docker runtime", () => {
    const config = defineConfig({ environments: { local: {} } })
    expect(resolveConfig(config, "local").runtime).toBe("docker")
  })

  it("defaults `test` env to minikube runtime with minikube kubectl context", () => {
    const config = defineConfig({ environments: { test: {} } })
    const resolved = resolveConfig(config, "test")
    expect(resolved.runtime).toBe("minikube")
    expect(resolved.kubectl.context).toBe("minikube")
  })

  it("defaults `staging` and `production` to kubernetes runtime (no default context)", () => {
    const config = defineConfig({
      environments: { staging: {}, production: {} },
    })
    const staging = resolveConfig(config, "staging")
    const production = resolveConfig(config, "production")
    expect(staging.runtime).toBe("kubernetes")
    expect(staging.kubectl.context).toBeUndefined()
    expect(production.runtime).toBe("kubernetes")
    expect(production.kubectl.context).toBeUndefined()
  })

  it("defaults unknown env names to docker", () => {
    const config = defineConfig({ environments: { qa: {} } })
    expect(resolveConfig(config, "qa").runtime).toBe("docker")
  })

  it("honors an explicit runtime over the env-name default", () => {
    const config = defineConfig({
      environments: {
        development: { runtime: "minikube", kubectl: { context: "dev-k8s" } },
      },
    })
    const resolved = resolveConfig(config, "development")
    expect(resolved.runtime).toBe("minikube")
    expect(resolved.kubectl.context).toBe("dev-k8s")
  })

  it("rejects unknown runtime values", () => {
    const config = defineConfig({
      environments: {
        development: { runtime: "swarm" as unknown as "docker" },
      },
    })
    expect(() => resolveConfig(config, "development")).toThrow(
      /Unknown runtime 'swarm'/,
    )
  })

  it("exposes supportedRuntimes for multi-runtime envs", () => {
    const config = defineConfig({
      environments: {
        development: {
          runtime: "docker",
          supportedRuntimes: ["docker", "minikube"],
        },
      },
    })
    const resolved = resolveConfig(config, "development")
    expect(resolved.supportedRuntimes).toEqual(["docker", "minikube"])
  })

  it("rejects runtime not listed in supportedRuntimes", () => {
    const config = defineConfig({
      environments: {
        development: {
          runtime: "minikube",
          supportedRuntimes: ["docker"],
        },
      },
    })
    expect(() => resolveConfig(config, "development")).toThrow(
      /Runtime 'minikube' is not listed in supportedRuntimes/,
    )
  })
})

describe("toInfraConfigFromResolved()", () => {
  it("extracts InfraConfig from resolved config", () => {
    const config = defineConfig({
      flink: { version: "2.0" },
      kubernetes: { namespace: "prod" },
      kafka: { bootstrapServers: "kafka:9092" },
    })

    const resolved = resolveConfig(config)
    const infra = toInfraConfigFromResolved(resolved)

    expect(infra.kubernetes?.namespace).toBe("prod")
    expect(infra.kafka?.bootstrapServers).toBe("kafka:9092")
  })
})
