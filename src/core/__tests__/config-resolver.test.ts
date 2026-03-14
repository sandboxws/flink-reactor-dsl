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
