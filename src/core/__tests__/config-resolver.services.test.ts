// ── Services resolution + legacy back-fill tests ─────────────────────
//
// Covers the three branches in `resolveConfig` for the `services` block:
//   1. `services` declared explicitly → use as-is, ignore legacy alias
//   2. Only legacy `kafka.bootstrapServers` set → back-fill + deprecation
//   3. Neither declared → resolved.services === {}

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { defineConfig } from "../config.js"
import {
  __resetDeprecationWarningsForTesting,
  resolveConfig,
} from "../config-resolver.js"

describe("resolveConfig() — services", () => {
  beforeEach(() => {
    __resetDeprecationWarningsForTesting()
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it("returns an empty services block when none declared", () => {
    const config = defineConfig({ flink: { version: "2.0" } })
    const resolved = resolveConfig(config)
    expect(resolved.services).toEqual({})
  })

  it("preserves a declared services block verbatim", () => {
    const config = defineConfig({
      flink: { version: "2.0" },
      services: { kafka: {}, postgres: { flavor: "plain" } },
    })
    const resolved = resolveConfig(config)
    expect(resolved.services).toEqual({
      kafka: {},
      postgres: { flavor: "plain" },
    })
  })

  it("merges environment-level services over common", () => {
    const config = defineConfig({
      flink: { version: "2.0" },
      services: { kafka: {} },
      environments: {
        development: { services: { iceberg: {} } },
      },
    })
    const resolved = resolveConfig(config, "development")
    // The env override replaces the common services block (per existing
    // resolver semantics — same as `cluster`/`kafka`).
    expect(resolved.services).toMatchObject({ iceberg: {} })
  })

  it("disables a service via `false` in an environment override", () => {
    const config = defineConfig({
      flink: { version: "2.0" },
      services: { kafka: {} },
      environments: {
        production: { services: { kafka: false } },
      },
    })
    const resolved = resolveConfig(config, "production")
    expect(resolved.services.kafka).toBe(false)
  })

  it("back-fills `services.kafka.bootstrapServers` from legacy field with a deprecation warning", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {})
    const config = defineConfig({
      flink: { version: "2.0" },
      kafka: { bootstrapServers: "kafka:9092" },
    })
    const resolved = resolveConfig(config)
    expect(resolved.services).toEqual({
      kafka: { bootstrapServers: "kafka:9092" },
    })
    expect(warn).toHaveBeenCalledTimes(1)
    expect(warn.mock.calls[0]?.[0]).toMatch(/deprecated/i)
  })

  it("ignores legacy field when services is also declared, with a single warning", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {})
    const config = defineConfig({
      flink: { version: "2.0" },
      kafka: { bootstrapServers: "old:9092" },
      services: { kafka: { bootstrapServers: "new:9092" } },
    })
    const resolved = resolveConfig(config)
    expect(resolved.services).toMatchObject({
      kafka: { bootstrapServers: "new:9092" },
    })
    expect(warn).toHaveBeenCalledTimes(1)
    expect(warn.mock.calls[0]?.[0]).toMatch(/ignored/i)
  })

  it("dedupes the deprecation warning across multiple resolves", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {})
    const config = defineConfig({
      flink: { version: "2.0" },
      kafka: { bootstrapServers: "kafka:9092" },
    })
    resolveConfig(config)
    resolveConfig(config)
    resolveConfig(config)
    expect(warn).toHaveBeenCalledTimes(1)
  })

  it("keeps the legacy `resolved.kafka.bootstrapServers` accurate for un-migrated callers", () => {
    const config = defineConfig({
      flink: { version: "2.0" },
      services: { kafka: { bootstrapServers: "kafka:9092" } },
    })
    const resolved = resolveConfig(config)
    expect(resolved.kafka.bootstrapServers).toBe("kafka:9092")
  })
})
