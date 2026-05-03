// ── service-selection: profilesFromConfig + buildComposeEnv ──────────
//
// Pure-translator tests. Each input is a synthetic ResolvedConfig produced
// via `resolveConfig` so we exercise the same shape `cluster up` sees at
// runtime. Assertions are set-based (using `toContain` / `not.toContain`)
// so adding new profiles in the future doesn't churn tests for unrelated
// services.

import { describe, expect, it } from "vitest"
import { defineConfig } from "@/core/config.js"
import { resolveConfig } from "@/core/config-resolver.js"
import {
  ALL_PROFILES,
  buildComposeEnv,
  profilesFromConfig,
} from "../service-selection.js"

function profilesFor(services: Record<string, unknown>): string[] {
  const config = defineConfig({
    flink: { version: "2.0" },
    services: services as never,
  })
  return profilesFromConfig(resolveConfig(config))
}

describe("profilesFromConfig()", () => {
  it("returns no profiles when services is empty", () => {
    expect(profilesFor({})).toEqual([])
  })

  it("activates kafka when services.kafka is declared", () => {
    expect(profilesFor({ kafka: {} })).toEqual(["kafka"])
  })

  it("activates fluss when services.fluss is declared", () => {
    expect(profilesFor({ fluss: {} })).toEqual(["fluss"])
  })

  it("activates iceberg AND a postgres flavor (Lakekeeper backing store)", () => {
    const profiles = profilesFor({ iceberg: {} })
    expect(profiles).toContain("iceberg")
    // Iceberg implies postgres — default flavor is timescaledb.
    expect(profiles).toContain("timescaledb")
    expect(profiles).not.toContain("postgres-plain")
  })

  it("respects services.postgres.flavor when iceberg is also declared", () => {
    const profiles = profilesFor({
      iceberg: {},
      postgres: { flavor: "plain" },
    })
    expect(profiles).toContain("iceberg")
    expect(profiles).toContain("postgres-plain")
    expect(profiles).not.toContain("timescaledb")
  })

  it("selects timescaledb by default for an explicit postgres declaration", () => {
    expect(profilesFor({ postgres: {} })).toEqual(["timescaledb"])
  })

  it("selects postgres-plain when flavor is 'plain'", () => {
    expect(profilesFor({ postgres: { flavor: "plain" } })).toEqual([
      "postgres-plain",
    ])
  })

  it("is deterministic — same input yields same profile order across runs", () => {
    const a = profilesFor({ iceberg: {}, kafka: {}, fluss: {} })
    const b = profilesFor({ fluss: {}, kafka: {}, iceberg: {} })
    expect(a).toEqual(b)
  })

  it("orders profiles to match ALL_PROFILES (stable across runs)", () => {
    const profiles = profilesFor({ iceberg: {}, fluss: {}, kafka: {} })
    // Each emitted profile should come from ALL_PROFILES in declaration
    // order — the test asserts the relative order of any subset.
    const allOrder = ALL_PROFILES.map(String)
    const indices = profiles.map((p) => allOrder.indexOf(p))
    const sorted = [...indices].sort((a, b) => a - b)
    expect(indices).toEqual(sorted)
  })

  it("merges environment-level disable: services.kafka=false suppresses kafka profile", () => {
    const config = defineConfig({
      flink: { version: "2.0" },
      services: { kafka: {} },
      environments: {
        production: { services: { kafka: false } },
      },
    })
    const profiles = profilesFromConfig(resolveConfig(config, "production"))
    expect(profiles).not.toContain("kafka")
  })
})

describe("buildComposeEnv()", () => {
  it("returns an empty object when services is undefined", () => {
    expect(buildComposeEnv(undefined)).toEqual({})
  })

  it("returns an empty object when services has no overrides", () => {
    expect(buildComposeEnv({})).toEqual({})
    expect(buildComposeEnv({ kafka: {}, postgres: {} })).toEqual({})
  })

  it("emits KAFKA_EXTERNAL_PORT and KAFKA_IMAGE when overridden", () => {
    expect(
      buildComposeEnv({
        kafka: { externalPort: 19094, image: "apache/kafka:3.10.0" },
      }),
    ).toEqual({
      KAFKA_EXTERNAL_PORT: "19094",
      KAFKA_IMAGE: "apache/kafka:3.10.0",
    })
  })

  it("emits POSTGRES_PORT, FLUSS_COORDINATOR_PORT, ICEBERG_REST_PORT", () => {
    expect(
      buildComposeEnv({
        postgres: { externalPort: 15432 },
        fluss: { externalPort: 19123 },
        iceberg: { externalPort: 18181 },
      }),
    ).toMatchObject({
      POSTGRES_PORT: "15432",
      FLUSS_COORDINATOR_PORT: "19123",
      ICEBERG_REST_PORT: "18181",
    })
  })

  it("ignores services that are set to false (no overrides applied)", () => {
    expect(
      buildComposeEnv({
        kafka: false,
        postgres: false,
      }),
    ).toEqual({})
  })
})
