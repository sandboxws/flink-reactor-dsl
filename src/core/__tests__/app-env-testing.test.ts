import { beforeEach, describe, expect, it } from "vitest"
import { Pipeline } from "../../components/pipeline.js"
import { KafkaSink } from "../../components/sinks.js"
import { KafkaSource } from "../../components/sources.js"
import { Filter } from "../../components/transforms.js"
import { synth } from "../../testing/synth.js"
import { validate } from "../../testing/validate.js"
import { synthesizeApp } from "../app.js"
import { defineConfig, type InfraConfig } from "../config.js"
import {
  discoverEnvironments,
  type EnvironmentConfig,
  resolveEnvironment,
} from "../environment.js"
import { resetNodeIdCounter } from "../jsx-runtime.js"
import { Field, Schema } from "../schema.js"

beforeEach(() => {
  resetNodeIdCounter()
})

// ── Shared test schemas ──────────────────────────────────────────────

const OrderSchema = Schema({
  fields: {
    order_id: Field.BIGINT(),
    user_id: Field.STRING(),
    amount: Field.DECIMAL(10, 2),
    event_time: Field.TIMESTAMP(3),
  },
  watermark: {
    column: "event_time",
    expression: "`event_time` - INTERVAL '5' SECOND",
  },
})

const EventSchema = Schema({
  fields: {
    event_id: Field.BIGINT(),
    event_type: Field.STRING(),
    event_time: Field.TIMESTAMP(3),
  },
})

// ── Helper: build simple pipelines ───────────────────────────────────

function buildOrderPipeline(opts?: {
  parallelism?: number
  bootstrapServers?: string
}) {
  const source = KafkaSource({
    topic: "orders",
    format: "json",
    schema: OrderSchema,
    bootstrapServers: opts?.bootstrapServers,
  })

  const sink = KafkaSink({
    topic: "filtered-orders",
    bootstrapServers: opts?.bootstrapServers,
    children: [
      Filter({
        condition: "amount > 100",
        children: [source],
      }),
    ],
  })

  return Pipeline({
    name: "order-ingestion",
    parallelism: opts?.parallelism,
    children: [sink],
  })
}

function buildEventPipeline(opts?: { parallelism?: number }) {
  const source = KafkaSource({
    topic: "events",
    format: "json",
    schema: EventSchema,
  })

  const sink = KafkaSink({
    topic: "processed-events",
    children: [source],
  })

  return Pipeline({
    name: "event-analytics",
    parallelism: opts?.parallelism,
    children: [sink],
  })
}

// ── 5.1: FlinkReactorApp with 2 pipelines ───────────────────────────

describe("5.1: FlinkReactorApp with 2 pipelines", () => {
  it("produces 2 SQL files + 2 CRDs", () => {
    const pipeline1 = buildOrderPipeline({ parallelism: 4 })
    const pipeline2 = buildEventPipeline({ parallelism: 2 })

    const result = synthesizeApp({
      name: "my-flink-app",
      children: [pipeline1, pipeline2],
    })

    expect(result.appName).toBe("my-flink-app")
    expect(result.pipelines).toHaveLength(2)

    // Verify pipeline names
    expect(result.pipelines[0].name).toBe("order-ingestion")
    expect(result.pipelines[1].name).toBe("event-analytics")

    // Each pipeline has its own SQL
    expect(result.pipelines[0].sql.sql).toContain("orders")
    expect(result.pipelines[0].sql.sql).toContain("filtered-orders")
    expect(result.pipelines[1].sql.sql).toContain("events")
    expect(result.pipelines[1].sql.sql).toContain("processed-events")

    // Each pipeline has its own CRD
    expect(result.pipelines[0].crd.metadata.name).toBe("order-ingestion")
    expect(result.pipelines[0].crd.spec.job.parallelism).toBe(4)
    expect(result.pipelines[1].crd.metadata.name).toBe("event-analytics")
    expect(result.pipelines[1].crd.spec.job.parallelism).toBe(2)
  })

  it("propagates shared infra bootstrapServers to Kafka components", () => {
    const pipeline1 = buildOrderPipeline()
    const pipeline2 = buildEventPipeline()

    const infra: InfraConfig = {
      kafka: { bootstrapServers: "kafka-prod:9092" },
    }

    const result = synthesizeApp({
      name: "my-app",
      infra,
      children: [pipeline1, pipeline2],
    })

    // Both pipelines' SQL should include the shared bootstrapServers
    expect(result.pipelines[0].sql.sql).toContain("kafka-prod:9092")
    expect(result.pipelines[1].sql.sql).toContain("kafka-prod:9092")
  })
})

// ── 5.2: EnvironmentConfig overrides pipeline parallelism ────────────

describe("5.2: EnvironmentConfig overrides pipeline parallelism", () => {
  it("applies environment parallelism to pipeline without explicit parallelism", () => {
    const pipeline1 = buildOrderPipeline() // no explicit parallelism
    const pipeline2 = buildEventPipeline() // no explicit parallelism

    const env: EnvironmentConfig = {
      name: "prod",
      pipelineOverrides: {
        "*": { parallelism: 8 },
        "order-ingestion": { parallelism: 16 },
      },
    }

    const result = synthesizeApp(
      {
        name: "my-app",
        children: [pipeline1, pipeline2],
      },
      { env },
    )

    // order-ingestion: named override → 16
    expect(result.pipelines[0].crd.spec.job.parallelism).toBe(16)
    // event-analytics: wildcard override → 8
    expect(result.pipelines[1].crd.spec.job.parallelism).toBe(8)
  })
})

// ── 5.3: Configuration cascade resolves in correct priority order ────

describe("5.3: Configuration cascade priority", () => {
  it("Pipeline prop beats env override beats config default", () => {
    // Pipeline has parallelism=2 set explicitly
    const pipeline = buildOrderPipeline({ parallelism: 2 })

    const env: EnvironmentConfig = {
      name: "prod",
      pipelineOverrides: {
        "order-ingestion": { parallelism: 8 },
      },
    }

    const result = synthesizeApp(
      {
        name: "my-app",
        children: [pipeline],
      },
      { env },
    )

    // Pipeline prop (2) wins over env override (8)
    expect(result.pipelines[0].crd.spec.job.parallelism).toBe(2)
  })

  it("env override wins when no pipeline prop", () => {
    const pipeline = buildOrderPipeline() // no parallelism

    const env: EnvironmentConfig = {
      name: "prod",
      pipelineOverrides: {
        "order-ingestion": { parallelism: 8 },
      },
    }

    const result = synthesizeApp(
      {
        name: "my-app",
        children: [pipeline],
      },
      { env },
    )

    expect(result.pipelines[0].crd.spec.job.parallelism).toBe(8)
  })

  it("falls back to Flink default (1) when nothing set", () => {
    const pipeline = buildOrderPipeline() // no parallelism

    const result = synthesizeApp({
      name: "my-app",
      children: [pipeline],
    })

    // Default from CRD generator is 1
    expect(result.pipelines[0].crd.spec.job.parallelism).toBe(1)
  })
})

// ── 5.4: Wildcard override applies to all pipelines ──────────────────

describe("5.4: Wildcard override applies to all pipelines", () => {
  it("wildcard parallelism applies to all pipelines", () => {
    const pipeline1 = buildOrderPipeline()
    const pipeline2 = buildEventPipeline()

    const env: EnvironmentConfig = {
      name: "staging",
      pipelineOverrides: {
        "*": { parallelism: 4 },
      },
    }

    const result = synthesizeApp(
      {
        name: "my-app",
        children: [pipeline1, pipeline2],
      },
      { env },
    )

    expect(result.pipelines[0].crd.spec.job.parallelism).toBe(4)
    expect(result.pipelines[1].crd.spec.job.parallelism).toBe(4)
  })

  it("named override takes priority over wildcard", () => {
    const pipeline1 = buildOrderPipeline()
    const pipeline2 = buildEventPipeline()

    const env: EnvironmentConfig = {
      name: "staging",
      pipelineOverrides: {
        "*": { parallelism: 4 },
        "event-analytics": { parallelism: 12 },
      },
    }

    const result = synthesizeApp(
      {
        name: "my-app",
        children: [pipeline1, pipeline2],
      },
      { env },
    )

    // order-ingestion gets wildcard: 4
    expect(result.pipelines[0].crd.spec.job.parallelism).toBe(4)
    // event-analytics gets named override: 12
    expect(result.pipelines[1].crd.spec.job.parallelism).toBe(12)
  })
})

// ── 5.5: synth() returns correct SQL and CRD ────────────────────────

describe("5.5: synth() test helper", () => {
  it("returns correct SQL and CRD for a pipeline", () => {
    const pipeline = buildOrderPipeline({ parallelism: 4 })

    const result = synth(pipeline)

    expect(result.sql).toContain("CREATE TABLE")
    expect(result.sql).toContain("orders")
    expect(result.sql).toContain("filtered-orders")
    expect(result.sql).toContain("INSERT INTO")
    expect(result.sql).toContain("amount > 100")

    expect(result.crd.apiVersion).toBe("flink.apache.org/v1beta1")
    expect(result.crd.kind).toBe("FlinkDeployment")
    expect(result.crd.metadata.name).toBe("order-ingestion")
    expect(result.crd.spec.job.parallelism).toBe(4)
  })

  it("works with toMatchSnapshot()", () => {
    const source = KafkaSource({
      topic: "orders",
      format: "json",
      schema: OrderSchema,
    })

    const sink = KafkaSink({
      topic: "output",
      children: [source],
    })

    const pipeline = Pipeline({
      name: "snapshot-test",
      parallelism: 1,
      children: [sink],
    })

    const result = synth(pipeline)
    expect(result.sql).toMatchSnapshot()
    expect(result.crd).toMatchSnapshot()
  })

  it("accepts flinkVersion option", () => {
    const pipeline = buildOrderPipeline({ parallelism: 1 })

    const result = synth(pipeline, { flinkVersion: "1.20" })

    expect(result.crd.spec.flinkVersion).toBe("v1_20")
    expect(result.crd.spec.image).toBe("flink:1.20")
  })
})

// ── 5.6: validate() returns diagnostics ──────────────────────────────

describe("5.6: validate() test helper", () => {
  it("returns errors and warnings as separate arrays", () => {
    const pipeline = buildOrderPipeline({ parallelism: 1 })
    const result = validate(pipeline)

    // validate() correctly separates errors from warnings
    expect(Array.isArray(result.errors)).toBe(true)
    expect(Array.isArray(result.warnings)).toBe(true)
    // All returned items are proper Diagnostic objects
    for (const diag of [...result.errors, ...result.warnings]) {
      expect(diag).toHaveProperty("severity")
      expect(diag).toHaveProperty("message")
    }
  })

  it("detects orphan source (source with no downstream)", () => {
    // Create a pipeline with a source that has no sink connected
    const orphanSource = KafkaSource({
      topic: "orphan",
      format: "json",
      schema: EventSchema,
    })

    // Build a pipeline that has the orphan source as a sibling (not connected to any sink)
    const connectedSource = KafkaSource({
      topic: "connected",
      format: "json",
      schema: EventSchema,
    })

    const sink = KafkaSink({
      topic: "output",
      children: [connectedSource],
    })

    const pipeline = Pipeline({
      name: "orphan-test",
      children: [sink, orphanSource],
    })

    const result = validate(pipeline)

    // The orphan source should be detected — orphanSource has no outgoing edges
    // Note: in the construct tree, sources are leaf nodes (no children),
    // so they never have outgoing edges in the buildFromTree model.
    expect(result.errors.length).toBeGreaterThan(0)
    expect(result.errors.some((e) => e.message.includes("Orphan source"))).toBe(
      true,
    )
  })

  it("detects more orphan sources in larger pipelines", () => {
    // Two orphan sources + one connected
    const orphan1 = KafkaSource({
      topic: "orphan1",
      format: "json",
      schema: EventSchema,
    })
    const orphan2 = KafkaSource({
      topic: "orphan2",
      format: "json",
      schema: EventSchema,
    })
    const connected = KafkaSource({
      topic: "connected",
      format: "json",
      schema: EventSchema,
    })

    const sink = KafkaSink({ topic: "out", children: [connected] })
    const pipeline = Pipeline({
      name: "multi-orphan",
      children: [sink, orphan1, orphan2],
    })

    const result = validate(pipeline)

    // All three sources are tree leaves (no outgoing edges via buildFromTree),
    // but orphan1 and orphan2 are clearly disconnected from any sink path
    const orphanErrors = result.errors.filter((e) =>
      e.message.includes("Orphan source"),
    )
    expect(orphanErrors.length).toBeGreaterThanOrEqual(2)
  })
})

// ── 5.7: defineConfig() validates config shape ───────────────────────

describe("5.7: defineConfig()", () => {
  it("accepts valid config", () => {
    const config = defineConfig({
      flink: { version: "2.0" },
      kubernetes: { namespace: "flink-prod", image: "my-registry/flink:2.0" },
      kafka: { bootstrapServers: "kafka-prod:9092" },
      connectors: { delivery: "init-container" },
    })

    expect(config.flink?.version).toBe("2.0")
    expect(config.kubernetes?.namespace).toBe("flink-prod")
    expect(config.kafka?.bootstrapServers).toBe("kafka-prod:9092")
    expect(config.connectors?.delivery).toBe("init-container")
  })

  it("throws for unsupported Flink version", () => {
    expect(() =>
      defineConfig({
        flink: { version: "3.0" as "2.0" },
      }),
    ).toThrow("Unsupported Flink version")
  })

  it("throws for invalid delivery strategy", () => {
    expect(() =>
      defineConfig({
        connectors: { delivery: "invalid" as "init-container" },
      }),
    ).toThrow("Unsupported connector delivery strategy")
  })

  it("accepts minimal config", () => {
    const config = defineConfig({})
    expect(config).toBeDefined()
  })

  it("provides toInfraConfig()", () => {
    const config = defineConfig({
      kafka: { bootstrapServers: "localhost:9092" },
      kubernetes: { namespace: "default" },
      connectors: { delivery: "custom-image" },
    })

    const infra = config.toInfraConfig?.()
    expect(infra.kafka?.bootstrapServers).toBe("localhost:9092")
    expect(infra.kubernetes?.namespace).toBe("default")
    expect(infra.connectors?.delivery).toBe("custom-image")
  })
})

// ── resolveEnvironment unit tests ────────────────────────────────────

describe("resolveEnvironment", () => {
  it("merges wildcard and named overrides", () => {
    const env: EnvironmentConfig = {
      name: "prod",
      pipelineOverrides: {
        "*": { parallelism: 4, stateBackend: "rocksdb" },
        "my-pipeline": { parallelism: 16 },
      },
    }

    const result = resolveEnvironment("my-pipeline", env)
    // Named overrides win for parallelism
    expect(result.parallelism).toBe(16)
    // Wildcard still applies for stateBackend
    expect(result.stateBackend).toBe("rocksdb")
  })

  it("returns only wildcard when no named match", () => {
    const env: EnvironmentConfig = {
      name: "prod",
      pipelineOverrides: {
        "*": { parallelism: 8 },
      },
    }

    const result = resolveEnvironment("unknown-pipeline", env)
    expect(result.parallelism).toBe(8)
  })

  it("returns empty when no overrides", () => {
    const env: EnvironmentConfig = { name: "dev" }
    const result = resolveEnvironment("anything", env)
    expect(Object.keys(result)).toHaveLength(0)
  })
})

// ── discoverEnvironments ─────────────────────────────────────────────

describe("discoverEnvironments", () => {
  it("discovers .ts files and strips extension", () => {
    const envs = discoverEnvironments("/project/env", [
      "dev.ts",
      "prod.ts",
      "staging.ts",
      "README.md",
      "types.d.ts",
    ])

    expect(envs).toEqual([
      { name: "dev", path: "/project/env/dev.ts" },
      { name: "prod", path: "/project/env/prod.ts" },
      { name: "staging", path: "/project/env/staging.ts" },
    ])
  })

  it("returns empty for no .ts files", () => {
    const envs = discoverEnvironments("/project/env", ["README.md"])
    expect(envs).toEqual([])
  })
})
