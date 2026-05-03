// ── Synth-time validator: services × pipelines coherence ─────────────
//
// Verifies that `synthesizeApp` rejects pipelines whose connectors
// require an undeclared service. The check fires *before* per-pipeline
// codegen so a missing service halts synthesis early — this test asserts
// the failure path and confirms the error carries `code: "missing_service"`
// for structured handling.

import { beforeEach, describe, expect, it } from "vitest"
import { Pipeline } from "@/components/pipeline.js"
import { KafkaSink } from "@/components/sinks.js"
import { KafkaSource } from "@/components/sources.js"
import { Filter } from "@/components/transforms.js"
import { synthesizeApp } from "@/core/app.js"
import { defineConfig } from "@/core/config.js"
import { resolveConfig } from "@/core/config-resolver.js"
import { resetNodeIdCounter } from "@/core/jsx-runtime.js"
import { Field, Schema } from "@/core/schema.js"

beforeEach(() => {
  resetNodeIdCounter()
})

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

function buildKafkaPipeline(name: string) {
  const source = KafkaSource({
    topic: "orders",
    format: "json",
    schema: OrderSchema,
  })
  const sink = KafkaSink({
    topic: "filtered-orders",
    children: [Filter({ condition: "amount > 100", children: [source] })],
  })
  return Pipeline({ name, children: [sink] })
}

describe("synthesizeApp() — services × pipelines coherence", () => {
  it("rejects a Kafka pipeline when services.kafka is undeclared", () => {
    const config = defineConfig({
      flink: { version: "2.0" },
      services: {}, // Empty: no kafka.
    })
    const resolvedConfig = resolveConfig(config)

    expect(() =>
      synthesizeApp(
        { name: "my-app", children: [buildKafkaPipeline("orders")] },
        { resolvedConfig },
      ),
    ).toThrow(/uses connector 'kafka' but `services\.kafka` is not declared/i)
  })

  it("attaches code: 'missing_service' on the thrown error", () => {
    const config = defineConfig({
      flink: { version: "2.0" },
      services: {},
    })
    const resolvedConfig = resolveConfig(config)

    let caught: unknown
    try {
      synthesizeApp(
        { name: "my-app", children: [buildKafkaPipeline("orders")] },
        { resolvedConfig },
      )
    } catch (err) {
      caught = err
    }
    expect(caught).toBeInstanceOf(Error)
    expect((caught as { code?: string }).code).toBe("missing_service")
  })

  it("succeeds when services.kafka is declared", () => {
    const config = defineConfig({
      flink: { version: "2.0" },
      services: { kafka: {} },
    })
    const resolvedConfig = resolveConfig(config)

    const result = synthesizeApp(
      { name: "my-app", children: [buildKafkaPipeline("orders")] },
      { resolvedConfig },
    )
    expect(result.pipelines).toHaveLength(1)
    expect(result.pipelines[0].name).toBe("orders")
  })

  it("rejects when an environment override sets services.kafka to false", () => {
    const config = defineConfig({
      flink: { version: "2.0" },
      services: { kafka: {} },
      environments: {
        production: { services: { kafka: false } },
      },
    })
    const resolvedConfig = resolveConfig(config, "production")

    expect(() =>
      synthesizeApp(
        { name: "my-app", children: [buildKafkaPipeline("orders")] },
        { resolvedConfig },
      ),
    ).toThrow(/services\.kafka.*not declared/i)
  })

  it("names the offending pipeline in the error message", () => {
    const config = defineConfig({
      flink: { version: "2.0" },
      services: {},
    })
    const resolvedConfig = resolveConfig(config)

    expect(() =>
      synthesizeApp(
        {
          name: "my-app",
          children: [buildKafkaPipeline("checkout-flow")],
        },
        { resolvedConfig },
      ),
    ).toThrow(/pipeline 'checkout-flow'/)
  })

  it("skips the check when no resolvedConfig is supplied (legacy path)", () => {
    // Synth without resolvedConfig — legacy projects without a config file
    // continue to work. The pipeline still synthesizes successfully.
    const result = synthesizeApp({
      name: "my-app",
      children: [buildKafkaPipeline("orders")],
    })
    expect(result.pipelines).toHaveLength(1)
  })
})
