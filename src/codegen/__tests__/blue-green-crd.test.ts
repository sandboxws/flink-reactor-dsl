import { beforeEach, describe, expect, it } from "vitest"
import {
  type FlinkBlueGreenDeploymentCrd,
  type FlinkDeploymentCrd,
  generateCrd,
  generateCrdYaml,
  getFlinkConfiguration,
  withFlinkConfiguration,
} from "@/codegen/crd-generator.js"
import { consumeValidationWarnings, Pipeline } from "@/components/pipeline.js"
import { KafkaSink } from "@/components/sinks.js"
import { KafkaSource } from "@/components/sources.js"
import { resetNodeIdCounter } from "@/core/jsx-runtime.js"
import { Field, Schema } from "@/core/schema.js"

beforeEach(() => {
  resetNodeIdCounter()
})

const OrderSchema = Schema({
  fields: {
    order_id: Field.BIGINT(),
    amount: Field.DECIMAL(10, 2),
    event_time: Field.TIMESTAMP(3),
  },
})

function makeSource() {
  return KafkaSource({
    topic: "orders",
    format: "json",
    schema: OrderSchema,
  })
}

function makeSink(source: ReturnType<typeof makeSource>) {
  return KafkaSink({ topic: "output", children: [source] })
}

// ── Blue-green CRD generation ───────────────────────────────────────

describe("Blue-green CRD generation", () => {
  it("produces FlinkBlueGreenDeployment kind", () => {
    const source = makeSource()
    const sink = makeSink(source)

    const pipeline = Pipeline({
      name: "order-processor",
      parallelism: 4,
      checkpoint: { interval: "30s" },
      upgradeStrategy: { mode: "blue-green" },
      children: [sink],
    })

    const crd = generateCrd(pipeline, { flinkVersion: "2.0" })
    expect(crd.kind).toBe("FlinkBlueGreenDeployment")
    expect(crd.apiVersion).toBe("flink.apache.org/v1beta1")
    expect(crd.metadata.name).toBe("order-processor")
  })

  it("produces snapshot-stable YAML", () => {
    const source = makeSource()
    const sink = makeSink(source)

    const pipeline = Pipeline({
      name: "order-processor",
      parallelism: 4,
      checkpoint: { interval: "30s", mode: "exactly-once" },
      upgradeStrategy: { mode: "blue-green" },
      children: [sink],
    })

    const yaml = generateCrdYaml(pipeline, { flinkVersion: "2.0" })
    expect(yaml).toMatchSnapshot()
  })

  it("defaults upgradeMode to 'savepoint'", () => {
    const pipeline = Pipeline({
      name: "test",
      checkpoint: { interval: "30s" },
      upgradeStrategy: { mode: "blue-green" },
      children: [],
    })

    const crd = generateCrd(pipeline, {
      flinkVersion: "2.0",
    }) as FlinkBlueGreenDeploymentCrd
    expect(crd.spec.template.spec.job.upgradeMode).toBe("savepoint")
  })

  it("respects explicit upgradeMode", () => {
    const pipeline = Pipeline({
      name: "test",
      checkpoint: { interval: "30s" },
      upgradeStrategy: { mode: "blue-green", upgradeMode: "last-state" },
      children: [],
    })

    const crd = generateCrd(pipeline, {
      flinkVersion: "2.0",
    }) as FlinkBlueGreenDeploymentCrd
    expect(crd.spec.template.spec.job.upgradeMode).toBe("last-state")
  })

  it("nests Flink config under template.spec", () => {
    const pipeline = Pipeline({
      name: "test",
      parallelism: 8,
      checkpoint: { interval: "60s", mode: "exactly-once" },
      stateBackend: "rocksdb",
      upgradeStrategy: { mode: "blue-green" },
      children: [],
    })

    const crd = generateCrd(pipeline, {
      flinkVersion: "2.0",
    }) as FlinkBlueGreenDeploymentCrd
    const inner = crd.spec.template.spec

    expect(inner.image).toBe("flink:2.0")
    expect(inner.flinkVersion).toBe("v2_0")
    expect(inner.flinkConfiguration["execution.checkpointing.interval"]).toBe(
      "60000",
    )
    expect(inner.flinkConfiguration["state.backend.type"]).toBe("rocksdb")
    expect(inner.job.parallelism).toBe(8)
  })
})

// ── BG with full config ─────────────────────────────────────────────

describe("Blue-green CRD: operator configuration", () => {
  it("maps BlueGreenConfig to spec.configuration", () => {
    const pipeline = Pipeline({
      name: "test",
      checkpoint: { interval: "30s" },
      upgradeStrategy: {
        mode: "blue-green",
        blueGreen: {
          abortGracePeriod: "10m",
          deploymentDeletionDelay: "2s",
          rescheduleInterval: "15s",
        },
      },
      children: [],
    })

    const crd = generateCrd(pipeline, {
      flinkVersion: "2.0",
    }) as FlinkBlueGreenDeploymentCrd

    expect(crd.spec.configuration).toEqual({
      "blue-green.abort.grace-period": "10m",
      "blue-green.deployment.deletion.delay": "2s",
      "blue-green.reschedule.interval": "15s",
    })
  })

  it("writes duration strings as-is (not converted to ms)", () => {
    const pipeline = Pipeline({
      name: "test",
      checkpoint: { interval: "30s" },
      upgradeStrategy: {
        mode: "blue-green",
        blueGreen: { abortGracePeriod: "10m" },
      },
      children: [],
    })

    const crd = generateCrd(pipeline, {
      flinkVersion: "2.0",
    }) as FlinkBlueGreenDeploymentCrd

    // "10m" should stay as "10m", not be converted to "600000"
    expect(crd.spec.configuration?.["blue-green.abort.grace-period"]).toBe(
      "10m",
    )
  })

  it("omits spec.configuration when no BG config provided", () => {
    const pipeline = Pipeline({
      name: "test",
      checkpoint: { interval: "30s" },
      upgradeStrategy: { mode: "blue-green" },
      children: [],
    })

    const crd = generateCrd(pipeline, {
      flinkVersion: "2.0",
    }) as FlinkBlueGreenDeploymentCrd

    expect(crd.spec.configuration).toBeUndefined()
  })

  it("produces snapshot-stable YAML with full config", () => {
    const source = makeSource()
    const sink = makeSink(source)

    const pipeline = Pipeline({
      name: "order-processor",
      parallelism: 4,
      checkpoint: { interval: "30s", mode: "exactly-once" },
      upgradeStrategy: {
        mode: "blue-green",
        upgradeMode: "savepoint",
        blueGreen: {
          abortGracePeriod: "10m",
          deploymentDeletionDelay: "2s",
        },
      },
      children: [sink],
    })

    const yaml = generateCrdYaml(pipeline, { flinkVersion: "2.0" })
    expect(yaml).toMatchSnapshot()
  })
})

// ── BG with ingress ─────────────────────────────────────────────────

describe("Blue-green CRD: ingress config", () => {
  it("maps ingress to spec.ingress", () => {
    const pipeline = Pipeline({
      name: "test",
      checkpoint: { interval: "30s" },
      upgradeStrategy: {
        mode: "blue-green",
        ingress: {
          template: "{{name}}.{{namespace}}.example.com",
          className: "nginx",
          annotations: { "nginx.ingress.kubernetes.io/rewrite-target": "/" },
        },
      },
      children: [],
    })

    const crd = generateCrd(pipeline, {
      flinkVersion: "2.0",
    }) as FlinkBlueGreenDeploymentCrd

    expect(crd.spec.ingress).toEqual({
      template: "{{name}}.{{namespace}}.example.com",
      className: "nginx",
      annotations: { "nginx.ingress.kubernetes.io/rewrite-target": "/" },
    })
  })

  it("omits spec.ingress when not provided", () => {
    const pipeline = Pipeline({
      name: "test",
      checkpoint: { interval: "30s" },
      upgradeStrategy: { mode: "blue-green" },
      children: [],
    })

    const crd = generateCrd(pipeline, {
      flinkVersion: "2.0",
    }) as FlinkBlueGreenDeploymentCrd

    expect(crd.spec.ingress).toBeUndefined()
  })

  it("produces snapshot-stable YAML with ingress", () => {
    const pipeline = Pipeline({
      name: "order-processor",
      parallelism: 4,
      checkpoint: { interval: "30s" },
      upgradeStrategy: {
        mode: "blue-green",
        ingress: {
          template: "{{name}}.example.com",
          className: "nginx",
        },
      },
      children: [],
    })

    const yaml = generateCrdYaml(pipeline, { flinkVersion: "2.0" })
    expect(yaml).toMatchSnapshot()
  })
})

// ── Validation ──────────────────────────────────────────────────────

describe("Blue-green validation", () => {
  it("throws error when BG + batch mode", () => {
    expect(() =>
      Pipeline({
        name: "test",
        mode: "batch",
        upgradeStrategy: { mode: "blue-green" },
        children: [],
      }),
    ).toThrow(
      "Blue-green upgrade strategy is not supported for batch pipelines",
    )
  })

  it("emits warning when BG without checkpoint", () => {
    Pipeline({
      name: "test",
      upgradeStrategy: { mode: "blue-green" },
      children: [],
    })

    const warnings = consumeValidationWarnings()
    expect(warnings).toHaveLength(1)
    expect(warnings[0].level).toBe("warning")
    expect(warnings[0].message).toContain("checkpoint")
  })

  it("no warning when BG with checkpoint", () => {
    Pipeline({
      name: "test",
      checkpoint: { interval: "30s" },
      upgradeStrategy: { mode: "blue-green" },
      children: [],
    })

    const warnings = consumeValidationWarnings()
    expect(warnings).toHaveLength(0)
  })
})

// ── Regression: standard pipeline unchanged ─────────────────────────

describe("Standard FlinkDeployment regression", () => {
  it("still produces FlinkDeployment kind without upgradeStrategy", () => {
    const source = makeSource()
    const sink = makeSink(source)

    const pipeline = Pipeline({
      name: "orders",
      parallelism: 4,
      checkpoint: { interval: "60s" },
      children: [sink],
    })

    const crd = generateCrd(pipeline, {
      flinkVersion: "2.0",
    }) as FlinkDeploymentCrd
    expect(crd.kind).toBe("FlinkDeployment")
    expect(crd.spec.job.parallelism).toBe(4)
    expect(
      crd.spec.flinkConfiguration["execution.checkpointing.interval"],
    ).toBe("60000")
  })

  it("produces snapshot-stable YAML (unchanged)", () => {
    const source = makeSource()
    const sink = makeSink(source)

    const pipeline = Pipeline({
      name: "orders",
      parallelism: 4,
      checkpoint: { interval: "60s", mode: "exactly-once" },
      children: [sink],
    })

    const yaml = generateCrdYaml(pipeline, { flinkVersion: "2.0" })
    expect(yaml).toMatchSnapshot()
  })
})

// ── CRD utility functions ───────────────────────────────────────────

describe("getFlinkConfiguration / withFlinkConfiguration", () => {
  it("extracts config from FlinkDeploymentCrd", () => {
    const pipeline = Pipeline({
      name: "test",
      checkpoint: { interval: "30s" },
      children: [],
    })
    const crd = generateCrd(pipeline, {
      flinkVersion: "2.0",
    }) as FlinkDeploymentCrd

    const config = getFlinkConfiguration(crd)
    expect(config["execution.checkpointing.interval"]).toBe("30000")
  })

  it("extracts config from FlinkBlueGreenDeploymentCrd", () => {
    const pipeline = Pipeline({
      name: "test",
      checkpoint: { interval: "30s" },
      upgradeStrategy: { mode: "blue-green" },
      children: [],
    })
    const crd = generateCrd(pipeline, {
      flinkVersion: "2.0",
    }) as FlinkBlueGreenDeploymentCrd

    const config = getFlinkConfiguration(crd)
    expect(config["execution.checkpointing.interval"]).toBe("30000")
  })

  it("returns new CRD with updated config (FlinkDeployment)", () => {
    const pipeline = Pipeline({ name: "test", children: [] })
    const crd = generateCrd(pipeline, { flinkVersion: "2.0" })

    const updated = withFlinkConfiguration(crd, { "custom.key": "value" })
    expect(getFlinkConfiguration(updated)).toEqual({ "custom.key": "value" })
    expect(updated.kind).toBe("FlinkDeployment")
  })

  it("returns new CRD with updated config (BlueGreen)", () => {
    const pipeline = Pipeline({
      name: "test",
      checkpoint: { interval: "30s" },
      upgradeStrategy: { mode: "blue-green" },
      children: [],
    })
    const crd = generateCrd(pipeline, { flinkVersion: "2.0" })

    const updated = withFlinkConfiguration(crd, { "custom.key": "value" })
    expect(getFlinkConfiguration(updated)).toEqual({ "custom.key": "value" })
    expect(updated.kind).toBe("FlinkBlueGreenDeployment")
  })
})

// ── Plugin receives AnyFlinkCrd ─────────────────────────────────────

describe("Plugin transformCrd with AnyFlinkCrd", () => {
  it("plugin can discriminate on crd.kind", () => {
    const pipeline = Pipeline({
      name: "test",
      checkpoint: { interval: "30s" },
      upgradeStrategy: { mode: "blue-green" },
      children: [],
    })
    const crd = generateCrd(pipeline, { flinkVersion: "2.0" })

    // Simulate a plugin transformCrd that handles both kinds
    if (crd.kind === "FlinkBlueGreenDeployment") {
      expect(crd.spec.template.spec.job.upgradeMode).toBe("savepoint")
    } else {
      // Should not reach here
      expect.unreachable()
    }
  })
})
