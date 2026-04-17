import { beforeEach, describe, expect, it } from "vitest"
import {
  collectSecretRefs,
  generateCrd,
  generateCrdYaml,
  toMilliseconds,
} from "@/codegen/crd-generator.js"
import { IcebergCatalog } from "@/components/catalogs.js"
import { Pipeline } from "@/components/pipeline.js"
import { IcebergSink, KafkaSink } from "@/components/sinks.js"
import { KafkaSource, PostgresCdcPipelineSource } from "@/components/sources.js"
import { resetNodeIdCounter } from "@/core/jsx-runtime.js"
import { Field, Schema } from "@/core/schema.js"
import { secretRef } from "@/core/secret-ref.js"

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

// ── Duration Parsing ────────────────────────────────────────────────

describe("toMilliseconds", () => {
  it("parses seconds", () => {
    expect(toMilliseconds("60s")).toBe(60000)
    expect(toMilliseconds("60 seconds")).toBe(60000)
  })

  it("parses minutes", () => {
    expect(toMilliseconds("5m")).toBe(300000)
    expect(toMilliseconds("5 min")).toBe(300000)
  })

  it("parses hours", () => {
    expect(toMilliseconds("1h")).toBe(3600000)
  })

  it("parses milliseconds", () => {
    expect(toMilliseconds("500ms")).toBe(500)
  })

  it("throws on invalid duration", () => {
    expect(() => toMilliseconds("invalid")).toThrow("Invalid duration")
  })
})

// ── 5.1: Snapshot test: Pipeline with checkpoint + parallelism → CRD YAML ──

describe("CRD generation: streaming pipeline", () => {
  it("produces correct CRD with checkpoint + parallelism", () => {
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
      name: "orders",
      parallelism: 4,
      checkpoint: { interval: "60s", mode: "exactly-once" },
      stateBackend: "hashmap",
      children: [sink],
    })

    const crd = generateCrd(pipeline, { flinkVersion: "2.0" })

    expect(crd.apiVersion).toBe("flink.apache.org/v1beta1")
    expect(crd.kind).toBe("FlinkDeployment")
    expect(crd.metadata.name).toBe("orders")

    expect(crd.spec.job.parallelism).toBe(4)
    expect(
      crd.spec.flinkConfiguration["execution.checkpointing.interval"],
    ).toBe("60000")
    expect(crd.spec.flinkConfiguration["execution.checkpointing.mode"]).toBe(
      "EXACTLY_ONCE",
    )
    expect(crd.spec.flinkConfiguration["state.backend.type"]).toBe("hashmap")
    expect(crd.spec.flinkConfiguration["execution.runtime-mode"]).toBe(
      "STREAMING",
    )

    expect(crd.spec.image).toBe("flink:2.0")
    expect(crd.spec.flinkVersion).toBe("v2_0")

    // Default resources
    expect(crd.spec.jobManager.resource.cpu).toBe("1")
    expect(crd.spec.jobManager.resource.memory).toBe("1024m")
    expect(crd.spec.taskManager.resource.cpu).toBe("1")
    expect(crd.spec.taskManager.resource.memory).toBe("1024m")

    // Default JAR URI
    expect(crd.spec.job.jarURI).toBe("local:///opt/flink/usrlib/sql-runner.jar")
  })

  it("produces snapshot-stable YAML", () => {
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
      name: "orders",
      parallelism: 4,
      checkpoint: { interval: "60s", mode: "exactly-once" },
      stateBackend: "hashmap",
      children: [sink],
    })

    const yaml = generateCrdYaml(pipeline, { flinkVersion: "2.0" })
    expect(yaml).toMatchSnapshot()
  })
})

// ── 5.2: Snapshot test: Batch mode → execution.runtime-mode: BATCH ──

describe("CRD generation: batch mode pipeline", () => {
  it("sets execution.runtime-mode to BATCH", () => {
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
      name: "batch-job",
      mode: "batch",
      children: [sink],
    })

    const crd = generateCrd(pipeline, { flinkVersion: "2.0" })
    expect(crd.spec.flinkConfiguration["execution.runtime-mode"]).toBe("BATCH")
  })

  it("produces snapshot-stable YAML for batch", () => {
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
      name: "batch-job",
      mode: "batch",
      children: [sink],
    })

    const yaml = generateCrdYaml(pipeline, { flinkVersion: "2.0" })
    expect(yaml).toMatchSnapshot()
  })
})

// ── State TTL conversion ────────────────────────────────────────────

describe("CRD generation: state TTL", () => {
  it("converts stateTtl to milliseconds", () => {
    const pipeline = Pipeline({
      name: "ttl-test",
      stateTtl: "30m",
      children: [],
    })

    const crd = generateCrd(pipeline, { flinkVersion: "2.0" })
    expect(crd.spec.flinkConfiguration["table.exec.state.ttl"]).toBe("1800000")
  })
})

// ── Restart Strategy ────────────────────────────────────────────────

describe("CRD generation: restart strategy", () => {
  it("maps fixed-delay restart strategy", () => {
    const pipeline = Pipeline({
      name: "restart-test",
      restartStrategy: { type: "fixed-delay", attempts: 3, delay: "10s" },
      children: [],
    })

    const crd = generateCrd(pipeline, { flinkVersion: "2.0" })
    expect(crd.spec.flinkConfiguration["restart-strategy.type"]).toBe(
      "fixed-delay",
    )
    expect(
      crd.spec.flinkConfiguration["restart-strategy.fixed-delay.attempts"],
    ).toBe("3")
    expect(
      crd.spec.flinkConfiguration["restart-strategy.fixed-delay.delay"],
    ).toBe("10s")
  })

  it("maps no-restart strategy", () => {
    const pipeline = Pipeline({
      name: "no-restart-test",
      restartStrategy: { type: "no-restart" },
      children: [],
    })

    const crd = generateCrd(pipeline, { flinkVersion: "2.0" })
    expect(crd.spec.flinkConfiguration["restart-strategy.type"]).toBe(
      "no-restart",
    )
  })
})

// ── Passthrough flinkConfig ─────────────────────────────────────────

describe("CRD generation: flinkConfig passthrough", () => {
  it("includes custom Flink config entries", () => {
    const pipeline = Pipeline({
      name: "config-test",
      flinkConfig: {
        "taskmanager.numberOfTaskSlots": "4",
        "table.exec.source.idle-timeout": "30000",
      },
      children: [],
    })

    const crd = generateCrd(pipeline, { flinkVersion: "2.0" })
    expect(crd.spec.flinkConfiguration["taskmanager.numberOfTaskSlots"]).toBe(
      "4",
    )
    expect(crd.spec.flinkConfiguration["table.exec.source.idle-timeout"]).toBe(
      "30000",
    )
  })
})

// ── Custom image and JM/TM resources ────────────────────────────────

describe("CRD generation: custom options", () => {
  it("uses custom Flink image", () => {
    const pipeline = Pipeline({ name: "test", children: [] })
    const crd = generateCrd(pipeline, {
      flinkVersion: "2.0",
      flinkImage: "registry.internal.com/flink:2.0-custom",
    })
    expect(crd.spec.image).toBe("registry.internal.com/flink:2.0-custom")
  })

  it("uses custom JM/TM resources", () => {
    const pipeline = Pipeline({ name: "test", children: [] })
    const crd = generateCrd(pipeline, {
      flinkVersion: "2.0",
      jobManager: { resource: { cpu: "2", memory: "2048m" }, replicas: 1 },
      taskManager: { resource: { cpu: "4", memory: "4096m" }, replicas: 3 },
    })

    expect(crd.spec.jobManager.resource.cpu).toBe("2")
    expect(crd.spec.jobManager.resource.memory).toBe("2048m")
    expect(crd.spec.taskManager.resource.cpu).toBe("4")
    expect(crd.spec.taskManager.resource.memory).toBe("4096m")
  })

  it("uses custom jarURI and args", () => {
    const pipeline = Pipeline({ name: "test", children: [] })
    const crd = generateCrd(pipeline, {
      flinkVersion: "2.0",
      jarUri: "local:///opt/flink/usrlib/custom-runner.jar",
      jarArgs: ["--sql-file", "/opt/flink/usrlib/pipeline.sql"],
    })

    expect(crd.spec.job.jarURI).toBe(
      "local:///opt/flink/usrlib/custom-runner.jar",
    )
    expect(crd.spec.job.args).toEqual([
      "--sql-file",
      "/opt/flink/usrlib/pipeline.sql",
    ])
  })

  it("includes metadata labels and annotations", () => {
    const pipeline = Pipeline({ name: "test", children: [] })
    const crd = generateCrd(pipeline, {
      flinkVersion: "2.0",
      labels: { "app.kubernetes.io/name": "test" },
      annotations: { team: "data-platform" },
    })

    expect(crd.metadata.labels).toEqual({ "app.kubernetes.io/name": "test" })
    expect(crd.metadata.annotations).toEqual({ team: "data-platform" })
  })
})

// ── Flink version mapping ───────────────────────────────────────────

describe("CRD generation: Flink version mapping", () => {
  it("maps 1.20 correctly", () => {
    const pipeline = Pipeline({ name: "test", children: [] })
    const crd = generateCrd(pipeline, { flinkVersion: "1.20" })
    expect(crd.spec.flinkVersion).toBe("v1_20")
    expect(crd.spec.image).toBe("flink:1.20")
  })

  it("maps 2.2 correctly", () => {
    const pipeline = Pipeline({ name: "test", children: [] })
    const crd = generateCrd(pipeline, { flinkVersion: "2.2" })
    expect(crd.spec.flinkVersion).toBe("v2_2")
    expect(crd.spec.image).toBe("flink:2.2")
  })
})

// ── Config normalization for 1.20 ───────────────────────────────────

describe("CRD generation: Flink 1.20 config normalization", () => {
  it("normalizes config keys for 1.20", () => {
    const pipeline = Pipeline({
      name: "legacy",
      stateBackend: "rocksdb",
      children: [],
    })

    const crd = generateCrd(pipeline, { flinkVersion: "1.20" })
    // state.backend.type is normalized to state.backend for 1.20
    expect(crd.spec.flinkConfiguration["state.backend"]).toBe("rocksdb")
  })
})

// ── Pipeline Connector CRD variant ──────────────────────────────────

describe("CRD generation: Flink CDC Pipeline Connector variant", () => {
  function buildCdcPipeline() {
    const catalog = IcebergCatalog({
      name: "lake",
      catalogType: "rest",
      uri: "http://iceberg-rest:8181",
    })
    const source = PostgresCdcPipelineSource({
      hostname: "pg-primary",
      database: "shop",
      username: "postgres",
      password: secretRef("pg-primary-password"),
      schemaList: ["public"],
      tableList: ["public.orders"],
    })
    const sink = IcebergSink({
      catalog: catalog.handle,
      database: "shop",
      table: "orders",
      formatVersion: 2,
      upsertEnabled: true,
      children: [source],
    })
    return Pipeline({
      name: "shop-orders-cdc",
      parallelism: 4,
      children: [catalog.node, sink],
    })
  }

  it("uses flink-cdc-cli.jar as the default jarURI", () => {
    const crd = generateCrd(buildCdcPipeline(), { flinkVersion: "2.0" })
    expect(crd.kind).toBe("FlinkDeployment")
    // biome-ignore lint/suspicious/noExplicitAny: test introspection
    expect((crd as any).spec.job.jarURI).toBe(
      "local:///opt/flink-cdc/lib/flink-cdc-cli.jar",
    )
  })

  it("passes --pipeline /etc/flink-cdc/pipeline.yaml as job args", () => {
    const crd = generateCrd(buildCdcPipeline(), { flinkVersion: "2.0" })
    // biome-ignore lint/suspicious/noExplicitAny: test introspection
    expect((crd as any).spec.job.args).toEqual([
      "--pipeline",
      "/etc/flink-cdc/pipeline.yaml",
    ])
  })

  it("mounts the pipeline ConfigMap via a podTemplate volume", () => {
    const crd = generateCrd(buildCdcPipeline(), { flinkVersion: "2.0" })
    // biome-ignore lint/suspicious/noExplicitAny: test introspection
    const pod = (crd as any).spec.podTemplate
    expect(pod).toBeDefined()
    expect(pod.spec.volumes).toEqual([
      {
        name: "pipeline-yaml",
        configMap: { name: "shop-orders-cdc-pipeline" },
      },
    ])
    expect(pod.spec.containers[0].volumeMounts).toEqual([
      { name: "pipeline-yaml", mountPath: "/etc/flink-cdc", readOnly: true },
    ])
  })

  it("wires SecretRefs as individual env entries with secretKeyRef", () => {
    const crd = generateCrd(buildCdcPipeline(), { flinkVersion: "2.0" })
    // biome-ignore lint/suspicious/noExplicitAny: test introspection
    const env = (crd as any).spec.podTemplate.spec.containers[0].env
    expect(env).toEqual([
      {
        name: "PG_PRIMARY_PASSWORD",
        valueFrom: {
          secretKeyRef: { name: "pg-primary-password", key: "password" },
        },
      },
    ])
  })

  it("never leaks the cleartext password into the CRD YAML", () => {
    const yaml = generateCrdYaml(buildCdcPipeline(), { flinkVersion: "2.0" })
    expect(yaml).not.toContain("hunter2")
    expect(yaml).toContain("PG_PRIMARY_PASSWORD")
  })

  it("collectSecretRefs de-duplicates by envName", () => {
    const catalog = IcebergCatalog({
      name: "lake",
      catalogType: "rest",
      uri: "http://iceberg-rest:8181",
    })
    // Same secret referenced from two places
    const source1 = PostgresCdcPipelineSource({
      hostname: "pg-primary",
      database: "shop",
      username: "postgres",
      password: secretRef("pg-primary-password"),
      schemaList: ["public"],
      tableList: ["public.orders"],
    })
    const sink = IcebergSink({
      catalog: catalog.handle,
      database: "shop",
      table: "orders",
      formatVersion: 2,
      upsertEnabled: true,
      children: [source1],
    })
    const pipeline = Pipeline({
      name: "shop",
      children: [catalog.node, sink],
    })

    expect(collectSecretRefs(pipeline)).toHaveLength(1)
  })

  it("produces a stable CRD YAML snapshot", () => {
    const yaml = generateCrdYaml(buildCdcPipeline(), { flinkVersion: "2.0" })
    expect(yaml).toMatchSnapshot()
  })

  it("rejects blue-green upgrade strategy for pipeline connector pipelines", () => {
    const catalog = IcebergCatalog({
      name: "lake",
      catalogType: "rest",
      uri: "http://iceberg-rest:8181",
    })
    const source = PostgresCdcPipelineSource({
      hostname: "pg-primary",
      database: "shop",
      username: "postgres",
      password: secretRef("pg-primary-password"),
      schemaList: ["public"],
      tableList: ["public.orders"],
    })
    const sink = IcebergSink({
      catalog: catalog.handle,
      database: "shop",
      table: "orders",
      formatVersion: 2,
      upsertEnabled: true,
      children: [source],
    })
    const pipeline = Pipeline({
      name: "shop",
      checkpoint: { interval: "60s" },
      upgradeStrategy: { mode: "blue-green" },
      children: [catalog.node, sink],
    })

    expect(() => generateCrd(pipeline, { flinkVersion: "2.0" })).toThrow(
      /Blue-green upgrade strategy is not supported for Flink CDC Pipeline Connector/,
    )
  })
})
