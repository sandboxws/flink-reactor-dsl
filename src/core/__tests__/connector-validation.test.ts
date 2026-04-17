import { describe, expect, it } from "vitest"
import { validateConnectorProperties } from "@/core/connector-validation.js"
import type { ConstructNode } from "@/core/types.js"

function makeNode(
  component: string,
  kind: ConstructNode["kind"],
  props: Record<string, unknown> = {},
  children: ConstructNode[] = [],
): ConstructNode {
  return { id: `${component}-1`, kind, component, props, children }
}

function makePipeline(...children: ConstructNode[]): ConstructNode {
  return makeNode("Pipeline", "Pipeline", { name: "test" }, children)
}

describe("validateConnectorProperties", () => {
  // ── KafkaSource ──────────────────────────────────────────────────

  it("errors when KafkaSource is missing topic", () => {
    const tree = makePipeline(
      makeNode("KafkaSource", "Source", {
        schema: { columns: [] },
        bootstrapServers: "localhost:9092",
      }),
    )

    const diags = validateConnectorProperties(tree)
    const topicErrors = diags.filter(
      (d) =>
        d.severity === "error" && d.details?.missingProps?.includes("topic"),
    )
    expect(topicErrors).toHaveLength(1)
    expect(topicErrors[0].category).toBe("connector")
    expect(topicErrors[0].component).toBe("KafkaSource")
  })

  it("errors when KafkaSource is missing schema", () => {
    const tree = makePipeline(
      makeNode("KafkaSource", "Source", {
        topic: "events",
        bootstrapServers: "localhost:9092",
      }),
    )

    const diags = validateConnectorProperties(tree)
    const schemaErrors = diags.filter(
      (d) =>
        d.severity === "error" && d.details?.missingProps?.includes("schema"),
    )
    expect(schemaErrors).toHaveLength(1)
    expect(schemaErrors[0].component).toBe("KafkaSource")
  })

  it("no error when KafkaSource has bootstrapServers from InfraConfig", () => {
    // After propagateInfraToChildren(), bootstrapServers is in props
    const tree = makePipeline(
      makeNode("KafkaSource", "Source", {
        topic: "events",
        schema: { columns: [] },
        bootstrapServers: "kafka:9092",
      }),
    )

    const diags = validateConnectorProperties(tree)
    expect(diags).toHaveLength(0)
  })

  it("errors when KafkaSource has no bootstrapServers and no InfraConfig", () => {
    const tree = makePipeline(
      makeNode("KafkaSource", "Source", {
        topic: "events",
        schema: { columns: [] },
      }),
    )

    const diags = validateConnectorProperties(tree)
    const bsErrors = diags.filter(
      (d) =>
        d.severity === "error" &&
        d.details?.missingProps?.includes("bootstrapServers"),
    )
    expect(bsErrors).toHaveLength(1)
    expect(bsErrors[0].component).toBe("KafkaSource")
  })

  it("errors when debezium-protobuf KafkaSource is missing schemaRegistryUrl", () => {
    const tree = makePipeline(
      makeNode("KafkaSource", "Source", {
        topic: "orders",
        schema: { columns: [] },
        bootstrapServers: "kafka:9092",
        format: "debezium-protobuf",
      }),
    )

    const diags = validateConnectorProperties(tree)
    const srErrors = diags.filter(
      (d) =>
        d.severity === "error" &&
        d.details?.missingProps?.includes("schemaRegistryUrl"),
    )
    expect(srErrors).toHaveLength(1)
    expect(srErrors[0].component).toBe("KafkaSource")
    expect(srErrors[0].message).toContain("debezium-protobuf")
    expect(srErrors[0].message).toContain("schema-registry.url")
  })

  it("errors when debezium-avro KafkaSource is missing schemaRegistryUrl", () => {
    const tree = makePipeline(
      makeNode("KafkaSource", "Source", {
        topic: "orders",
        schema: { columns: [] },
        bootstrapServers: "kafka:9092",
        format: "debezium-avro",
      }),
    )

    const diags = validateConnectorProperties(tree)
    const srErrors = diags.filter(
      (d) =>
        d.severity === "error" &&
        d.details?.missingProps?.includes("schemaRegistryUrl"),
    )
    expect(srErrors).toHaveLength(1)
  })

  it("no error when debezium-protobuf KafkaSource provides schemaRegistryUrl", () => {
    const tree = makePipeline(
      makeNode("KafkaSource", "Source", {
        topic: "orders",
        schema: { columns: [] },
        bootstrapServers: "kafka:9092",
        format: "debezium-protobuf",
        schemaRegistryUrl: "http://sr:8081",
      }),
    )

    const diags = validateConnectorProperties(tree)
    const srErrors = diags.filter((d) =>
      d.details?.missingProps?.includes("schemaRegistryUrl"),
    )
    expect(srErrors).toHaveLength(0)
  })

  it("no error for plain JSON KafkaSource without schemaRegistryUrl", () => {
    const tree = makePipeline(
      makeNode("KafkaSource", "Source", {
        topic: "events",
        schema: { columns: [] },
        bootstrapServers: "kafka:9092",
        format: "json",
      }),
    )

    const diags = validateConnectorProperties(tree)
    const srErrors = diags.filter((d) =>
      d.details?.missingProps?.includes("schemaRegistryUrl"),
    )
    expect(srErrors).toHaveLength(0)
  })

  // ── KafkaSink ────────────────────────────────────────────────────

  it("errors when KafkaSink is missing topic", () => {
    const tree = makePipeline(
      makeNode("KafkaSink", "Sink", {
        bootstrapServers: "localhost:9092",
      }),
    )

    const diags = validateConnectorProperties(tree)
    const topicErrors = diags.filter(
      (d) =>
        d.severity === "error" && d.details?.missingProps?.includes("topic"),
    )
    expect(topicErrors).toHaveLength(1)
  })

  // ── JdbcSink ─────────────────────────────────────────────────────

  it("errors when JdbcSink has upsertMode but no keyFields", () => {
    const tree = makePipeline(
      makeNode("JdbcSink", "Sink", {
        url: "jdbc:postgresql://localhost/db",
        table: "users",
        upsertMode: true,
      }),
    )

    const diags = validateConnectorProperties(tree)
    const keyFieldErrors = diags.filter(
      (d) =>
        d.severity === "error" &&
        d.details?.missingProps?.includes("keyFields"),
    )
    expect(keyFieldErrors).toHaveLength(1)
    expect(keyFieldErrors[0].component).toBe("JdbcSink")
  })

  it("no error when JdbcSink has no upsertMode and no keyFields", () => {
    const tree = makePipeline(
      makeNode("JdbcSink", "Sink", {
        url: "jdbc:postgresql://localhost/db",
        table: "users",
      }),
    )

    const diags = validateConnectorProperties(tree)
    expect(diags).toHaveLength(0)
  })

  // ── FileSystemSink ───────────────────────────────────────────────

  it("errors when FileSystemSink is missing path", () => {
    const tree = makePipeline(makeNode("FileSystemSink", "Sink", {}))

    const diags = validateConnectorProperties(tree)
    const pathErrors = diags.filter(
      (d) =>
        d.severity === "error" && d.details?.missingProps?.includes("path"),
    )
    expect(pathErrors).toHaveLength(1)
    expect(pathErrors[0].component).toBe("FileSystemSink")
  })

  // ── Standalone mode ──────────────────────────────────────────────

  it("emits warnings (not errors) for infra-provided props in standalone mode", () => {
    const tree = makePipeline(
      makeNode("KafkaSource", "Source", {
        topic: "events",
        schema: { columns: [] },
        // bootstrapServers intentionally missing
      }),
    )

    const diags = validateConnectorProperties(tree, { standalone: true })
    const bsDiags = diags.filter((d) =>
      d.details?.missingProps?.includes("bootstrapServers"),
    )
    expect(bsDiags).toHaveLength(1)
    expect(bsDiags[0].severity).toBe("warning")
    expect(bsDiags[0].category).toBe("connector")
  })

  // ── Diagnostic structure ─────────────────────────────────────────

  it("includes category and missingProps in all diagnostics", () => {
    const tree = makePipeline(
      makeNode("KafkaSource", "Source", {}), // missing topic, schema, bootstrapServers
    )

    const diags = validateConnectorProperties(tree)
    for (const d of diags) {
      expect(d.category).toBe("connector")
      expect(d.details?.missingProps).toBeDefined()
      expect(d.details?.missingProps?.length).toBeGreaterThan(0)
    }
  })

  // ── PostgresCdcPipelineSource ────────────────────────────────────

  function makeCdcSource(props: Record<string, unknown> = {}): ConstructNode {
    return makeNode("PostgresCdcPipelineSource", "Source", {
      hostname: "pg-primary",
      database: "shop",
      username: "postgres",
      password: { name: "pg", key: "password", envName: "PG" },
      schemaList: ["public"],
      tableList: ["public.orders"],
      ...props,
    })
  }

  it("errors when PostgresCdcPipelineSource is missing tableList", () => {
    const src = makeNode("PostgresCdcPipelineSource", "Source", {
      hostname: "pg-primary",
      database: "shop",
      username: "postgres",
      password: { name: "pg", key: "password", envName: "PG" },
      schemaList: ["public"],
    })
    const sink = makeNode(
      "IcebergSink",
      "Sink",
      { formatVersion: 2, upsertEnabled: true },
      [src],
    )
    const tree = makePipeline(sink)
    const diags = validateConnectorProperties(tree)
    const missing = diags.find(
      (d) =>
        d.component === "PostgresCdcPipelineSource" &&
        d.details?.missingProps?.includes("tableList"),
    )
    expect(missing).toBeDefined()
  })

  it("errors when a PostgresCdcPipelineSource feeds a KafkaSink", () => {
    const src = makeCdcSource()
    const sink = makeNode("KafkaSink", "Sink", { topic: "out" }, [src])
    const tree = makePipeline(sink)
    const diags = validateConnectorProperties(tree)
    const compat = diags.find(
      (d) =>
        d.severity === "error" &&
        d.message.includes("PostgresCdcPipelineSource") &&
        d.message.includes("KafkaSink"),
    )
    expect(compat).toBeDefined()
    expect(compat?.details?.sourceNodeId).toBe(src.id)
    expect(compat?.details?.sinkNodeId).toBe(sink.id)
  })

  it("errors when an IcebergSink has formatVersion=1 downstream of a CDC source", () => {
    const src = makeCdcSource()
    const sink = makeNode(
      "IcebergSink",
      "Sink",
      { formatVersion: 1, upsertEnabled: true },
      [src],
    )
    const tree = makePipeline(sink)
    const diags = validateConnectorProperties(tree)
    expect(
      diags.some(
        (d) =>
          d.message.includes("IcebergSink") &&
          d.message.includes("formatVersion"),
      ),
    ).toBe(true)
  })

  it("errors when IcebergSink lacks upsertEnabled", () => {
    const src = makeCdcSource()
    const sink = makeNode("IcebergSink", "Sink", { formatVersion: 2 }, [src])
    const tree = makePipeline(sink)
    const diags = validateConnectorProperties(tree)
    expect(diags.some((d) => d.message.includes("upsertEnabled"))).toBe(true)
  })

  it("passes when IcebergSink has formatVersion=2 + upsertEnabled", () => {
    const src = makeCdcSource()
    const sink = makeNode(
      "IcebergSink",
      "Sink",
      { formatVersion: 2, upsertEnabled: true },
      [src],
    )
    const tree = makePipeline(sink)
    const diags = validateConnectorProperties(tree)
    const compat = diags.find(
      (d) =>
        d.message.includes("PostgresCdcPipelineSource") &&
        d.message.includes("→"),
    )
    expect(compat).toBeUndefined()
  })

  it("passes when PaimonSink has a primary key downstream of a CDC source", () => {
    const src = makeCdcSource()
    const sink = makeNode("PaimonSink", "Sink", { primaryKey: ["order_id"] }, [
      src,
    ])
    const tree = makePipeline(sink)
    const diags = validateConnectorProperties(tree)
    const compat = diags.find(
      (d) =>
        d.message.includes("PostgresCdcPipelineSource") &&
        d.message.includes("→"),
    )
    expect(compat).toBeUndefined()
  })

  it("errors when PaimonSink has no primaryKey downstream of a CDC source", () => {
    const src = makeCdcSource()
    const sink = makeNode("PaimonSink", "Sink", {}, [src])
    const tree = makePipeline(sink)
    const diags = validateConnectorProperties(tree)
    expect(diags.some((d) => d.message.includes("primaryKey"))).toBe(true)
  })

  // ── IcebergSink MoR misconfiguration ─────────────────────────────

  it("errors when IcebergSink has upsertEnabled but neither equalityFieldColumns nor primaryKey", () => {
    const sink = makeNode("IcebergSink", "Sink", {
      upsertEnabled: true,
      formatVersion: 2,
    })
    const tree = makePipeline(sink)
    const diags = validateConnectorProperties(tree)
    const err = diags.find(
      (d) =>
        d.severity === "error" &&
        d.component === "IcebergSink" &&
        d.nodeId === sink.id &&
        /equalityFieldColumns.*primaryKey/i.test(d.message) &&
        /upsertEnabled/.test(d.message),
    )
    expect(err).toBeDefined()
    expect(err?.message).toContain(sink.id)
  })

  it("accepts IcebergSink upsertEnabled + primaryKey without equalityFieldColumns", () => {
    const sink = makeNode("IcebergSink", "Sink", {
      upsertEnabled: true,
      formatVersion: 2,
      primaryKey: ["order_id"],
    })
    const tree = makePipeline(sink)
    const diags = validateConnectorProperties(tree)
    const morErrors = diags.filter(
      (d) =>
        d.severity === "error" &&
        d.component === "IcebergSink" &&
        /equalityFieldColumns.*primaryKey/i.test(d.message),
    )
    expect(morErrors).toHaveLength(0)
  })

  it("accepts IcebergSink upsertEnabled + equalityFieldColumns without primaryKey", () => {
    const sink = makeNode("IcebergSink", "Sink", {
      upsertEnabled: true,
      formatVersion: 2,
      equalityFieldColumns: ["order_id"],
    })
    const tree = makePipeline(sink)
    const diags = validateConnectorProperties(tree)
    const morErrors = diags.filter(
      (d) =>
        d.severity === "error" &&
        d.component === "IcebergSink" &&
        /equalityFieldColumns.*primaryKey/i.test(d.message),
    )
    expect(morErrors).toHaveLength(0)
  })

  it("warns (non-fatal) when writeDistributionMode='none' and parallelism > 1", () => {
    const sink = makeNode("IcebergSink", "Sink", {
      upsertEnabled: true,
      primaryKey: ["order_id"],
      writeDistributionMode: "none",
    })
    const tree = makeNode(
      "Pipeline",
      "Pipeline",
      {
        name: "test",
        parallelism: 4,
      },
      [sink],
    )
    const diags = validateConnectorProperties(tree)
    const warn = diags.find(
      (d) =>
        d.severity === "warning" &&
        d.component === "IcebergSink" &&
        /small file/i.test(d.message) &&
        /parallelism/.test(d.message),
    )
    expect(warn).toBeDefined()

    const morFatal = diags.find(
      (d) =>
        d.severity === "error" &&
        d.component === "IcebergSink" &&
        /distribution/i.test(d.message),
    )
    expect(morFatal).toBeUndefined()
  })

  it("does not warn about distribution=none when parallelism is 1 or unset", () => {
    const sink = makeNode("IcebergSink", "Sink", {
      upsertEnabled: true,
      primaryKey: ["order_id"],
      writeDistributionMode: "none",
    })
    const tree = makePipeline(sink)
    const diags = validateConnectorProperties(tree)
    const warn = diags.find(
      (d) =>
        d.severity === "warning" &&
        d.component === "IcebergSink" &&
        /small file/i.test(d.message),
    )
    expect(warn).toBeUndefined()
  })

  it("errors when tap is set on a PostgresCdcPipelineSource", () => {
    const src = makeCdcSource({ tap: true })
    const sink = makeNode(
      "IcebergSink",
      "Sink",
      { formatVersion: 2, upsertEnabled: true },
      [src],
    )
    const tree = makePipeline(sink)
    const diags = validateConnectorProperties(tree)
    const tapErr = diags.find(
      (d) =>
        d.severity === "error" &&
        d.component === "PostgresCdcPipelineSource" &&
        /operator tapping is not supported/i.test(d.message),
    )
    expect(tapErr).toBeDefined()
  })
})
