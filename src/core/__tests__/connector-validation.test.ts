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
      (d) => d.severity === "error" && d.details?.missingProps?.includes("topic"),
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

  // ── KafkaSink ────────────────────────────────────────────────────

  it("errors when KafkaSink is missing topic", () => {
    const tree = makePipeline(
      makeNode("KafkaSink", "Sink", {
        bootstrapServers: "localhost:9092",
      }),
    )

    const diags = validateConnectorProperties(tree)
    const topicErrors = diags.filter(
      (d) => d.severity === "error" && d.details?.missingProps?.includes("topic"),
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
      (d) => d.severity === "error" && d.details?.missingProps?.includes("path"),
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
      expect(d.details!.missingProps!.length).toBeGreaterThan(0)
    }
  })
})
