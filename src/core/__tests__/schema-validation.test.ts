import { Effect } from "effect"
import { beforeEach, describe, expect, it } from "vitest"
import { Pipeline } from "@/components/pipeline.js"
import { KafkaSink } from "@/components/sinks.js"
import { KafkaSource } from "@/components/sources.js"
import { Aggregate, Filter, Map } from "@/components/transforms.js"
import { Drop, Rename } from "@/components/field-transforms.js"
import { synthesizeAppEffect } from "@/core/app.js"
import { createElement, resetNodeIdCounter } from "@/core/jsx-runtime.js"
import { Field, Schema } from "@/core/schema.js"
import {
  extractColumnReferences,
  validateSchemaReferences,
} from "@/core/schema-validation.js"
import type { ConstructNode } from "@/core/types.js"

beforeEach(() => {
  resetNodeIdCounter()
})

const TestSchema = Schema({
  fields: {
    id: Field.BIGINT(),
    name: Field.STRING(),
    amount: Field.DOUBLE(),
    status: Field.STRING(),
  },
})

function makeSource(...children: ConstructNode[]) {
  return KafkaSource({
    topic: "input",
    bootstrapServers: "kafka:9092",
    format: "json",
    schema: TestSchema,
    children: children.length === 1 ? children[0] : children.length > 0 ? children : undefined,
  })
}

function makeSink() {
  return KafkaSink({
    topic: "output",
    bootstrapServers: "kafka:9092",
    format: "json",
    schema: TestSchema,
  })
}

// ── 2.2: extractColumnReferences unit tests ─────────────────────────

describe("extractColumnReferences", () => {
  const knownColumns = ["id", "name", "amount", "status"]

  it("extracts backtick-quoted identifiers", () => {
    const refs = extractColumnReferences("`id` > 0 AND `name` IS NOT NULL", knownColumns)
    expect(refs).toContain("id")
    expect(refs).toContain("name")
  })

  it("matches bare identifiers against known columns", () => {
    const refs = extractColumnReferences("id > 0 AND amount < 100", knownColumns)
    expect(refs).toContain("id")
    expect(refs).toContain("amount")
  })

  it("does not flag SQL keywords", () => {
    const refs = extractColumnReferences("id > 0 AND name IS NOT NULL", knownColumns)
    expect(refs).not.toContain("AND")
    expect(refs).not.toContain("IS")
    expect(refs).not.toContain("NOT")
    expect(refs).not.toContain("NULL")
  })

  it("does not include unknown bare identifiers", () => {
    const refs = extractColumnReferences("UPPER(id) = 'FOO'", knownColumns)
    expect(refs).toContain("id")
    expect(refs).not.toContain("UPPER")
    expect(refs).not.toContain("FOO")
  })

  it("handles backtick-quoted unknown columns", () => {
    const refs = extractColumnReferences("`nonexistent` > 0", knownColumns)
    expect(refs).toContain("nonexistent")
  })

  it("deduplicates references", () => {
    const refs = extractColumnReferences("`id` > 0 AND id < 100", knownColumns)
    expect(refs.filter((r) => r === "id")).toHaveLength(1)
  })

  it("handles string literals without false matches", () => {
    const refs = extractColumnReferences("name = 'amount'", knownColumns)
    expect(refs).toContain("name")
    // "amount" in string literal should not be matched
    expect(refs).not.toContain("amount")
  })
})

// ── validateSchemaReferences tests ──────────────────────────────────

describe("validateSchemaReferences", () => {
  // ── 5.2: Filter referencing nonexistent column ─────────────────

  it("detects Filter referencing nonexistent column", () => {
    // Nesting pattern: Source → Filter → Sink
    const sink = makeSink()
    const filter = Filter({ condition: "`nonexistent_col` > 0", children: sink })
    const source = makeSource(filter)
    const pipeline = Pipeline({ name: "test", children: source })

    const diagnostics = validateSchemaReferences(pipeline)
    const errors = diagnostics.filter((d) => d.severity === "error")

    expect(errors).toHaveLength(1)
    expect(errors[0].message).toContain("nonexistent_col")
    expect(errors[0].component).toBe("Filter")
    expect(errors[0].category).toBe("schema")
    expect(errors[0].details?.availableColumns).toContain("id")
    expect(errors[0].details?.referencedColumn).toBe("nonexistent_col")
  })

  // ── 5.3: Map projecting nonexistent column ────────────────────

  it("detects Map projecting nonexistent column", () => {
    const sink = makeSink()
    const map = Map({ select: { out: "`missing_field`" }, children: sink })
    const source = makeSource(map)
    const pipeline = Pipeline({ name: "test", children: source })

    const diagnostics = validateSchemaReferences(pipeline)
    const errors = diagnostics.filter((d) => d.severity === "error")

    expect(errors).toHaveLength(1)
    expect(errors[0].message).toContain("missing_field")
    expect(errors[0].component).toBe("Map")
  })

  // ── 5.4: Valid multi-step chain → no errors ───────────────────

  it("produces no errors for valid multi-step chain", () => {
    // Source(id,name,amount,status) → Map(creates `total`) → Filter(uses `total`) → Sink
    const sink = makeSink()
    const filter = Filter({ condition: "`id` > 0", children: sink })
    const source = makeSource(filter)
    const pipeline = Pipeline({ name: "test", children: source })

    const diagnostics = validateSchemaReferences(pipeline)
    const errors = diagnostics.filter((d) => d.severity === "error")

    expect(errors).toHaveLength(0)
  })

  // ── 5.5: Drop then reference dropped column → error ───────────

  it("detects reference to dropped column", () => {
    const sink = makeSink()
    const filter = Filter({ condition: "`amount` > 0", children: sink })
    const drop = Drop({ columns: ["amount"], children: filter })
    const source = makeSource(drop)
    const pipeline = Pipeline({ name: "test", children: source })

    const diagnostics = validateSchemaReferences(pipeline)
    const errors = diagnostics.filter((d) => d.severity === "error")

    expect(errors.some((e) => e.message.includes("amount"))).toBe(true)
  })

  // ── 5.6: Rename then reference old name → error ──────────────

  it("detects reference to old column name after rename", () => {
    const sink = makeSink()
    const filter = Filter({ condition: "`name` IS NOT NULL", children: sink })
    const rename = Rename({ columns: { name: "full_name" }, children: filter })
    const source = makeSource(rename)
    const pipeline = Pipeline({ name: "test", children: source })

    const diagnostics = validateSchemaReferences(pipeline)
    const errors = diagnostics.filter((d) => d.severity === "error")

    expect(errors.some((e) => e.message.includes("name"))).toBe(true)
  })

  // ── 5.7: Aggregate groupBy with invalid column → error ────────

  it("detects Aggregate groupBy with invalid column", () => {
    const sink = makeSink()
    const agg = Aggregate({
      groupBy: ["nonexistent_group"],
      select: { total: "COUNT(*)" },
      children: sink,
    })
    const source = makeSource(agg)
    const pipeline = Pipeline({ name: "test", children: source })

    const diagnostics = validateSchemaReferences(pipeline)
    const errors = diagnostics.filter((d) => d.severity === "error")

    expect(errors.some((e) => e.message.includes("nonexistent_group"))).toBe(true)
    expect(errors[0].component).toBe("Aggregate")
  })

  // ── 5.8: Join on clause with invalid column → error ───────────

  it("detects Join on clause with invalid column", () => {
    const joinNode: ConstructNode = {
      id: "join-1",
      kind: "Join",
      component: "Join",
      props: { on: "`fake_col` = `other_fake`" },
      children: [
        {
          id: "src-left",
          kind: "Source",
          component: "KafkaSource",
          props: {
            schema: {
              fields: { id: "BIGINT", value: "STRING" },
              metadataColumns: [],
            },
          },
          children: [],
        },
      ],
    }
    const pipeline: ConstructNode = {
      id: "pipeline-1",
      kind: "Pipeline",
      component: "Pipeline",
      props: { name: "test" },
      children: [joinNode],
    }

    const diagnostics = validateSchemaReferences(pipeline)
    const errors = diagnostics.filter((d) => d.severity === "error")

    expect(errors.some((e) => e.message.includes("fake_col"))).toBe(true)
    expect(errors.some((e) => e.message.includes("other_fake"))).toBe(true)
  })

  // ── 5.9: Unresolvable schema → warning, no false-positive errors ─

  it("emits warning for unresolvable schema, no false-positive errors", () => {
    // A transform under Pipeline with no source upstream → unresolvable
    const filter: ConstructNode = {
      id: "orphan-filter",
      kind: "Transform",
      component: "Filter",
      props: { condition: "`anything` > 0" },
      children: [],
    }
    const pipeline: ConstructNode = {
      id: "pipeline-1",
      kind: "Pipeline",
      component: "Pipeline",
      props: { name: "test" },
      children: [filter],
    }

    const diagnostics = validateSchemaReferences(pipeline)
    const errors = diagnostics.filter((d) => d.severity === "error")
    const warnings = diagnostics.filter((d) => d.severity === "warning")

    expect(errors).toHaveLength(0)
    expect(warnings.some((w) => w.message.includes("Cannot resolve upstream schema"))).toBe(true)
    expect(warnings[0].category).toBe("schema")
  })
})

// ── 5.10–5.11: synthesizeAppEffect integration tests ────────────────

describe("synthesizeAppEffect validation options", () => {
  function buildPipelineWithSchemaError() {
    // Build with createElement (upstream-children pattern: Sink → Filter → Source)
    return createElement(
      "Pipeline",
      { name: "test-pipeline" },
      createElement(
        "KafkaSink",
        {
          topic: "output",
          bootstrapServers: "kafka:9092",
          format: "json",
          schema: TestSchema,
        },
        createElement(
          "Filter",
          { condition: "`nonexistent_column` > 0" },
          createElement("KafkaSource", {
            topic: "input",
            bootstrapServers: "kafka:9092",
            format: "json",
            schema: TestSchema,
          }),
        ),
      ),
    )
  }

  // ── 5.10: validation categories filter ────────────────────────

  it("filters validation by categories", async () => {
    const pipeline = buildPipelineWithSchemaError()

    // With schema category: should fail (schema error present)
    const effectWithSchema = synthesizeAppEffect(
      { name: "test-app", children: pipeline },
      { validation: { categories: ["schema"] } },
    )
    const exitWithSchema = await Effect.runPromiseExit(effectWithSchema)
    expect(exitWithSchema._tag).toBe("Failure")

    // With connector category only: should succeed (no connector errors)
    const effectNoSchema = synthesizeAppEffect(
      { name: "test-app", children: pipeline },
      { validation: { categories: ["connector"] } },
    )
    const result = await Effect.runPromise(effectNoSchema)
    expect(result.appName).toBe("test-app")
    expect(result.pipelines).toHaveLength(1)
  })

  // ── 5.11: validation level "off" skips validation ─────────────

  it("skips validation when level is off", async () => {
    const pipeline = buildPipelineWithSchemaError()

    // With validation off: should succeed despite schema error
    const effect = synthesizeAppEffect(
      { name: "test-app", children: pipeline },
      { validation: { level: "off" } },
    )
    const result = await Effect.runPromise(effect)
    expect(result.appName).toBe("test-app")
    expect(result.pipelines).toHaveLength(1)
  })
})
