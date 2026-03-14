import { beforeEach, describe, expect, it } from "vitest"
import { Pipeline } from "@/components/pipeline.js"
import { KafkaSink } from "@/components/sinks.js"
import { KafkaSource } from "@/components/sources.js"
import { Filter, Join } from "@/components/transforms.js"
import { resetNodeIdCounter } from "@/core/jsx-runtime.js"
import { Field, Schema } from "@/core/schema.js"
import { validateExpressionSyntax } from "@/core/schema-validation.js"
import { validateSqlExpression } from "@/core/sql-expr-validator.js"
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

// ── 5.1: sql-expr-validator unit tests ──────────────────────────────

describe("validateSqlExpression", () => {
  // ── 5.2: valid expression ─────────────────────────────────────────
  it('validates "amount > 100" as valid', async () => {
    const result = await validateSqlExpression("amount > 100")
    expect(result.valid).toBe(true)
    expect(result.errors).toHaveLength(0)
  })

  // ── 5.3: incomplete expression ────────────────────────────────────
  it('validates "amount > 100 AND" as invalid', async () => {
    const result = await validateSqlExpression("amount > 100 AND")
    expect(result.valid).toBe(false)
    expect(result.errors.length).toBeGreaterThan(0)
  })

  // ── 5.4: unbalanced parens ────────────────────────────────────────
  it("validates \"CONCAT('hello', name\" as invalid (unbalanced parens)", async () => {
    const result = await validateSqlExpression("CONCAT('hello', name")
    expect(result.valid).toBe(false)
    expect(result.errors.length).toBeGreaterThan(0)
  })

  // ── 5.5: unterminated string ──────────────────────────────────────
  it("validates \"status = 'active\" as invalid (unterminated string)", async () => {
    const result = await validateSqlExpression("status = 'active")
    expect(result.valid).toBe(false)
    expect(result.errors.length).toBeGreaterThan(0)
  })

  // ── 5.6: empty expression ────────────────────────────────────────
  it('validates "" as invalid (empty)', async () => {
    const result = await validateSqlExpression("")
    expect(result.valid).toBe(false)
    expect(result.errors).toHaveLength(1)
    expect(result.errors[0].message).toBe("Expression is empty")
  })
})

// ── 5.7–5.8: Integration with validateExpressionSyntax ──────────────

describe("validateExpressionSyntax integration", () => {
  function makeSource(...children: ConstructNode[]) {
    return KafkaSource({
      topic: "input",
      bootstrapServers: "kafka:9092",
      format: "json",
      schema: TestSchema,
      children:
        children.length === 1
          ? children[0]
          : children.length > 0
            ? children
            : undefined,
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

  // ── 5.7: Join on clause with invalid syntax → diagnostic ──────────
  it("emits diagnostic for Join on clause with invalid syntax", async () => {
    const joinNode: ConstructNode = {
      id: "join-1",
      kind: "Join",
      component: "Join",
      props: { on: "left.id = AND right.id" },
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

    const diagnostics = await validateExpressionSyntax(pipeline)
    const exprDiags = diagnostics.filter((d) => d.category === "expression")

    expect(exprDiags.length).toBeGreaterThan(0)
    expect(exprDiags[0].component).toBe("Join")
    expect(exprDiags[0].details?.expressionErrors).toBeDefined()
    expect(exprDiags[0].details!.expressionErrors!.length).toBeGreaterThan(0)
  })

  // ── 5.8: Diagnostic includes error offset ─────────────────────────
  it("diagnostic includes error offset in expressionErrors", async () => {
    const result = await validateSqlExpression("amount > 100 AND")
    expect(result.valid).toBe(false)
    expect(result.errors.length).toBeGreaterThan(0)
    // Error offset should be adjusted (relative to original expression, not wrapper)
    expect(result.errors[0].startColumn).toBeGreaterThanOrEqual(0)
    expect(result.errors[0].startLine).toBe(1)
  })

  it("emits diagnostic for Filter with invalid condition", async () => {
    const sink = makeSink()
    const filter = Filter({
      condition: "amount > 100 AND",
      children: sink,
    })
    const source = makeSource(filter)
    const pipeline = Pipeline({ name: "test", children: source })

    const diagnostics = await validateExpressionSyntax(pipeline)
    const exprDiags = diagnostics.filter((d) => d.category === "expression")

    expect(exprDiags.length).toBeGreaterThan(0)
    expect(exprDiags[0].component).toBe("Filter")
  })

  it("emits no diagnostics for valid expressions", async () => {
    const sink = makeSink()
    const filter = Filter({
      condition: "amount > 100",
      children: sink,
    })
    const source = makeSource(filter)
    const pipeline = Pipeline({ name: "test", children: source })

    const diagnostics = await validateExpressionSyntax(pipeline)
    expect(diagnostics).toHaveLength(0)
  })
})
