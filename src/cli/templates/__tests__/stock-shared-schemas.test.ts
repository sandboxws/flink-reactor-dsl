import { describe, expect, it } from "vitest"
import { bundleReadme } from "@/cli/templates/stock-shared/readmes.js"
import {
  alertSimTable,
  currencyRateSchemaFile,
  currencyRateSimTable,
  customerSchemaFile,
  customerSimTable,
  eventAlertSchemaFile,
  eventSimTable,
  keyedEventSchemaFile,
  keyedEventSimTable,
  nameGradeSchemaFile,
  nameGradeSimTable,
  nameSalarySchemaFile,
  nameSalarySimTable,
  orderSchemaFile,
  orderSimTable,
  populationUpdatesSchemaFile,
  populationUpdatesSimTable,
  productSalesSchemaFile,
  productSalesSimTable,
  transactionSchemaFile,
  transactionSimTable,
  userScoreSchemaFile,
  userScoreSimTable,
  wordSchemaFile,
  wordSimTable,
} from "@/cli/templates/stock-shared/schemas.js"
import { Field, Schema, type SchemaDefinition } from "@/core/schema.js"

// ── Helpers ──────────────────────────────────────────────────────────

/**
 * Evaluate a scaffolded schema-file content string and return the resulting
 * SchemaDefinition. The content is expected to be a self-contained module
 * with one `import` line for `Schema` and `Field`, and a `export default
 * Schema({...})` expression.
 *
 * Stronger than `tsc --noEmit`: this actually constructs the schema at
 * runtime, so any type-validation logic inside `Schema()` (e.g., watermark
 * column existence checks) runs too.
 */
function evalSchemaContent(content: string): SchemaDefinition {
  const body = content
    .replace(
      /import\s+\{[^}]+\}\s+from\s+['"]@flink-reactor\/dsl['"];?\s*/g,
      "",
    )
    .replace(/export\s+default\s+/, "return ")
  // biome-ignore lint/security/noGlobalEval: trusted scaffolder-side code
  return new Function("Schema", "Field", body)(
    Schema,
    Field,
  ) as SchemaDefinition
}

interface ParsedSimTable {
  readonly table: string
  readonly topic: string
  readonly format: string
  readonly columns: Record<string, string>
  readonly watermark?: { column: string; expression: string }
  readonly primaryKey?: string[]
  readonly fields?: Record<string, unknown>
  readonly rowsPerSecond: number
}

function parseSimTable(expr: string): ParsedSimTable {
  // biome-ignore lint/security/noGlobalEval: trusted scaffolder-side code
  return new Function(`return ${expr}`)() as ParsedSimTable
}

// ── Per-schema parity tests ──────────────────────────────────────────

interface SchemaCase {
  readonly name: string
  readonly file: { path: string; content: string }
  readonly simTable: string
  readonly expectedFields: readonly string[]
  readonly expectedPath: string
}

const cases: readonly SchemaCase[] = [
  {
    name: "word",
    file: wordSchemaFile(),
    simTable: wordSimTable(),
    expectedFields: ["word", "frequency"],
    expectedPath: "schemas/word.ts",
  },
  {
    name: "order",
    file: orderSchemaFile(),
    simTable: orderSimTable(),
    expectedFields: ["user", "product", "amount", "ts"],
    expectedPath: "schemas/order.ts",
  },
  {
    name: "customer",
    file: customerSchemaFile(),
    simTable: customerSimTable(),
    expectedFields: [
      "name",
      "date_of_birth",
      "street",
      "zip_code",
      "city",
      "gender",
      "has_newsletter",
    ],
    expectedPath: "schemas/customer.ts",
  },
  {
    name: "transaction",
    file: transactionSchemaFile(),
    simTable: transactionSimTable(),
    expectedFields: ["id", "currencyCode", "amount", "trxTime"],
    expectedPath: "schemas/transaction.ts",
  },
  {
    name: "currencyRate",
    file: currencyRateSchemaFile(),
    simTable: currencyRateSimTable(),
    expectedFields: ["currencyCode", "euroRate", "rateTime"],
    expectedPath: "schemas/currency-rate.ts",
  },
  {
    name: "populationUpdates",
    file: populationUpdatesSchemaFile(),
    simTable: populationUpdatesSimTable(),
    expectedFields: ["city", "state", "updateYear", "populationDiff"],
    expectedPath: "schemas/population-updates.ts",
  },
  {
    name: "nameGrade",
    file: nameGradeSchemaFile(),
    simTable: nameGradeSimTable(),
    expectedFields: ["name", "grade", "time"],
    expectedPath: "schemas/name-grade.ts",
  },
  {
    name: "nameSalary",
    file: nameSalarySchemaFile(),
    simTable: nameSalarySimTable(),
    expectedFields: ["name", "salary", "time"],
    expectedPath: "schemas/name-salary.ts",
  },
  {
    name: "userScore",
    file: userScoreSchemaFile(),
    simTable: userScoreSimTable(),
    expectedFields: ["name", "score", "ts"],
    expectedPath: "schemas/user-score.ts",
  },
  {
    name: "productSales",
    file: productSalesSchemaFile(),
    simTable: productSalesSimTable(),
    expectedFields: ["productId", "timestamp"],
    expectedPath: "schemas/product-sales.ts",
  },
  {
    name: "keyedEvent",
    file: keyedEventSchemaFile(),
    simTable: keyedEventSimTable(),
    expectedFields: ["id", "ts", "value"],
    expectedPath: "schemas/keyed-event.ts",
  },
]

describe("stock-shared schema builders", () => {
  for (const c of cases) {
    describe(c.name, () => {
      it("returns { path, content } with the expected path and non-empty content", () => {
        expect(c.file.path).toBe(c.expectedPath)
        expect(c.file.content.length).toBeGreaterThan(0)
      })

      it("emits content that imports Schema/Field from @flink-reactor/dsl", () => {
        expect(c.file.content).toContain(
          "import { Schema, Field } from '@flink-reactor/dsl'",
        )
        expect(c.file.content).toContain("export default Schema(")
      })

      it("yields a valid SchemaDefinition with the expected fields", () => {
        const schema = evalSchemaContent(c.file.content)
        expect(Object.keys(schema.fields).sort()).toEqual(
          [...c.expectedFields].sort(),
        )
      })

      it("sim-table parses as a valid object literal", () => {
        const parsed = parseSimTable(c.simTable)
        expect(parsed.table).toBeTruthy()
        expect(parsed.topic).toBeTruthy()
        expect(parsed.format).toBeTruthy()
      })

      it("sim-table column count matches schema field count", () => {
        const schema = evalSchemaContent(c.file.content)
        const parsed = parseSimTable(c.simTable)
        expect(Object.keys(parsed.columns).length).toBe(
          Object.keys(schema.fields).length,
        )
      })

      it("sim-table column types match schema field types", () => {
        const schema = evalSchemaContent(c.file.content)
        const parsed = parseSimTable(c.simTable)
        for (const [field, type] of Object.entries(schema.fields)) {
          expect(parsed.columns[field]).toBe(type)
        }
      })
    })
  }
})

// ── Event/Alert pair (different shape: returns two files) ────────────

describe("eventAlertSchemaFile", () => {
  it("returns both event.ts and alert.ts", () => {
    const { event, alert } = eventAlertSchemaFile()
    expect(event.path).toBe("schemas/event.ts")
    expect(alert.path).toBe("schemas/alert.ts")
    expect(event.content.length).toBeGreaterThan(0)
    expect(alert.content.length).toBeGreaterThan(0)
  })

  it("event schema yields a SchemaDefinition with sourceAddress/type/timestamp", () => {
    const { event } = eventAlertSchemaFile()
    const schema = evalSchemaContent(event.content)
    expect(Object.keys(schema.fields).sort()).toEqual(
      ["sourceAddress", "timestamp", "type"].sort(),
    )
  })

  it("alert schema yields a SchemaDefinition with sourceAddress/state/transition/timestamp", () => {
    const { alert } = eventAlertSchemaFile()
    const schema = evalSchemaContent(alert.content)
    expect(Object.keys(schema.fields).sort()).toEqual(
      ["sourceAddress", "state", "timestamp", "transition"].sort(),
    )
  })

  it("eventSimTable column count matches event schema", () => {
    const { event } = eventAlertSchemaFile()
    const schema = evalSchemaContent(event.content)
    const parsed = parseSimTable(eventSimTable())
    expect(Object.keys(parsed.columns).length).toBe(
      Object.keys(schema.fields).length,
    )
  })

  it("alertSimTable column count matches alert schema", () => {
    const { alert } = eventAlertSchemaFile()
    const schema = evalSchemaContent(alert.content)
    const parsed = parseSimTable(alertSimTable())
    expect(Object.keys(parsed.columns).length).toBe(
      Object.keys(schema.fields).length,
    )
  })
})

// ── Snapshots for the most-shared schemas ────────────────────────────

describe("snapshot pins for canonical schemas", () => {
  it("Word schema content is stable", () => {
    expect(wordSchemaFile().content).toMatchSnapshot()
  })

  it("Order schema content is stable", () => {
    expect(orderSchemaFile().content).toMatchSnapshot()
  })
})

// ── bundleReadme ─────────────────────────────────────────────────────

describe("bundleReadme", () => {
  it("includes the template name, tagline, per-pipeline links, and Apache Flink mapping", () => {
    const file = bundleReadme({
      templateName: "stock-basics",
      tagline: "Apache Flink stock examples ported to FlinkReactor.",
      pipelines: [
        {
          name: "wordcount-sql",
          pitch: "Streaming word count via Flink SQL",
          sourceFile: "WordCountSQLExample.java",
        },
        {
          name: "stream-sql",
          pitch: "Tumbling-window order analytics",
          sourceFile: "StreamSQLExample.java",
        },
      ],
    })

    expect(file.path).toBe("README.md")
    expect(file.content).toContain("# Stock Basics")
    expect(file.content).toContain(
      "Apache Flink stock examples ported to FlinkReactor.",
    )
    expect(file.content).toContain(
      "[README](pipelines/wordcount-sql/README.md)",
    )
    expect(file.content).toContain("## Mapping to Apache Flink stock examples")
    expect(file.content).toContain("`WordCountSQLExample.java`")
    expect(file.content).toContain("`StreamSQLExample.java`")
  })

  it("preserves optional prerequisites and gettingStarted from templateReadme", () => {
    const file = bundleReadme({
      templateName: "stock-basics",
      tagline: "x",
      pipelines: [{ name: "p1", pitch: "p", sourceFile: "P.java" }],
      prerequisites: ["Kafka on :9092"],
      gettingStarted: ["pnpm install", "pnpm dev"],
    })

    expect(file.content).toContain("## Prerequisites")
    expect(file.content).toContain("- Kafka on :9092")
    expect(file.content).toContain("## Getting Started")
    expect(file.content).toContain("pnpm install")
    expect(file.content).toContain("## Mapping to Apache Flink stock examples")
  })
})
