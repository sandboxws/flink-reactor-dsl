// Stock-example schema library.
//
// Each `*SchemaFile()` returns a `TemplateFile` whose `content` is a
// self-contained TypeScript module using only the public DSL surface
// (`Schema`, `Field.*`). Each schema has a sibling `*SimTable()` helper
// that emits a JS-expression string suitable for inclusion in
// `flink-reactor.config.ts` `sim.init.kafka.tables` — keeping the
// scaffolded schema definition in lockstep with the simulator's
// Kafka-topic spec so Flink type drift can't sneak in.
import type { TemplateFile } from "@/cli/commands/new.js"

const HEADER = "import { Schema, Field } from '@flink-reactor/dsl';"

export interface SimTableOpts {
  /** Flink SQL table name. Defaults vary per schema. */
  readonly table?: string
  /** Kafka topic name. Defaults vary per schema. */
  readonly topic?: string
  /** Kafka message format (default 'json'). */
  readonly format?: string
  /** DataGen rows-per-second (default 10). */
  readonly rowsPerSecond?: number
}

// ── Word ─────────────────────────────────────────────────────────────
// Apache Flink: WordCountSQLExample, WindowWordCount

export function wordSchemaFile(): TemplateFile {
  return {
    path: "schemas/word.ts",
    content: `${HEADER}

export default Schema({
  fields: {
    word: Field.STRING(),
    frequency: Field.INT(),
  },
});
`,
  }
}

export function wordSimTable(opts: SimTableOpts = {}): string {
  const table = opts.table ?? "words"
  const topic = opts.topic ?? "words"
  const format = opts.format ?? "json"
  const rowsPerSecond = opts.rowsPerSecond ?? 10
  return `{
  table: '${table}',
  topic: '${topic}',
  format: '${format}',
  rowsPerSecond: ${rowsPerSecond},
  columns: {
    word: 'STRING',
    frequency: 'INT',
  },
  fields: {
    word: { kind: 'random', length: 8 },
    frequency: { kind: 'random', min: 1, max: 100 },
  },
}`
}

// ── Order ────────────────────────────────────────────────────────────
// Apache Flink: StreamSQLExample, StreamWindowSQLExample
// Java POJO: Order(user: Long, product: String, amount: Int) + ts

export function orderSchemaFile(): TemplateFile {
  return {
    path: "schemas/order.ts",
    content: `${HEADER}

export default Schema({
  fields: {
    user: Field.BIGINT(),
    product: Field.STRING(),
    amount: Field.INT(),
    ts: Field.TIMESTAMP(3),
  },
  watermark: { column: 'ts', expression: "ts - INTERVAL '5' SECOND" },
});
`,
  }
}

export function orderSimTable(opts: SimTableOpts = {}): string {
  const table = opts.table ?? "orders"
  const topic = opts.topic ?? "orders"
  const format = opts.format ?? "json"
  const rowsPerSecond = opts.rowsPerSecond ?? 10
  return `{
  table: '${table}',
  topic: '${topic}',
  format: '${format}',
  rowsPerSecond: ${rowsPerSecond},
  columns: {
    user: 'BIGINT',
    product: 'STRING',
    amount: 'INT',
    ts: 'TIMESTAMP(3)',
  },
  watermark: { column: 'ts', expression: "ts - INTERVAL '5' SECOND" },
  fields: {
    user: { kind: 'random', min: 1, max: 1000 },
    product: { kind: 'random', length: 6 },
    amount: { kind: 'random', min: 1, max: 100 },
  },
}`
}

// ── Customer ─────────────────────────────────────────────────────────
// Apache Flink: GettingStartedExample
// Java POJO: Customer(name, date_of_birth, street, zip_code, city, gender, has_newsletter)

export function customerSchemaFile(): TemplateFile {
  return {
    path: "schemas/customer.ts",
    content: `${HEADER}

export default Schema({
  fields: {
    name: Field.STRING(),
    date_of_birth: Field.DATE(),
    street: Field.STRING(),
    zip_code: Field.STRING(),
    city: Field.STRING(),
    gender: Field.STRING(),
    has_newsletter: Field.BOOLEAN(),
  },
});
`,
  }
}

export function customerSimTable(opts: SimTableOpts = {}): string {
  const table = opts.table ?? "customers"
  const topic = opts.topic ?? "customers"
  const format = opts.format ?? "json"
  const rowsPerSecond = opts.rowsPerSecond ?? 10
  return `{
  table: '${table}',
  topic: '${topic}',
  format: '${format}',
  rowsPerSecond: ${rowsPerSecond},
  columns: {
    name: 'STRING',
    date_of_birth: 'DATE',
    street: 'STRING',
    zip_code: 'STRING',
    city: 'STRING',
    gender: 'STRING',
    has_newsletter: 'BOOLEAN',
  },
  fields: {
    name: { kind: 'random', length: 12 },
    street: { kind: 'random', length: 20 },
    zip_code: { kind: 'random', length: 5 },
    city: { kind: 'random', length: 10 },
    gender: { kind: 'random', length: 1 },
  },
}`
}

// ── Transaction ──────────────────────────────────────────────────────
// Apache Flink: TemporalJoinSQLExample
// Java POJO: Transaction(id, currencyCode, amount, trxTime: TIMESTAMP_LTZ)

export function transactionSchemaFile(): TemplateFile {
  return {
    path: "schemas/transaction.ts",
    content: `${HEADER}

export default Schema({
  fields: {
    id: Field.BIGINT(),
    currencyCode: Field.STRING(),
    amount: Field.DOUBLE(),
    trxTime: Field.TIMESTAMP_LTZ(3),
  },
  watermark: { column: 'trxTime', expression: "trxTime - INTERVAL '5' SECOND" },
});
`,
  }
}

export function transactionSimTable(opts: SimTableOpts = {}): string {
  const table = opts.table ?? "transactions"
  const topic = opts.topic ?? "transactions"
  const format = opts.format ?? "json"
  const rowsPerSecond = opts.rowsPerSecond ?? 10
  return `{
  table: '${table}',
  topic: '${topic}',
  format: '${format}',
  rowsPerSecond: ${rowsPerSecond},
  columns: {
    id: 'BIGINT',
    currencyCode: 'STRING',
    amount: 'DOUBLE',
    trxTime: 'TIMESTAMP_LTZ(3)',
  },
  watermark: { column: 'trxTime', expression: "trxTime - INTERVAL '5' SECOND" },
  fields: {
    id: { kind: 'sequence', start: 1, end: 1000000 },
    currencyCode: { kind: 'random', length: 3 },
    amount: { kind: 'random', min: 1, max: 10000 },
  },
}`
}

// ── CurrencyRate ─────────────────────────────────────────────────────
// Apache Flink: TemporalJoinSQLExample (versioned table)

export function currencyRateSchemaFile(): TemplateFile {
  return {
    path: "schemas/currency-rate.ts",
    content: `${HEADER}

export default Schema({
  fields: {
    currencyCode: Field.STRING(),
    euroRate: Field.DOUBLE(),
    rateTime: Field.TIMESTAMP_LTZ(3),
  },
  primaryKey: { columns: ['currencyCode'] },
  watermark: { column: 'rateTime', expression: "rateTime - INTERVAL '5' SECOND" },
});
`,
  }
}

export function currencyRateSimTable(opts: SimTableOpts = {}): string {
  const table = opts.table ?? "currency_rates"
  const topic = opts.topic ?? "currency-rates"
  const format = opts.format ?? "debezium-json"
  const rowsPerSecond = opts.rowsPerSecond ?? 1
  return `{
  table: '${table}',
  topic: '${topic}',
  format: '${format}',
  rowsPerSecond: ${rowsPerSecond},
  columns: {
    currencyCode: 'STRING',
    euroRate: 'DOUBLE',
    rateTime: 'TIMESTAMP_LTZ(3)',
  },
  primaryKey: ['currencyCode'],
  watermark: { column: 'rateTime', expression: "rateTime - INTERVAL '5' SECOND" },
  fields: {
    currencyCode: { kind: 'random', length: 3 },
    euroRate: { kind: 'random', min: 0, max: 2 },
  },
}`
}

// ── PopulationUpdates ────────────────────────────────────────────────
// Apache Flink: UpdatingTopCityExample

export function populationUpdatesSchemaFile(): TemplateFile {
  return {
    path: "schemas/population-updates.ts",
    content: `${HEADER}

export default Schema({
  fields: {
    city: Field.STRING(),
    state: Field.STRING(),
    updateYear: Field.INT(),
    populationDiff: Field.BIGINT(),
  },
});
`,
  }
}

export function populationUpdatesSimTable(opts: SimTableOpts = {}): string {
  const table = opts.table ?? "population_updates"
  const topic = opts.topic ?? "population-updates"
  const format = opts.format ?? "json"
  const rowsPerSecond = opts.rowsPerSecond ?? 5
  return `{
  table: '${table}',
  topic: '${topic}',
  format: '${format}',
  rowsPerSecond: ${rowsPerSecond},
  columns: {
    city: 'STRING',
    state: 'STRING',
    updateYear: 'INT',
    populationDiff: 'BIGINT',
  },
  fields: {
    city: { kind: 'random', length: 10 },
    state: { kind: 'random', length: 2 },
    updateYear: { kind: 'random', min: 2000, max: 2025 },
    populationDiff: { kind: 'random', min: -10000, max: 50000 },
  },
}`
}

// ── NameGrade / NameSalary ───────────────────────────────────────────
// Apache Flink: WindowJoin (DataStream)

export function nameGradeSchemaFile(): TemplateFile {
  return {
    path: "schemas/name-grade.ts",
    content: `${HEADER}

export default Schema({
  fields: {
    name: Field.STRING(),
    grade: Field.INT(),
    time: Field.TIMESTAMP_LTZ(3),
  },
  watermark: { column: 'time', expression: "time - INTERVAL '5' SECOND" },
});
`,
  }
}

export function nameGradeSimTable(opts: SimTableOpts = {}): string {
  const table = opts.table ?? "name_grades"
  const topic = opts.topic ?? "name-grades"
  const format = opts.format ?? "json"
  const rowsPerSecond = opts.rowsPerSecond ?? 10
  return `{
  table: '${table}',
  topic: '${topic}',
  format: '${format}',
  rowsPerSecond: ${rowsPerSecond},
  columns: {
    name: 'STRING',
    grade: 'INT',
    time: 'TIMESTAMP_LTZ(3)',
  },
  watermark: { column: 'time', expression: "time - INTERVAL '5' SECOND" },
  fields: {
    name: { kind: 'random', length: 8 },
    grade: { kind: 'random', min: 0, max: 100 },
  },
}`
}

export function nameSalarySchemaFile(): TemplateFile {
  return {
    path: "schemas/name-salary.ts",
    content: `${HEADER}

export default Schema({
  fields: {
    name: Field.STRING(),
    salary: Field.INT(),
    time: Field.TIMESTAMP_LTZ(3),
  },
  watermark: { column: 'time', expression: "time - INTERVAL '5' SECOND" },
});
`,
  }
}

export function nameSalarySimTable(opts: SimTableOpts = {}): string {
  const table = opts.table ?? "name_salaries"
  const topic = opts.topic ?? "name-salaries"
  const format = opts.format ?? "json"
  const rowsPerSecond = opts.rowsPerSecond ?? 10
  return `{
  table: '${table}',
  topic: '${topic}',
  format: '${format}',
  rowsPerSecond: ${rowsPerSecond},
  columns: {
    name: 'STRING',
    salary: 'INT',
    time: 'TIMESTAMP_LTZ(3)',
  },
  watermark: { column: 'time', expression: "time - INTERVAL '5' SECOND" },
  fields: {
    name: { kind: 'random', length: 8 },
    salary: { kind: 'random', min: 30000, max: 200000 },
  },
}`
}

// ── UserScore ────────────────────────────────────────────────────────
// Apache Flink: Join (DSv2)

export function userScoreSchemaFile(): TemplateFile {
  return {
    path: "schemas/user-score.ts",
    content: `${HEADER}

export default Schema({
  fields: {
    name: Field.STRING(),
    score: Field.INT(),
    ts: Field.TIMESTAMP_LTZ(3),
  },
  watermark: { column: 'ts', expression: "ts - INTERVAL '5' SECOND" },
});
`,
  }
}

export function userScoreSimTable(opts: SimTableOpts = {}): string {
  const table = opts.table ?? "user_scores"
  const topic = opts.topic ?? "user-scores"
  const format = opts.format ?? "json"
  const rowsPerSecond = opts.rowsPerSecond ?? 10
  return `{
  table: '${table}',
  topic: '${topic}',
  format: '${format}',
  rowsPerSecond: ${rowsPerSecond},
  columns: {
    name: 'STRING',
    score: 'INT',
    ts: 'TIMESTAMP_LTZ(3)',
  },
  watermark: { column: 'ts', expression: "ts - INTERVAL '5' SECOND" },
  fields: {
    name: { kind: 'random', length: 8 },
    score: { kind: 'random', min: 0, max: 1000 },
  },
}`
}

// ── Event / Alert ────────────────────────────────────────────────────
// Apache Flink: StateMachineExample
// Returns BOTH event and alert TemplateFiles via a single helper
// (the two are always used together in StateMachineExample).

export interface EventAlertFiles {
  readonly event: TemplateFile
  readonly alert: TemplateFile
}

export function eventAlertSchemaFile(): EventAlertFiles {
  return {
    event: {
      path: "schemas/event.ts",
      content: `${HEADER}

export default Schema({
  fields: {
    sourceAddress: Field.STRING(),
    type: Field.INT(),
    timestamp: Field.TIMESTAMP_LTZ(3),
  },
  watermark: { column: 'timestamp', expression: "timestamp - INTERVAL '5' SECOND" },
});
`,
    },
    alert: {
      path: "schemas/alert.ts",
      content: `${HEADER}

export default Schema({
  fields: {
    sourceAddress: Field.STRING(),
    state: Field.STRING(),
    transition: Field.INT(),
    timestamp: Field.TIMESTAMP_LTZ(3),
  },
});
`,
    },
  }
}

export function eventSimTable(opts: SimTableOpts = {}): string {
  const table = opts.table ?? "events"
  const topic = opts.topic ?? "events"
  const format = opts.format ?? "json"
  const rowsPerSecond = opts.rowsPerSecond ?? 100
  return `{
  table: '${table}',
  topic: '${topic}',
  format: '${format}',
  rowsPerSecond: ${rowsPerSecond},
  columns: {
    sourceAddress: 'STRING',
    type: 'INT',
    timestamp: 'TIMESTAMP_LTZ(3)',
  },
  watermark: { column: 'timestamp', expression: "timestamp - INTERVAL '5' SECOND" },
  fields: {
    sourceAddress: { kind: 'random', length: 12 },
    type: { kind: 'random', min: 0, max: 5 },
  },
}`
}

export function alertSimTable(opts: SimTableOpts = {}): string {
  const table = opts.table ?? "alerts"
  const topic = opts.topic ?? "alerts"
  const format = opts.format ?? "json"
  const rowsPerSecond = opts.rowsPerSecond ?? 0
  return `{
  table: '${table}',
  topic: '${topic}',
  format: '${format}',
  rowsPerSecond: ${rowsPerSecond},
  columns: {
    sourceAddress: 'STRING',
    state: 'STRING',
    transition: 'INT',
    timestamp: 'TIMESTAMP_LTZ(3)',
  },
}`
}

// ── ProductSales ─────────────────────────────────────────────────────
// Apache Flink: CountProductSalesWindowing (DSv2)

export function productSalesSchemaFile(): TemplateFile {
  return {
    path: "schemas/product-sales.ts",
    content: `${HEADER}

export default Schema({
  fields: {
    productId: Field.BIGINT(),
    timestamp: Field.BIGINT(),
  },
});
`,
  }
}

export function productSalesSimTable(opts: SimTableOpts = {}): string {
  const table = opts.table ?? "product_sales"
  const topic = opts.topic ?? "product-sales"
  const format = opts.format ?? "json"
  const rowsPerSecond = opts.rowsPerSecond ?? 50
  return `{
  table: '${table}',
  topic: '${topic}',
  format: '${format}',
  rowsPerSecond: ${rowsPerSecond},
  columns: {
    productId: 'BIGINT',
    timestamp: 'BIGINT',
  },
  fields: {
    productId: { kind: 'random', min: 1, max: 100 },
    timestamp: { kind: 'sequence', start: 1, end: 1000000 },
  },
}`
}

// ── KeyedEvent ───────────────────────────────────────────────────────
// Apache Flink: SessionWindowing

export function keyedEventSchemaFile(): TemplateFile {
  return {
    path: "schemas/keyed-event.ts",
    content: `${HEADER}

export default Schema({
  fields: {
    id: Field.STRING(),
    ts: Field.BIGINT(),
    value: Field.INT(),
  },
});
`,
  }
}

export function keyedEventSimTable(opts: SimTableOpts = {}): string {
  const table = opts.table ?? "keyed_events"
  const topic = opts.topic ?? "keyed-events"
  const format = opts.format ?? "json"
  const rowsPerSecond = opts.rowsPerSecond ?? 20
  return `{
  table: '${table}',
  topic: '${topic}',
  format: '${format}',
  rowsPerSecond: ${rowsPerSecond},
  columns: {
    id: 'STRING',
    ts: 'BIGINT',
    value: 'INT',
  },
  fields: {
    id: { kind: 'random', length: 6 },
    ts: { kind: 'sequence', start: 1, end: 1000000 },
    value: { kind: 'random', min: 0, max: 100 },
  },
}`
}
