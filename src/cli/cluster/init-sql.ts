import type {
  JdbcCatalogDefinition,
  KafkaCatalogDefinition,
  KafkaTableDefinition,
  SimInitConfig,
} from "@/core/config.js"

// ── Context ─────────────────────────────────────────────────────────

export interface InitSqlContext {
  /** Kafka bootstrap servers (e.g. 'kafka:9092') */
  kafkaBootstrapServers: string
  /** PostgreSQL host (e.g. 'postgres' for docker, 'postgres.flink-demo.svc' for k8s) */
  postgresHost: string
  /** PostgreSQL port (default: 5432) */
  postgresPort: number
  /** PostgreSQL username (default: 'reactor') */
  postgresUser: string
  /** PostgreSQL password (default: 'reactor') */
  postgresPassword: string
}

export interface InitSqlOptions {
  /** Include built-in JDBC catalogs for pagila/chinook/employees/flink_sink */
  includeBuiltinJdbc?: boolean
}

// ── Result types ────────────────────────────────────────────────────

export interface InitSqlResult {
  /** DDL statements (CREATE CATALOG, CREATE TABLE) — idempotent, safe to replay */
  ddl: string[]
  /** DataGen seeding statements (CREATE TEMPORARY TABLE + INSERT INTO) — streaming jobs */
  seeding: string[]
}

// ── Built-in JDBC catalogs ──────────────────────────────────────────

function builtinJdbcCatalogs(ctx: InitSqlContext): JdbcCatalogDefinition[] {
  const baseUrl = `jdbc:postgresql://${ctx.postgresHost}:${ctx.postgresPort}/`
  return [
    {
      name: "pagila",
      baseUrl,
      defaultDatabase: "pagila",
      username: ctx.postgresUser,
      password: ctx.postgresPassword,
    },
    {
      name: "chinook",
      baseUrl,
      defaultDatabase: "chinook",
      username: ctx.postgresUser,
      password: ctx.postgresPassword,
    },
    {
      name: "employees",
      baseUrl,
      defaultDatabase: "employees",
      username: ctx.postgresUser,
      password: ctx.postgresPassword,
    },
    {
      name: "flink_sink",
      baseUrl,
      defaultDatabase: "flink_sink",
      username: ctx.postgresUser,
      password: ctx.postgresPassword,
    },
  ]
}

// ── SQL generation ──────────────────────────────────────────────────

function generateJdbcCatalogSql(catalog: JdbcCatalogDefinition): string {
  const username = catalog.username ?? "reactor"
  const password = catalog.password ?? "reactor"
  const defaultDb = catalog.defaultDatabase ?? catalog.name

  return [
    `CREATE CATALOG \`${catalog.name}\` WITH (`,
    `  'type' = 'jdbc',`,
    `  'base-url' = '${catalog.baseUrl}',`,
    `  'default-database' = '${defaultDb}',`,
    `  'username' = '${username}',`,
    `  'password' = '${password}'`,
    `);`,
  ].join("\n")
}

function generateKafkaTableSql(
  table: KafkaTableDefinition,
  ctx: InitSqlContext,
): string {
  const format = table.format ?? "json"
  const startupMode = table.scanStartupMode ?? "earliest-offset"

  // Column definitions
  const columns = Object.entries(table.columns).map(
    ([name, type]) => `  \`${name}\` ${type}`,
  )

  // Primary key constraint
  if (table.primaryKey && table.primaryKey.length > 0) {
    const pkCols = table.primaryKey.map((c) => `\`${c}\``).join(", ")
    columns.push(`  PRIMARY KEY (${pkCols}) NOT ENFORCED`)
  }

  // Watermark
  if (table.watermark) {
    columns.push(
      `  WATERMARK FOR \`${table.watermark.column}\` AS ${table.watermark.expression}`,
    )
  }

  const withProps = [
    `  'connector' = 'kafka'`,
    `  'topic' = '${table.topic}'`,
    `  'properties.bootstrap.servers' = '${ctx.kafkaBootstrapServers}'`,
    `  'scan.startup.mode' = '${startupMode}'`,
    `  'format' = '${format}'`,
  ]

  return [
    `CREATE TABLE IF NOT EXISTS \`${table.table}\` (`,
    columns.join(",\n"),
    `) WITH (`,
    withProps.join(",\n"),
    `);`,
  ].join("\n")
}

function generateKafkaCatalogSql(
  catalog: KafkaCatalogDefinition,
  ctx: InitSqlContext,
): string[] {
  const statements: string[] = []

  statements.push(
    `CREATE CATALOG \`${catalog.name}\` WITH ('type' = 'generic_in_memory');`,
  )
  statements.push(`USE CATALOG \`${catalog.name}\`;`)

  for (const table of catalog.tables) {
    statements.push(generateKafkaTableSql(table, ctx))
  }

  statements.push("USE CATALOG `default_catalog`;")

  return statements
}

function generateDataGenSeedingSql(
  catalog: KafkaCatalogDefinition,
  _ctx: InitSqlContext,
): string[] {
  const statements: string[] = []

  for (const table of catalog.tables) {
    const rowsPerSecond = table.rowsPerSecond ?? 10
    if (rowsPerSecond <= 0) continue

    // Only include non-computed columns for DataGen (skip watermarks, metadata)
    const genColumns = Object.entries(table.columns).map(
      ([name, type]) => `  \`${name}\` ${type}`,
    )

    const tempName = `_datagen_${catalog.name}_${table.table}`

    statements.push(
      [
        `CREATE TEMPORARY TABLE \`${tempName}\` (`,
        genColumns.join(",\n"),
        `) WITH (`,
        `  'connector' = 'datagen',`,
        `  'rows-per-second' = '${rowsPerSecond}'`,
        `);`,
      ].join("\n"),
    )

    statements.push(
      `INSERT INTO \`${catalog.name}\`.\`default\`.\`${table.table}\` SELECT * FROM \`${tempName}\`;`,
    )
  }

  return statements
}

// ── Public API ──────────────────────────────────────────────────────

/**
 * Generate init SQL from a SimInitConfig.
 *
 * Returns DDL statements (idempotent, safe to replay) and DataGen seeding
 * statements (streaming INSERT jobs).
 */
export function generateInitSql(
  config: SimInitConfig | undefined,
  ctx: InitSqlContext,
  options?: InitSqlOptions,
): InitSqlResult {
  const ddl: string[] = []
  const seeding: string[] = []

  // Built-in JDBC catalogs (always for cluster command)
  if (options?.includeBuiltinJdbc) {
    for (const catalog of builtinJdbcCatalogs(ctx)) {
      ddl.push(generateJdbcCatalogSql(catalog))
    }
  }

  if (!config) {
    return { ddl, seeding }
  }

  // User-defined JDBC catalogs
  if (config.jdbc?.catalogs) {
    for (const catalog of config.jdbc.catalogs) {
      ddl.push(generateJdbcCatalogSql(catalog))
    }
  }

  // Per-domain Kafka catalogs
  if (config.kafka?.catalogs) {
    for (const catalog of config.kafka.catalogs) {
      ddl.push(...generateKafkaCatalogSql(catalog, ctx))

      // DataGen seeding for each catalog's tables
      seeding.push(...generateDataGenSeedingSql(catalog, ctx))
    }
  }

  return { ddl, seeding }
}
