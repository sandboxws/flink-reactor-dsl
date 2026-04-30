// ── Live Postgres schema introspection ──────────────────────────────
// Connects to a running Postgres server and reads column metadata for a
// batched set of tables in ONE `information_schema.columns` query. The
// result is cached for the lifetime of the process so subsequent calls
// with the same connection + table set don't re-hit the server.
//
// The `pg` driver is a lazy dynamic import so the library entry doesn't
// force a Postgres client on every consumer. It is declared as an
// `optionalDependencies` entry in package.json.

import { mapPgTypeToFlink } from "@/codegen/connectors/postgres-types.js"
import type { IntrospectedColumn } from "@/codegen/schema-introspect.js"

// Re-export so existing CLI/test imports keep working without churn.
// The mapping table itself lives in @/codegen/connectors/postgres-types.
export { mapPgTypeToFlink }

// ── Options / types ─────────────────────────────────────────────────

export interface IntrospectPostgresOptions {
  readonly connectionString: string
  readonly schemaList: readonly string[]
  /** Table names WITHOUT the schema prefix (e.g. `orders`, not `public.orders`). */
  readonly tableList: readonly string[]
}

/**
 * Keyed by `fullyQualified = schema.table`. Each value is the column list
 * in ordinal-position order.
 */
export type IntrospectResult = ReadonlyMap<
  string,
  readonly IntrospectedColumn[]
>

// ── Lazy pg driver loader ───────────────────────────────────────────

interface PgClient {
  connect(): Promise<void>
  query(
    text: string,
    values?: unknown[],
  ): Promise<{ rows: Record<string, unknown>[] }>
  end(): Promise<void>
}

interface PgClientCtor {
  new (config: { connectionString: string }): PgClient
}

async function loadPg(): Promise<PgClientCtor> {
  try {
    // `pg` is an optional peer dep — not bundled with the library. We load
    // it dynamically with a string expression so TypeScript's module
    // resolution doesn't require the types to be installed for downstream
    // consumers of the library.
    const modulePath = "pg"
    // biome-ignore lint/suspicious/noExplicitAny: optional driver, dynamic import
    const mod = (await import(modulePath)) as any
    return mod.default?.Client ?? mod.Client
  } catch (err) {
    throw new Error(
      "The `pg` package is required for live Postgres introspection. Install it with `npm install pg` or `pnpm add pg`.\n" +
        `Underlying error: ${(err as Error).message}`,
    )
  }
}

// ── Cache ───────────────────────────────────────────────────────────

const cache = new Map<string, IntrospectResult>()

function cacheKey(opts: IntrospectPostgresOptions): string {
  const schemas = [...opts.schemaList].sort().join("|")
  const tables = [...opts.tableList].sort().join("|")
  return `${opts.connectionString}::${schemas}::${tables}`
}

/** Reset the in-memory cache. Primarily for tests. */
export function resetIntrospectCache(): void {
  cache.clear()
}

// ── Public API ──────────────────────────────────────────────────────

/**
 * Introspect Postgres columns for every `(schema, table)` combination
 * implied by `schemaList` × `tableList`. Tables are matched by their
 * bare name (no schema prefix). Results are cached so repeat calls with
 * the same inputs do not re-hit the server.
 *
 * Makes exactly ONE query per non-cached invocation — batched via
 * `WHERE table_schema = ANY($1) AND table_name = ANY($2)`.
 */
export async function introspectPostgresTables(
  opts: IntrospectPostgresOptions,
): Promise<IntrospectResult> {
  const key = cacheKey(opts)
  const cached = cache.get(key)
  if (cached) return cached

  const Client = await loadPg()
  const client = new Client({ connectionString: opts.connectionString })
  await client.connect()

  try {
    const result = await client.query(
      `SELECT
         table_schema,
         table_name,
         column_name,
         data_type,
         udt_name,
         character_maximum_length,
         numeric_precision,
         numeric_scale,
         ordinal_position,
         is_nullable
       FROM information_schema.columns
       WHERE table_schema = ANY($1) AND table_name = ANY($2)
       ORDER BY table_schema, table_name, ordinal_position`,
      [opts.schemaList, opts.tableList],
    )

    // Primary key query (second query — also batched — so PK columns get
    // the PK constraint annotation).
    const pkResult = await client.query(
      `SELECT
         tc.table_schema,
         tc.table_name,
         kcu.column_name
       FROM information_schema.table_constraints tc
       JOIN information_schema.key_column_usage kcu
         ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
       WHERE tc.constraint_type = 'PRIMARY KEY'
         AND tc.table_schema = ANY($1)
         AND tc.table_name   = ANY($2)
       ORDER BY tc.table_schema, tc.table_name, kcu.ordinal_position`,
      [opts.schemaList, opts.tableList],
    )

    const byTable = new Map<string, IntrospectedColumn[]>()
    const pkByTable = new Map<string, Set<string>>()

    for (const row of pkResult.rows) {
      const fq = `${row.table_schema}.${row.table_name}`
      let set = pkByTable.get(fq)
      if (!set) {
        set = new Set()
        pkByTable.set(fq, set)
      }
      set.add(row.column_name as string)
    }

    for (const row of result.rows) {
      const fq = `${row.table_schema}.${row.table_name}`
      const columnName = row.column_name as string
      const col: IntrospectedColumn = {
        name: columnName,
        type: mapPgTypeToFlink(
          row.data_type as string,
          row.udt_name as string | undefined,
          row.character_maximum_length as number | null | undefined,
          row.numeric_precision as number | null | undefined,
          row.numeric_scale as number | null | undefined,
        ),
        constraints: pkByTable.get(fq)?.has(columnName) ? ["PK"] : [],
      }
      let cols = byTable.get(fq)
      if (!cols) {
        cols = []
        byTable.set(fq, cols)
      }
      cols.push(col)
    }

    cache.set(key, byTable)
    return byTable
  } finally {
    await client.end()
  }
}

/**
 * Helper: split a `schema.table` string into `{ schema, table }`.
 * When the value has no dot, it falls back to `'public'` as the schema.
 */
export function splitQualifiedTable(qualified: string): {
  schema: string
  table: string
} {
  const idx = qualified.indexOf(".")
  if (idx < 0) return { schema: "public", table: qualified }
  return {
    schema: qualified.slice(0, idx),
    table: qualified.slice(idx + 1),
  }
}
