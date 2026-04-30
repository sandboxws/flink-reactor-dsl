// ── Postgres → Flink SQL type mapping ───────────────────────────────
//
// Pure mapping table. Lives in `src/codegen/` (not `src/cli/`) so the
// browser bundle, LSP, and future console use cases can reach it
// without pulling in the CLI's `pg` driver dependency. The CLI
// introspector imports this and applies it to rows it queries from
// `information_schema.columns`.

/**
 * Best-effort PG → Flink SQL type mapping. Unknown types fall back to
 * STRING; callers may log a warning if that is noisy for their use case.
 */
export function mapPgTypeToFlink(
  dataType: string,
  udtName?: string,
  charMaxLength?: number | null,
  numericPrecision?: number | null,
  numericScale?: number | null,
): string {
  const t = (udtName ?? dataType).toLowerCase()
  switch (t) {
    case "int2":
    case "smallint":
      return "SMALLINT"
    case "int4":
    case "integer":
      return "INT"
    case "int8":
    case "bigint":
      return "BIGINT"
    case "float4":
    case "real":
      return "FLOAT"
    case "float8":
    case "double precision":
      return "DOUBLE"
    case "numeric":
    case "decimal":
      if (numericPrecision && numericScale != null) {
        return `DECIMAL(${numericPrecision}, ${numericScale})`
      }
      return "DECIMAL(38, 18)"
    case "bool":
    case "boolean":
      return "BOOLEAN"
    case "date":
      return "DATE"
    case "time":
    case "timetz":
      return "TIME"
    case "timestamp":
    case "timestamp without time zone":
      return "TIMESTAMP(6)"
    case "timestamptz":
    case "timestamp with time zone":
      return "TIMESTAMP_LTZ(6)"
    case "bytea":
      return "BYTES"
    case "varchar":
    case "character varying":
      return charMaxLength ? `VARCHAR(${charMaxLength})` : "STRING"
    case "char":
    case "bpchar":
    case "character":
      return charMaxLength ? `CHAR(${charMaxLength})` : "STRING"
    case "text":
    case "uuid":
    case "json":
    case "jsonb":
    case "xml":
      return "STRING"
    default:
      return "STRING"
  }
}
