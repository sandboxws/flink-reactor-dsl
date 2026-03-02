import type { FlinkPrimitiveType, FlinkType } from "./types.js"

// ── Flink type validation ────────────────────────────────────────────

const PRIMITIVE_TYPES: ReadonlySet<string> = new Set<FlinkPrimitiveType>([
  "BOOLEAN",
  "TINYINT",
  "SMALLINT",
  "INT",
  "BIGINT",
  "FLOAT",
  "DOUBLE",
  "STRING",
  "DATE",
  "TIME",
  "BYTES",
])

const PARAMETERIZED_PATTERNS: readonly RegExp[] = [
  /^DECIMAL\(\d+,\s*\d+\)$/,
  /^TIMESTAMP\(\d+\)$/,
  /^TIMESTAMP_LTZ\(\d+\)$/,
  /^VARCHAR\(\d+\)$/,
  /^CHAR\(\d+\)$/,
  /^BINARY\(\d+\)$/,
  /^VARBINARY\(\d+\)$/,
]

const COMPOSITE_PATTERNS: readonly RegExp[] = [
  /^ARRAY<.+>$/,
  /^MAP<.+,\s*.+>$/,
  /^ROW<.+>$/,
]

export function isValidFlinkType(type: string): type is FlinkType {
  if (PRIMITIVE_TYPES.has(type)) return true
  if (PARAMETERIZED_PATTERNS.some((p) => p.test(type))) return true
  if (COMPOSITE_PATTERNS.some((p) => p.test(type))) return true
  return false
}

// ── Watermark declaration ────────────────────────────────────────────

export interface WatermarkDeclaration {
  readonly column: string
  readonly expression: string
}

// ── Primary key declaration ──────────────────────────────────────────

export interface PrimaryKeyDeclaration {
  readonly columns: readonly string[]
}

// ── Metadata column declaration ──────────────────────────────────────

export interface MetadataColumnDeclaration {
  readonly column: string
  readonly type: FlinkType
  readonly from?: string
  readonly isVirtual: boolean
}

// ── Schema definition ────────────────────────────────────────────────

export interface SchemaDefinition<
  T extends Record<string, FlinkType> = Record<string, FlinkType>,
> {
  readonly fields: T
  readonly watermark?: WatermarkDeclaration
  readonly primaryKey?: PrimaryKeyDeclaration
  readonly metadataColumns: readonly MetadataColumnDeclaration[]
}

// ── Schema builder options ───────────────────────────────────────────

export interface SchemaOptions<T extends Record<string, FlinkType>> {
  readonly fields: T
  readonly watermark?: WatermarkDeclaration
  readonly primaryKey?: PrimaryKeyDeclaration
  readonly metadataColumns?: readonly MetadataColumnDeclaration[]
}

/**
 * Build a validated schema definition.
 * Throws if any field type is not a recognized Flink SQL type.
 */
export function Schema<T extends Record<string, FlinkType>>(
  options: SchemaOptions<T>,
): SchemaDefinition<T> {
  for (const [name, type] of Object.entries(options.fields)) {
    if (!isValidFlinkType(type)) {
      throw new Error(`Invalid Flink SQL type '${type}' for field '${name}'`)
    }
  }

  if (options.watermark) {
    const { column } = options.watermark
    if (!(column in options.fields)) {
      throw new Error(`Watermark column '${column}' is not a declared field`)
    }
  }

  if (options.primaryKey) {
    for (const col of options.primaryKey.columns) {
      if (!(col in options.fields)) {
        throw new Error(`Primary key column '${col}' is not a declared field`)
      }
    }
  }

  return {
    fields: options.fields,
    watermark: options.watermark,
    primaryKey: options.primaryKey,
    metadataColumns: options.metadataColumns ?? [],
  }
}

// ── Field type builders ──────────────────────────────────────────────

export const Field = {
  BOOLEAN(): FlinkType {
    return "BOOLEAN"
  },
  TINYINT(): FlinkType {
    return "TINYINT"
  },
  SMALLINT(): FlinkType {
    return "SMALLINT"
  },
  INT(): FlinkType {
    return "INT"
  },
  BIGINT(): FlinkType {
    return "BIGINT"
  },
  FLOAT(): FlinkType {
    return "FLOAT"
  },
  DOUBLE(): FlinkType {
    return "DOUBLE"
  },
  STRING(): FlinkType {
    return "STRING"
  },
  DATE(): FlinkType {
    return "DATE"
  },
  TIME(): FlinkType {
    return "TIME"
  },
  BYTES(): FlinkType {
    return "BYTES"
  },

  DECIMAL(precision: number, scale: number): FlinkType {
    return `DECIMAL(${precision}, ${scale})` as FlinkType
  },
  TIMESTAMP(precision: number = 3): FlinkType {
    return `TIMESTAMP(${precision})` as FlinkType
  },
  TIMESTAMP_LTZ(precision: number = 3): FlinkType {
    return `TIMESTAMP_LTZ(${precision})` as FlinkType
  },
  VARCHAR(length: number): FlinkType {
    return `VARCHAR(${length})` as FlinkType
  },
  CHAR(length: number): FlinkType {
    return `CHAR(${length})` as FlinkType
  },
  BINARY(length: number): FlinkType {
    return `BINARY(${length})` as FlinkType
  },
  VARBINARY(length: number): FlinkType {
    return `VARBINARY(${length})` as FlinkType
  },

  ARRAY(elementType: FlinkType): FlinkType {
    return `ARRAY<${elementType}>` as FlinkType
  },
  MAP(keyType: FlinkType, valueType: FlinkType): FlinkType {
    return `MAP<${keyType}, ${valueType}>` as FlinkType
  },
  ROW(fields: string): FlinkType {
    return `ROW<${fields}>` as FlinkType
  },
} as const
