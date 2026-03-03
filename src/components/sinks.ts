import { createElement, toSqlIdentifier } from "@/core/jsx-runtime.js"
import type {
  BaseComponentProps,
  ConstructNode,
  TapConfig,
} from "@/core/types.js"
import type { CatalogHandle } from "./catalogs.js"

// ── Shared sink types ───────────────────────────────────────────────

export type SinkFormat = "json" | "avro" | "csv"

export type FileFormat = "parquet" | "orc" | "csv" | "json"

export interface RollingPolicy {
  readonly size?: string
  readonly interval?: string
}

// ── KafkaSink ───────────────────────────────────────────────────────

export interface KafkaSinkProps extends BaseComponentProps {
  /** Optional SQL table name. Defaults to topic name normalized as a SQL identifier. */
  readonly name?: string
  readonly topic: string
  readonly format?: SinkFormat
  readonly bootstrapServers?: string
  /** Enable operator tailing for this sink */
  readonly tap?: boolean | TapConfig
  readonly children?: ConstructNode | ConstructNode[]
}

/**
 * Kafka sink: writes to an Apache Kafka topic.
 *
 * Format defaults to 'json'. bootstrapServers falls back to
 * pipeline-level config if not specified here.
 */
export function KafkaSink(props: KafkaSinkProps): ConstructNode {
  const { children, name, ...rest } = props
  const childArray =
    children == null ? [] : Array.isArray(children) ? children : [children]

  const _nameHint = name ?? toSqlIdentifier(props.topic)

  return createElement("KafkaSink", { ...rest, _nameHint }, ...childArray)
}

// ── JdbcSink ────────────────────────────────────────────────────────

export interface JdbcSinkProps extends BaseComponentProps {
  /** Optional SQL table name. Defaults to the JDBC table name. */
  readonly name?: string
  readonly url: string
  readonly table: string
  readonly upsertMode?: boolean
  readonly keyFields?: readonly string[]
  /** Enable operator tailing for this sink */
  readonly tap?: boolean | TapConfig
  readonly children?: ConstructNode | ConstructNode[]
}

/**
 * JDBC sink: writes to a relational database via JDBC.
 *
 * When `upsertMode` is true, `keyFields` must be provided to
 * identify the primary key columns for upsert semantics.
 */
export function JdbcSink(props: JdbcSinkProps): ConstructNode {
  const { children, name, ...rest } = props
  const childArray =
    children == null ? [] : Array.isArray(children) ? children : [children]

  const _nameHint = name ?? toSqlIdentifier(props.table)

  return createElement("JdbcSink", { ...rest, _nameHint }, ...childArray)
}

// ── FileSystemSink ──────────────────────────────────────────────────

export interface FileSystemSinkProps extends BaseComponentProps {
  /** Optional SQL table name. Defaults to the last path segment. */
  readonly name?: string
  readonly path: string
  readonly format?: FileFormat
  readonly partitionBy?: readonly string[]
  readonly rollingPolicy?: RollingPolicy
  /** Enable operator tailing for this sink */
  readonly tap?: boolean | TapConfig
  readonly children?: ConstructNode | ConstructNode[]
}

/**
 * FileSystem sink: writes to a file system path (S3, HDFS, local).
 *
 * Supports partitioned output and configurable rolling policies
 * for file rotation.
 */
export function FileSystemSink(props: FileSystemSinkProps): ConstructNode {
  const { children, name, ...rest } = props
  const childArray =
    children == null ? [] : Array.isArray(children) ? children : [children]

  // Derive name from last path segment (e.g., "s3://bucket/output" → "output")
  const pathSegments = props.path.replace(/\/+$/, "").split("/")
  const _nameHint =
    name ?? toSqlIdentifier(pathSegments[pathSegments.length - 1])

  return createElement("FileSystemSink", { ...rest, _nameHint }, ...childArray)
}

// ── GenericSink ─────────────────────────────────────────────────────

export interface GenericSinkProps extends BaseComponentProps {
  /** Optional SQL table name. Defaults to the connector name. */
  readonly name?: string
  readonly connector: string
  readonly options?: Record<string, string>
  /** Enable operator tailing for this sink */
  readonly tap?: boolean | TapConfig
  readonly children?: ConstructNode | ConstructNode[]
}

/**
 * Generic sink: escape hatch for any Flink SQL sink connector
 * not covered by the specialized sink components.
 *
 * The `connector` identifier and all `options` are passed through
 * to the WITH clause during code generation.
 */
export function GenericSink(props: GenericSinkProps): ConstructNode {
  const { children, name, ...rest } = props
  const childArray =
    children == null ? [] : Array.isArray(children) ? children : [children]

  const _nameHint = name ?? toSqlIdentifier(props.connector)

  return createElement("GenericSink", { ...rest, _nameHint }, ...childArray)
}

// ── PaimonSink ─────────────────────────────────────────────────────

export type PaimonMergeEngine = "deduplicate" | "partial-update" | "aggregation"
export type PaimonChangelogProducer = "input" | "lookup" | "full-compaction"

export interface PaimonSinkProps extends BaseComponentProps {
  readonly catalog: CatalogHandle
  readonly database: string
  readonly table: string
  readonly primaryKey?: readonly string[]
  readonly mergeEngine?: PaimonMergeEngine
  readonly changelogProducer?: PaimonChangelogProducer
  readonly sequenceField?: string
  /** Enable operator tailing for this sink */
  readonly tap?: boolean | TapConfig
  readonly children?: ConstructNode | ConstructNode[]
}

/**
 * Paimon sink: writes to an Apache Paimon lakehouse table.
 *
 * References a PaimonCatalog handle to form catalog-qualified table names.
 * Supports merge engines for deduplication, partial updates, and aggregation.
 * `changelogProducer` controls how the table generates changelog for
 * downstream consumers.
 */
export function PaimonSink(props: PaimonSinkProps): ConstructNode {
  const { children, catalog, ...rest } = props
  const childArray =
    children == null ? [] : Array.isArray(children) ? children : [children]

  return createElement(
    "PaimonSink",
    {
      ...rest,
      catalogName: catalog.catalogName,
      catalogNodeId: catalog.nodeId,
    },
    ...childArray,
  )
}

// ── IcebergSink ────────────────────────────────────────────────────

export interface IcebergSinkProps extends BaseComponentProps {
  readonly catalog: CatalogHandle
  readonly database: string
  readonly table: string
  readonly primaryKey?: readonly string[]
  readonly formatVersion?: 1 | 2
  readonly upsertEnabled?: boolean
  /** Enable operator tailing for this sink */
  readonly tap?: boolean | TapConfig
  readonly children?: ConstructNode | ConstructNode[]
}

/**
 * Iceberg sink: writes to an Apache Iceberg table.
 *
 * References an IcebergCatalog handle to form catalog-qualified table names.
 * `formatVersion` 2 is required for row-level deletes (upsert support).
 * When `upsertEnabled` is true, the sink accepts retract/upsert streams.
 */
export function IcebergSink(props: IcebergSinkProps): ConstructNode {
  const { children, catalog, ...rest } = props
  const childArray =
    children == null ? [] : Array.isArray(children) ? children : [children]

  return createElement(
    "IcebergSink",
    {
      ...rest,
      catalogName: catalog.catalogName,
      catalogNodeId: catalog.nodeId,
    },
    ...childArray,
  )
}
