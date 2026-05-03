import {
  createElement,
  requireProps,
  toSqlIdentifier,
} from "@/core/jsx-runtime.js"
import type { SchemaDefinition, WatermarkDeclaration } from "@/core/schema.js"
import type { SecretRef } from "@/core/secret-ref.js"
import type {
  BaseComponentProps,
  ChangelogMode,
  ConstructNode,
  FlinkType,
  TapConfig,
} from "@/core/types.js"
import type { CatalogHandle } from "./catalogs.js"

// ── CDC format → ChangelogMode inference ────────────────────────────

const CDC_FORMATS: ReadonlySet<KafkaFormat> = new Set([
  "debezium-json",
  "debezium-avro",
  "debezium-protobuf",
  "canal-json",
  "maxwell-json",
])

export function inferChangelogMode(
  format: KafkaFormat | undefined,
): ChangelogMode {
  return CDC_FORMATS.has(format ?? "json") ? "retract" : "append-only"
}

// ── Shared source types ─────────────────────────────────────────────

export type KafkaFormat =
  | "json"
  | "avro"
  | "csv"
  | "debezium-json"
  | "debezium-avro"
  | "debezium-protobuf"
  | "canal-json"
  | "maxwell-json"

export type KafkaStartupMode =
  | "latest-offset"
  | "earliest-offset"
  | "group-offsets"
  | "timestamp"

export interface LookupCacheConfig {
  readonly maxRows: number
  readonly ttl: string
}

// ── KafkaSource ─────────────────────────────────────────────────────

export interface KafkaSourceProps<
  T extends Record<string, FlinkType> = Record<string, FlinkType>,
> extends BaseComponentProps {
  /** Optional SQL table name. Defaults to topic name normalized as a SQL identifier. */
  readonly name?: string
  readonly topic: string
  readonly bootstrapServers?: string
  readonly format?: KafkaFormat
  readonly schema: SchemaDefinition<T>
  readonly watermark?: WatermarkDeclaration
  readonly startupMode?: KafkaStartupMode
  readonly consumerGroup?: string
  readonly primaryKey?: readonly string[]
  /**
   * Confluent Schema Registry URL. Required when `format` is `"debezium-avro"`
   * or `"debezium-protobuf"` — the Flink deserializer fetches the writer schema
   * from this endpoint at runtime. The deployment template must expose a
   * reachable Schema Registry (in-cluster sidecar or external endpoint); see
   * the Confluent `flink-sql-avro-confluent-registry` /
   * `flink-sql-protobuf-confluent-registry` format docs for the request shape.
   */
  readonly schemaRegistryUrl?: string
  /** Enable operator tailing for this source */
  readonly tap?: boolean | TapConfig
  readonly children?: ConstructNode | ConstructNode[]
}

/**
 * Kafka source: reads from an Apache Kafka topic.
 *
 * Format defaults to 'json'. bootstrapServers falls back to
 * pipeline-level config if not specified here.
 *
 * When a CDC format (debezium-json, debezium-avro, debezium-protobuf,
 * canal-json, maxwell-json) is used, the resulting stream carries
 * ChangelogMode 'retract'. Non-CDC formats produce an 'append-only' stream.
 *
 * Registry-backed CDC formats (`debezium-avro`, `debezium-protobuf`) require
 * a Confluent Schema Registry endpoint — pass `schemaRegistryUrl` and ensure
 * the FlinkDeployment template reaches a reachable Schema Registry service.
 */
export function KafkaSource<T extends Record<string, FlinkType>>(
  props: KafkaSourceProps<T>,
): ConstructNode {
  requireProps("KafkaSource", props, ["topic", "schema"])
  const { children, name, ...rest } = props
  const childArray =
    children == null ? [] : Array.isArray(children) ? children : [children]

  const changelogMode = inferChangelogMode(props.format)
  const _nameHint = name ?? toSqlIdentifier(props.topic)

  return createElement(
    "KafkaSource",
    { ...rest, changelogMode, _nameHint },
    ...childArray,
  )
}

// ── JdbcSource ──────────────────────────────────────────────────────

export interface JdbcSourceProps<
  T extends Record<string, FlinkType> = Record<string, FlinkType>,
> extends BaseComponentProps {
  /** Optional SQL table name. Defaults to the JDBC table name. */
  readonly name?: string
  readonly url: string
  readonly table: string
  readonly schema: SchemaDefinition<T>
  readonly lookupCache?: LookupCacheConfig
  /** Enable operator tailing for this source */
  readonly tap?: boolean | TapConfig
  readonly children?: ConstructNode | ConstructNode[]
}

/**
 * JDBC source: reads from a relational database via JDBC.
 *
 * When `lookupCache` is provided, the source is intended for use
 * as a dimension table in lookup joins.
 */
export function JdbcSource<T extends Record<string, FlinkType>>(
  props: JdbcSourceProps<T>,
): ConstructNode {
  requireProps("JdbcSource", props, ["url", "table", "schema"])
  const { children, name, ...rest } = props
  const childArray =
    children == null ? [] : Array.isArray(children) ? children : [children]

  const _nameHint = name ?? toSqlIdentifier(props.table)

  return createElement("JdbcSource", { ...rest, _nameHint }, ...childArray)
}

// ── GenericSource ───────────────────────────────────────────────────

export interface GenericSourceProps<
  T extends Record<string, FlinkType> = Record<string, FlinkType>,
> extends BaseComponentProps {
  /** Optional SQL table name. Defaults to the connector name. */
  readonly name?: string
  readonly connector: string
  readonly format?: string
  readonly schema: SchemaDefinition<T>
  readonly options?: Record<string, string>
  /** Enable operator tailing for this source */
  readonly tap?: boolean | TapConfig
  readonly children?: ConstructNode | ConstructNode[]
}

/**
 * Generic source: escape hatch for any Flink SQL connector
 * not covered by the specialized source components.
 *
 * The `connector` identifier and all `options` are passed through
 * to the WITH clause during code generation.
 */
export function GenericSource<T extends Record<string, FlinkType>>(
  props: GenericSourceProps<T>,
): ConstructNode {
  requireProps("GenericSource", props, ["connector", "schema"])
  const { children, name, ...rest } = props
  const childArray =
    children == null ? [] : Array.isArray(children) ? children : [children]

  const _nameHint = name ?? toSqlIdentifier(props.connector)

  return createElement("GenericSource", { ...rest, _nameHint }, ...childArray)
}

// ── PostgresCdcPipelineSource ───────────────────────────────────────

/**
 * Initial-snapshot strategy for `PostgresCdcPipelineSource`.
 *
 * - `'initial'`      — full snapshot, then stream from captured LSN (default).
 * - `'never'`        — skip snapshot, stream from the current WAL position.
 * - `'initial_only'` — run the snapshot, then exit (useful for one-shot
 *                      backfills — the companion FlinkDeployment becomes a
 *                      batch-style bounded job).
 */
export type PostgresCdcSnapshotMode = "initial" | "never" | "initial_only"

/**
 * Streaming-startup offset for `PostgresCdcPipelineSource`.
 */
export type PostgresCdcStartupMode =
  | "initial"
  | "latest-offset"
  | "specific-offset"
  | "timestamp"

export interface PostgresCdcPipelineSourceProps<
  T extends Record<string, FlinkType> = Record<string, FlinkType>,
> extends BaseComponentProps {
  /** Optional SQL-identifier name. Defaults to the pipeline name when omitted. */
  readonly name?: string
  readonly hostname: string
  /** Defaults to `5432`. */
  readonly port?: number
  readonly database: string
  readonly username: string
  /**
   * Secret reference for the Postgres password. The cleartext value never
   * leaves the user-managed Kubernetes Secret — the DSL emits a
   * `${env:<envName>}` placeholder in the pipeline YAML and an individual
   * `env` entry in the FlinkDeployment podTemplate.
   *
   * A plain string here is rejected at compile time. Use `secretRef(...)`.
   */
  readonly password: SecretRef
  readonly schemaList: readonly string[]
  readonly tableList: readonly string[]
  /** Optional — when omitted, a deterministic name is derived from the pipeline. */
  readonly replicationSlotName?: string
  /** Optional — when omitted, a deterministic name is derived from the pipeline. */
  readonly publicationName?: string
  /** Defaults to `'initial'`. */
  readonly snapshotMode?: PostgresCdcSnapshotMode
  /** Defaults to `'initial'`. */
  readonly startupMode?: PostgresCdcStartupMode
  /** Logical-decoding plugin. Defaults to `'pgoutput'` (Postgres 10+). */
  readonly decodingPluginName?: string
  /** Postgres heartbeat interval, milliseconds. */
  readonly heartbeatIntervalMs?: number
  /** Drop the replication slot on job stop. Defaults to `false`. */
  readonly slotDropOnStop?: boolean
  /** Row-chunk size for the incremental snapshot fan-out. */
  readonly chunkSize?: number
  /**
   * Optional static schema. When absent, downstream tooling that requires
   * columns (e.g. `schema --live`) must introspect the Postgres server
   * directly via `information_schema.columns`.
   */
  readonly schema?: SchemaDefinition<T>
  /** Explicit primary-key hint. Overrides the PK inferred from `tableList`. */
  readonly primaryKey?: readonly string[]
  /**
   * Not supported for Pipeline Connector sources yet — passing `true` here
   * will raise a synthesis-time diagnostic. Kept on the type so the error
   * surfaces clearly instead of being silently ignored.
   */
  readonly tap?: boolean | TapConfig
  readonly children?: ConstructNode | ConstructNode[]
}

/**
 * Flink CDC 3.6 **Pipeline Connector** Postgres source.
 *
 * Unlike the Kafka-hop topology (`KafkaSource({ format: 'debezium-json' })`),
 * a Pipeline Connector source runs replication-slot / publication-based CDC
 * directly inside the Flink job — no external Debezium Connect cluster, no
 * per-record JSON serialization, and end-to-end schema evolution handled by
 * the CDC runtime. The synthesis engine emits a `pipeline.yaml` artifact
 * (instead of Flink SQL DDL) and a `FlinkDeployment` CRD that launches the
 * `flink-cdc-cli.jar` entrypoint with the YAML mounted as a ConfigMap.
 *
 * The resulting `Stream` carries ChangelogMode `'retract'` unconditionally
 * — Pipeline-Connector sources are always CDC, so downstream sinks must be
 * upsert-capable (e.g. `IcebergSink` with `formatVersion: 2` + `upsertEnabled`,
 * or `PaimonSink` with a `primaryKey`). The synthesis engine rejects
 * non-upsert sinks with a diagnostic naming both endpoints.
 *
 * ## Replication slot ownership
 *
 * If `replicationSlotName` is supplied, it is referenced verbatim and the
 * user owns the slot's creation / deletion. If omitted, a deterministic name
 * is derived from the pipeline name so that re-synthesizing the same pipeline
 * resumes from the same slot. `slotDropOnStop: true` drops the slot when the
 * job terminates — safe for one-shot `snapshotMode: 'initial_only'` jobs,
 * but *dangerous* for long-running streaming jobs because it forfeits the
 * low-water-mark that subsequent runs need to rejoin the WAL.
 *
 * ## Publication lifecycle
 *
 * `publicationName` behaves the same way as the slot name — bring your own
 * or the DSL derives one. The publication itself is always user-managed
 * (the DSL never executes `CREATE PUBLICATION`). Make sure the publication
 * covers every table named in `tableList`, or CDC will silently drop rows
 * from unpublished tables.
 *
 * ## Version pin
 *
 * The connector is pinned to `flink-cdc-pipeline-connector-postgres:3.6.0`
 * across Flink 1.20 and 2.x (Flink CDC pipeline connectors version
 * independently of Flink core).
 */
export function PostgresCdcPipelineSource<T extends Record<string, FlinkType>>(
  props: PostgresCdcPipelineSourceProps<T>,
): ConstructNode {
  requireProps("PostgresCdcPipelineSource", props, [
    "hostname",
    "database",
    "username",
    "password",
    "schemaList",
    "tableList",
  ])
  const { children, name, ...rest } = props
  const childArray =
    children == null ? [] : Array.isArray(children) ? children : [children]

  const changelogMode: ChangelogMode = "retract"
  const _nameHint = name ?? toSqlIdentifier(props.database)

  return createElement(
    "PostgresCdcPipelineSource",
    { ...rest, changelogMode, _nameHint },
    ...childArray,
  )
}

// ── DataGenSource ──────────────────────────────────────────────────

export interface DataGenSourceProps<
  T extends Record<string, FlinkType> = Record<string, FlinkType>,
> extends BaseComponentProps {
  /** Optional SQL table name. Defaults to "datagen". */
  readonly name?: string
  readonly schema: SchemaDefinition<T>
  /** Rows emitted per second (maps to 'rows-per-second'). */
  readonly rowsPerSecond?: number
  /** Number of fields generated per second. */
  readonly fieldsPerSecond?: number
  /** Total number of rows to generate (unbounded if omitted). */
  readonly numberOfRows?: number
  /** Enable operator tailing for this source */
  readonly tap?: boolean | TapConfig
  readonly children?: ConstructNode | ConstructNode[]
}

/**
 * DataGen source: Flink's built-in datagen connector for synthetic data.
 *
 * No external JAR required — the datagen connector ships with Flink.
 * Wraps GenericSource with `connector: "datagen"` and named props
 * for `rowsPerSecond`, `fieldsPerSecond`, and `numberOfRows`.
 */
export function DataGenSource<T extends Record<string, FlinkType>>(
  props: DataGenSourceProps<T>,
): ConstructNode {
  requireProps("DataGenSource", props, ["schema"])
  const {
    children,
    name,
    rowsPerSecond,
    fieldsPerSecond,
    numberOfRows,
    ...rest
  } = props
  const childArray =
    children == null ? [] : Array.isArray(children) ? children : [children]

  const options: Record<string, string> = {}
  if (rowsPerSecond != null) options["rows-per-second"] = String(rowsPerSecond)
  if (fieldsPerSecond != null)
    options["fields-per-second"] = String(fieldsPerSecond)
  if (numberOfRows != null) options["number-of-rows"] = String(numberOfRows)

  const _nameHint = name ?? "datagen"

  return createElement(
    "DataGenSource",
    { ...rest, connector: "datagen", options, _nameHint },
    ...childArray,
  )
}

// ── FlussSource ─────────────────────────────────────────────────────

/**
 * Streaming-startup offset for `FlussSource`.
 *
 * - `'initial'`   — read the table snapshot then continue from the captured
 *                   log offset (default; matches Fluss CLI default).
 * - `'earliest'`  — start from the earliest log offset, skipping snapshot.
 * - `'latest'`    — start from the latest log offset, ignoring history.
 * - `'timestamp'` — seek to a specific Fluss log timestamp; requires
 *                   `scanStartupTimestampMs`.
 */
export type FlussScanStartupMode =
  | "initial"
  | "earliest"
  | "latest"
  | "timestamp"

export interface FlussSourceProps<
  T extends Record<string, FlinkType> = Record<string, FlinkType>,
> extends BaseComponentProps {
  /** Optional SQL-identifier name. Defaults to `<database>_<table>` when omitted. */
  readonly name?: string
  /** Handle to a `FlussCatalog` declared earlier in the construct tree. */
  readonly catalog: CatalogHandle
  readonly database: string
  readonly table: string
  readonly schema: SchemaDefinition<T>
  readonly watermark?: WatermarkDeclaration
  /** Defaults to `'initial'`. */
  readonly scanStartupMode?: FlussScanStartupMode
  /**
   * Required when `scanStartupMode === 'timestamp'`. Forbidden in any other
   * mode — a synth-time diagnostic rejects a stale value carried over from a
   * previous configuration.
   */
  readonly scanStartupTimestampMs?: number
  /**
   * When set, identifies the upstream Fluss table as a PrimaryKey table and
   * the resulting stream carries ChangelogMode `'retract'`. Omitting it reads
   * a Log table (append-only). Setting `primaryKey` against an upstream Log
   * table fails at planner time inside Flink — the choice must match the
   * way the upstream table was originally created.
   */
  readonly primaryKey?: readonly string[]
  readonly parallelism?: number
  /** Not supported — passing `tap: true` raises a synthesis-time diagnostic. */
  readonly tap?: boolean | TapConfig
  readonly children?: ConstructNode | ConstructNode[]
}

/**
 * Apache Fluss streaming source.
 *
 * Reads a Fluss table over the Fluss Flink connector. Pair with a
 * `FlussCatalog` for connection wiring. Two table flavors:
 *
 *   • **PrimaryKey table** — set `primaryKey`. The source emits a CDC-style
 *     retract/upsert stream and downstream upsert sinks (e.g. `PaimonSink`
 *     with `mergeEngine`, `IcebergSink` with `upsertEnabled`) accept it.
 *   • **Log table** — omit `primaryKey`. The source emits an append-only
 *     stream; retract-requiring sinks reject it at synth time.
 *
 * Set `primaryKey` only when the upstream table was created as a PrimaryKey
 * table — wrong choice on a Log table fails at planner time inside Flink.
 *
 * `scanStartupMode` controls the offset Fluss seeks to before streaming:
 *
 *   • `'initial'`   — snapshot then log (default).
 *   • `'earliest'`  — entire log from the first available offset.
 *   • `'latest'`    — log only, starting at the current tip.
 *   • `'timestamp'` — seek to `scanStartupTimestampMs` (required in this mode).
 *
 * Tapping is not supported; pass-through `tap: true` raises a diagnostic.
 *
 * See https://fluss.apache.org/ for the underlying Apache Fluss connector
 * (project moved out of Alibaba into Apache Incubator in June 2025; the
 * canonical Maven coordinate is `org.apache.fluss:fluss-flink-<flink-major>`,
 * e.g. `fluss-flink-1.20` or `fluss-flink-2.2`) and the FlinkReactor docs
 * Fluss-source operator page (forthcoming) for
 * the worked Stage-B serve-side pattern this component is built around.
 *
 * @see https://flink-reactor.dev/docs/templates/pg-fluss-paimon/serve — worked
 *      walkthrough of `FlussSource` reading a PrimaryKey table inside the
 *      `pg-fluss-paimon` template's serve pipeline.
 */
export function FlussSource<T extends Record<string, FlinkType>>(
  props: FlussSourceProps<T>,
): ConstructNode {
  requireProps("FlussSource", props, ["catalog", "database", "table", "schema"])
  if (
    props.scanStartupMode === "timestamp" &&
    props.scanStartupTimestampMs == null
  ) {
    throw new Error(
      "FlussSource: `scanStartupTimestampMs` is required when `scanStartupMode === 'timestamp'`",
    )
  }

  const { children, name, catalog, ...rest } = props
  const childArray =
    children == null ? [] : Array.isArray(children) ? children : [children]

  const hasPk = Array.isArray(props.primaryKey) && props.primaryKey.length > 0
  const changelogMode: ChangelogMode = hasPk ? "retract" : "append-only"
  const _nameHint = name ?? toSqlIdentifier(`${props.database}_${props.table}`)

  return createElement(
    "FlussSource",
    {
      ...rest,
      catalogName: catalog.catalogName,
      catalogNodeId: catalog.nodeId,
      changelogMode,
      _nameHint,
    },
    ...childArray,
  )
}
