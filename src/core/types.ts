// ── Flink SQL type system ────────────────────────────────────────────

/** Flink SQL primitive types */
export type FlinkPrimitiveType =
  | "BOOLEAN"
  | "TINYINT"
  | "SMALLINT"
  | "INT"
  | "BIGINT"
  | "FLOAT"
  | "DOUBLE"
  | "STRING"
  | "DATE"
  | "TIME"
  | "BYTES"

/** Flink SQL parameterized types (expressed as string literals) */
export type FlinkParameterizedType =
  | `DECIMAL(${number}, ${number})`
  | `TIMESTAMP(${number})`
  | `TIMESTAMP_LTZ(${number})`
  | `VARCHAR(${number})`
  | `CHAR(${number})`
  | `BINARY(${number})`
  | `VARBINARY(${number})`

/** Flink SQL composite types */
export type FlinkCompositeType =
  | `ARRAY<${string}>`
  | `MAP<${string}, ${string}>`
  | `ROW<${string}>`

/** Union of all recognized Flink SQL types */
export type FlinkType =
  | FlinkPrimitiveType
  | FlinkParameterizedType
  | FlinkCompositeType

// ── Changelog mode ───────────────────────────────────────────────────

export type ChangelogMode = "append-only" | "retract" | "upsert"

// ── Flink version ────────────────────────────────────────────────────

export type FlinkMajorVersion = "1.20" | "2.0" | "2.1" | "2.2"

// ── Schema ───────────────────────────────────────────────────────────

/** A schema is a record mapping field names to Flink SQL types */
export type FlinkSchema<
  T extends Record<string, FlinkType> = Record<string, FlinkType>,
> = T

// ── Stream (branded type) ────────────────────────────────────────────

declare const streamBrand: unique symbol

/**
 * A branded type representing a typed data stream in the pipeline.
 * T is a record of field names to Flink SQL types.
 */
export type Stream<
  T extends Record<string, FlinkType> = Record<string, FlinkType>,
> = {
  readonly [streamBrand]: true
  readonly _tag: "Stream"
  readonly _schema: FlinkSchema<T>
  readonly _nodeId: string
  readonly _changelogMode: ChangelogMode
}

/** Create a Stream value (used internally by component factories) */
export function createStream<T extends Record<string, FlinkType>>(
  nodeId: string,
  schema: FlinkSchema<T>,
  changelogMode: ChangelogMode = "append-only",
): Stream<T> {
  return {
    _tag: "Stream",
    _schema: schema,
    _nodeId: nodeId,
    _changelogMode: changelogMode,
  } as Stream<T>
}

// ── Base component props ─────────────────────────────────────────────

/** Base props shared by all pipeline components */
export interface BaseComponentProps {
  readonly parallelism?: number
}

// ── Construct tree node types ────────────────────────────────────────

export type NodeKind =
  | "Pipeline"
  | "Source"
  | "Sink"
  | "Transform"
  | "Join"
  | "Window"
  | "Catalog"
  | "RawSQL"
  | "UDF"
  | "CEP"
  | "View"
  | "MaterializedTable"

export interface ConstructNode {
  readonly id: string
  readonly kind: NodeKind
  readonly component: string
  readonly props: Record<string, unknown>
  readonly children: ConstructNode[]
}

/**
 * Branded ConstructNode carrying a phantom component tag.
 *
 * Sub-component factories (Route.Branch, Query.Select, etc.) return this
 * type so that parent components can constrain their `children` prop at
 * compile time. The `__componentBrand` field never exists at runtime —
 * it's a phantom type used purely for type-level discrimination.
 *
 * `TypedConstructNode<C>` is assignable TO `ConstructNode` (extends it),
 * but `ConstructNode` is NOT assignable TO `TypedConstructNode<C>`.
 */
export interface TypedConstructNode<C extends string = string>
  extends ConstructNode {
  readonly __componentBrand: C
}

// ── Tap types ─────────────────────────────────────────────────────

/** Offset mode for tap observation queries */
export type TapOffsetMode = "latest" | "earliest" | "timestamp"

/** Configuration for an operator tap point */
export interface TapConfig {
  /** Display name for the tap point (defaults to component type + node ID) */
  readonly name?: string
  /** Consumer group ID prefix (auto-generated if omitted) */
  readonly groupIdPrefix?: string
  /** Where to start reading: 'latest' (default), 'earliest', or 'timestamp' */
  readonly offsetMode?: TapOffsetMode
  /** ISO timestamp for 'timestamp' offset mode */
  readonly startTimestamp?: string
  /** Optional end timestamp to bound the observation window */
  readonly endTimestamp?: string
}

/** Metadata describing how to observe a tapped operator's output stream */
export interface TapMetadata {
  /** Unique node ID from the construct tree */
  readonly nodeId: string
  /** Human-readable name (from TapConfig.name or auto-generated) */
  readonly name: string
  /** Component type: 'source' | 'sink' | 'transform' | 'join' | 'window' */
  readonly componentType: string
  /** Component name (e.g., 'KafkaSource', 'Filter', 'JdbcSink') */
  readonly componentName: string
  /** Output schema of the tapped operator */
  readonly schema: Record<string, string>
  /** Connector type used for observation */
  readonly connectorType: string
  /** Full SQL for the observation query (CREATE TEMPORARY TABLE + SELECT) */
  readonly observationSql: string
  /** Default consumer group ID */
  readonly consumerGroupId: string
  /** Tap configuration (merged defaults + user overrides) */
  readonly config: Required<TapConfig>
  /** Source connector properties needed to reconstruct the observation table */
  readonly connectorProperties: Record<string, string>
}

/** Manifest file emitted alongside synthesized SQL for all tap points */
export interface TapManifest {
  /** Pipeline name from Pipeline component */
  readonly pipelineName: string
  /** Target Flink version */
  readonly flinkVersion: string
  /** Timestamp when manifest was generated */
  readonly generatedAt: string
  /** All tapped operators */
  readonly taps: TapMetadata[]
}
