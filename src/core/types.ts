// ── Flink SQL type system ────────────────────────────────────────────

/** Flink SQL primitive types */
export type FlinkPrimitiveType =
  | 'BOOLEAN'
  | 'TINYINT'
  | 'SMALLINT'
  | 'INT'
  | 'BIGINT'
  | 'FLOAT'
  | 'DOUBLE'
  | 'STRING'
  | 'DATE'
  | 'TIME'
  | 'BYTES';

/** Flink SQL parameterized types (expressed as string literals) */
export type FlinkParameterizedType =
  | `DECIMAL(${number}, ${number})`
  | `TIMESTAMP(${number})`
  | `TIMESTAMP_LTZ(${number})`
  | `VARCHAR(${number})`
  | `CHAR(${number})`
  | `BINARY(${number})`
  | `VARBINARY(${number})`;

/** Flink SQL composite types */
export type FlinkCompositeType =
  | `ARRAY<${string}>`
  | `MAP<${string}, ${string}>`
  | `ROW<${string}>`;

/** Union of all recognized Flink SQL types */
export type FlinkType =
  | FlinkPrimitiveType
  | FlinkParameterizedType
  | FlinkCompositeType;

// ── Changelog mode ───────────────────────────────────────────────────

export type ChangelogMode = 'append-only' | 'retract' | 'upsert';

// ── Flink version ────────────────────────────────────────────────────

export type FlinkMajorVersion = '1.20' | '2.0' | '2.1' | '2.2';

// ── Schema ───────────────────────────────────────────────────────────

/** A schema is a record mapping field names to Flink SQL types */
export type FlinkSchema<T extends Record<string, FlinkType> = Record<string, FlinkType>> = T;

// ── Stream (branded type) ────────────────────────────────────────────

declare const streamBrand: unique symbol;

/**
 * A branded type representing a typed data stream in the pipeline.
 * T is a record of field names to Flink SQL types.
 */
export type Stream<T extends Record<string, FlinkType> = Record<string, FlinkType>> = {
  readonly [streamBrand]: true;
  readonly _tag: 'Stream';
  readonly _schema: FlinkSchema<T>;
  readonly _nodeId: string;
  readonly _changelogMode: ChangelogMode;
};

/** Create a Stream value (used internally by component factories) */
export function createStream<T extends Record<string, FlinkType>>(
  nodeId: string,
  schema: FlinkSchema<T>,
  changelogMode: ChangelogMode = 'append-only',
): Stream<T> {
  return {
    _tag: 'Stream',
    _schema: schema,
    _nodeId: nodeId,
    _changelogMode: changelogMode,
  } as Stream<T>;
}

// ── Base component props ─────────────────────────────────────────────

/** Base props shared by all pipeline components */
export interface BaseComponentProps {
  readonly parallelism?: number;
}

// ── Construct tree node types ────────────────────────────────────────

export type NodeKind =
  | 'Pipeline'
  | 'Source'
  | 'Sink'
  | 'Transform'
  | 'Join'
  | 'Window'
  | 'Catalog'
  | 'RawSQL'
  | 'UDF'
  | 'CEP'
  | 'View';

export interface ConstructNode {
  readonly id: string;
  readonly kind: NodeKind;
  readonly component: string;
  readonly props: Record<string, unknown>;
  readonly children: ConstructNode[];
}
