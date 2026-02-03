import type { BaseComponentProps, ConstructNode } from '../core/types.js';
import { createElement } from '../core/jsx-runtime.js';

// ── Shared sink types ───────────────────────────────────────────────

export type SinkFormat = 'json' | 'avro' | 'csv';

export type FileFormat = 'parquet' | 'orc' | 'csv' | 'json';

export interface RollingPolicy {
  readonly size?: string;
  readonly interval?: string;
}

// ── KafkaSink ───────────────────────────────────────────────────────

export interface KafkaSinkProps extends BaseComponentProps {
  readonly topic: string;
  readonly format?: SinkFormat;
  readonly bootstrapServers?: string;
  readonly children?: ConstructNode | ConstructNode[];
}

/**
 * Kafka sink: writes to an Apache Kafka topic.
 *
 * Format defaults to 'json'. bootstrapServers falls back to
 * pipeline-level config if not specified here.
 */
export function KafkaSink(props: KafkaSinkProps): ConstructNode {
  const { children, ...rest } = props;
  const childArray = children == null
    ? []
    : Array.isArray(children)
      ? children
      : [children];

  return createElement('KafkaSink', { ...rest }, ...childArray);
}

// ── JdbcSink ────────────────────────────────────────────────────────

export interface JdbcSinkProps extends BaseComponentProps {
  readonly url: string;
  readonly table: string;
  readonly upsertMode?: boolean;
  readonly keyFields?: readonly string[];
  readonly children?: ConstructNode | ConstructNode[];
}

/**
 * JDBC sink: writes to a relational database via JDBC.
 *
 * When `upsertMode` is true, `keyFields` must be provided to
 * identify the primary key columns for upsert semantics.
 */
export function JdbcSink(props: JdbcSinkProps): ConstructNode {
  const { children, ...rest } = props;
  const childArray = children == null
    ? []
    : Array.isArray(children)
      ? children
      : [children];

  return createElement('JdbcSink', { ...rest }, ...childArray);
}

// ── FileSystemSink ──────────────────────────────────────────────────

export interface FileSystemSinkProps extends BaseComponentProps {
  readonly path: string;
  readonly format?: FileFormat;
  readonly partitionBy?: readonly string[];
  readonly rollingPolicy?: RollingPolicy;
  readonly children?: ConstructNode | ConstructNode[];
}

/**
 * FileSystem sink: writes to a file system path (S3, HDFS, local).
 *
 * Supports partitioned output and configurable rolling policies
 * for file rotation.
 */
export function FileSystemSink(props: FileSystemSinkProps): ConstructNode {
  const { children, ...rest } = props;
  const childArray = children == null
    ? []
    : Array.isArray(children)
      ? children
      : [children];

  return createElement('FileSystemSink', { ...rest }, ...childArray);
}

// ── GenericSink ─────────────────────────────────────────────────────

export interface GenericSinkProps extends BaseComponentProps {
  readonly connector: string;
  readonly options?: Record<string, string>;
  readonly children?: ConstructNode | ConstructNode[];
}

/**
 * Generic sink: escape hatch for any Flink SQL sink connector
 * not covered by the specialized sink components.
 *
 * The `connector` identifier and all `options` are passed through
 * to the WITH clause during code generation.
 */
export function GenericSink(props: GenericSinkProps): ConstructNode {
  const { children, ...rest } = props;
  const childArray = children == null
    ? []
    : Array.isArray(children)
      ? children
      : [children];

  return createElement('GenericSink', { ...rest }, ...childArray);
}
