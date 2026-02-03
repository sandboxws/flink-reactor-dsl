import type { FlinkType, ChangelogMode, BaseComponentProps, ConstructNode } from '../core/types.js';
import type { SchemaDefinition, WatermarkDeclaration } from '../core/schema.js';
import { createElement } from '../core/jsx-runtime.js';

// ── CDC format → ChangelogMode inference ────────────────────────────

const CDC_FORMATS: ReadonlySet<KafkaFormat> = new Set([
  'debezium-json',
  'canal-json',
  'maxwell-json',
]);

export function inferChangelogMode(format: KafkaFormat | undefined): ChangelogMode {
  return CDC_FORMATS.has(format ?? 'json') ? 'retract' : 'append-only';
}

// ── Shared source types ─────────────────────────────────────────────

export type KafkaFormat =
  | 'json'
  | 'avro'
  | 'csv'
  | 'debezium-json'
  | 'canal-json'
  | 'maxwell-json';

export type KafkaStartupMode =
  | 'latest-offset'
  | 'earliest-offset'
  | 'group-offsets'
  | 'timestamp';

export interface LookupCacheConfig {
  readonly maxRows: number;
  readonly ttl: string;
}

// ── KafkaSource ─────────────────────────────────────────────────────

export interface KafkaSourceProps<T extends Record<string, FlinkType> = Record<string, FlinkType>>
  extends BaseComponentProps {
  readonly topic: string;
  readonly bootstrapServers?: string;
  readonly format?: KafkaFormat;
  readonly schema: SchemaDefinition<T>;
  readonly watermark?: WatermarkDeclaration;
  readonly startupMode?: KafkaStartupMode;
  readonly consumerGroup?: string;
  readonly primaryKey?: readonly string[];
  readonly children?: ConstructNode | ConstructNode[];
}

/**
 * Kafka source: reads from an Apache Kafka topic.
 *
 * Format defaults to 'json'. bootstrapServers falls back to
 * pipeline-level config if not specified here.
 *
 * When a CDC format (debezium-json, canal-json, maxwell-json) is used,
 * the resulting stream carries ChangelogMode 'retract'. Non-CDC formats
 * produce an 'append-only' stream.
 */
export function KafkaSource<T extends Record<string, FlinkType>>(
  props: KafkaSourceProps<T>,
): ConstructNode {
  const { children, ...rest } = props;
  const childArray = children == null
    ? []
    : Array.isArray(children)
      ? children
      : [children];

  const changelogMode = inferChangelogMode(props.format);

  return createElement('KafkaSource', { ...rest, changelogMode }, ...childArray);
}

// ── JdbcSource ──────────────────────────────────────────────────────

export interface JdbcSourceProps<T extends Record<string, FlinkType> = Record<string, FlinkType>>
  extends BaseComponentProps {
  readonly url: string;
  readonly table: string;
  readonly schema: SchemaDefinition<T>;
  readonly lookupCache?: LookupCacheConfig;
  readonly children?: ConstructNode | ConstructNode[];
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
  const { children, ...rest } = props;
  const childArray = children == null
    ? []
    : Array.isArray(children)
      ? children
      : [children];

  return createElement('JdbcSource', { ...rest }, ...childArray);
}

// ── GenericSource ───────────────────────────────────────────────────

export interface GenericSourceProps<T extends Record<string, FlinkType> = Record<string, FlinkType>>
  extends BaseComponentProps {
  readonly connector: string;
  readonly format?: string;
  readonly schema: SchemaDefinition<T>;
  readonly options?: Record<string, string>;
  readonly children?: ConstructNode | ConstructNode[];
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
  const { children, ...rest } = props;
  const childArray = children == null
    ? []
    : Array.isArray(children)
      ? children
      : [children];

  return createElement('GenericSource', { ...rest }, ...childArray);
}
