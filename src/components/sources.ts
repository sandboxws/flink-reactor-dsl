import type { FlinkType, ChangelogMode, BaseComponentProps, ConstructNode, TapConfig } from '../core/types.js';
import type { SchemaDefinition, WatermarkDeclaration } from '../core/schema.js';
import { createElement, toSqlIdentifier } from '../core/jsx-runtime.js';

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
  /** Optional SQL table name. Defaults to topic name normalized as a SQL identifier. */
  readonly name?: string;
  readonly topic: string;
  readonly bootstrapServers?: string;
  readonly format?: KafkaFormat;
  readonly schema: SchemaDefinition<T>;
  readonly watermark?: WatermarkDeclaration;
  readonly startupMode?: KafkaStartupMode;
  readonly consumerGroup?: string;
  readonly primaryKey?: readonly string[];
  /** Enable operator tailing for this source */
  readonly tap?: boolean | TapConfig;
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
  const { children, name, ...rest } = props;
  const childArray = children == null
    ? []
    : Array.isArray(children)
      ? children
      : [children];

  const changelogMode = inferChangelogMode(props.format);
  const _nameHint = name ?? toSqlIdentifier(props.topic);

  return createElement('KafkaSource', { ...rest, changelogMode, _nameHint }, ...childArray);
}

// ── JdbcSource ──────────────────────────────────────────────────────

export interface JdbcSourceProps<T extends Record<string, FlinkType> = Record<string, FlinkType>>
  extends BaseComponentProps {
  /** Optional SQL table name. Defaults to the JDBC table name. */
  readonly name?: string;
  readonly url: string;
  readonly table: string;
  readonly schema: SchemaDefinition<T>;
  readonly lookupCache?: LookupCacheConfig;
  /** Enable operator tailing for this source */
  readonly tap?: boolean | TapConfig;
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
  const { children, name, ...rest } = props;
  const childArray = children == null
    ? []
    : Array.isArray(children)
      ? children
      : [children];

  const _nameHint = name ?? toSqlIdentifier(props.table);

  return createElement('JdbcSource', { ...rest, _nameHint }, ...childArray);
}

// ── GenericSource ───────────────────────────────────────────────────

export interface GenericSourceProps<T extends Record<string, FlinkType> = Record<string, FlinkType>>
  extends BaseComponentProps {
  /** Optional SQL table name. Defaults to the connector name. */
  readonly name?: string;
  readonly connector: string;
  readonly format?: string;
  readonly schema: SchemaDefinition<T>;
  readonly options?: Record<string, string>;
  /** Enable operator tailing for this source */
  readonly tap?: boolean | TapConfig;
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
  const { children, name, ...rest } = props;
  const childArray = children == null
    ? []
    : Array.isArray(children)
      ? children
      : [children];

  const _nameHint = name ?? toSqlIdentifier(props.connector);

  return createElement('GenericSource', { ...rest, _nameHint }, ...childArray);
}
