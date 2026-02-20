import type {
  ConstructNode,
  TapConfig,
  TapMetadata,
  TapOffsetMode,
} from './types.js';
import type { ValidationDiagnostic } from './synth-context.js';

// ── Observation strategy ─────────────────────────────────────────────

export type ObservationStrategy =
  | 'consumer-group-clone'
  | 'periodic-poll'
  | 'incremental-read'
  | 'direct-read'
  | 'unsupported';

/** Map a connector type string to its observation strategy */
export function resolveObservationStrategy(connectorType: string): ObservationStrategy {
  switch (connectorType) {
    case 'kafka':
    case 'upsert-kafka':
      return 'consumer-group-clone';
    case 'jdbc':
      return 'periodic-poll';
    case 'paimon':
    case 'iceberg':
      return 'incremental-read';
    case 'datagen':
      return 'direct-read';
    case 'filesystem':
      return 'unsupported';
    default:
      return 'unsupported';
  }
}

// ── TapConfig normalization ──────────────────────────────────────────

/** Default tap configuration values */
const TAP_DEFAULTS: Required<TapConfig> = {
  name: '',
  groupIdPrefix: '',
  offsetMode: 'latest',
  startTimestamp: '',
  endTimestamp: '',
};

/**
 * Normalize a tap prop value into a full TapConfig with defaults.
 * `tap={true}` becomes a TapConfig with all defaults.
 */
export function normalizeTapConfig(
  tap: boolean | TapConfig,
  componentName: string,
  nodeId: string,
): Required<TapConfig> {
  if (typeof tap === 'boolean') {
    return {
      ...TAP_DEFAULTS,
      name: generateTapName(componentName, nodeId),
    };
  }

  return {
    name: tap.name ?? generateTapName(componentName, nodeId),
    groupIdPrefix: tap.groupIdPrefix ?? '',
    offsetMode: tap.offsetMode ?? 'latest',
    startTimestamp: tap.startTimestamp ?? '',
    endTimestamp: tap.endTimestamp ?? '',
  };
}

/**
 * Validate a normalized TapConfig.
 * Returns validation errors if the config is invalid.
 */
export function validateTapConfig(
  config: Required<TapConfig>,
  nodeId: string,
): ValidationDiagnostic[] {
  const diagnostics: ValidationDiagnostic[] = [];

  if (config.offsetMode === 'timestamp' && !config.startTimestamp) {
    diagnostics.push({
      severity: 'error',
      message: "startTimestamp is required when offsetMode is 'timestamp'",
      nodeId,
    });
  }

  if (config.startTimestamp && config.endTimestamp) {
    const start = new Date(config.startTimestamp).getTime();
    const end = new Date(config.endTimestamp).getTime();
    if (end <= start) {
      diagnostics.push({
        severity: 'error',
        message: 'endTimestamp must be after startTimestamp',
        nodeId,
      });
    }
  }

  return diagnostics;
}

/**
 * Generate a human-readable tap name from component type and node context.
 * e.g., "KafkaSink (orders)", "Filter #3"
 */
function generateTapName(componentName: string, nodeId: string): string {
  // If the nodeId looks like a meaningful name (not auto-generated), include it
  if (nodeId && !nodeId.match(/^[A-Z]\w+_\d+$/)) {
    return `${componentName} (${nodeId})`;
  }
  return `${componentName} ${nodeId}`;
}

// ── Consumer group ID ────────────────────────────────────────────────

/**
 * Generate a deterministic-prefix consumer group ID.
 * Pattern: `flink-reactor-tap-{pipelineName}-{nodeId}-{shortHash}`
 *
 * The hash is derived from the pipeline name and node ID for determinism.
 * A session-unique suffix should be appended at query execution time.
 */
export function buildConsumerGroupId(
  pipelineName: string,
  nodeId: string,
  prefix?: string,
): string {
  const shortHash = deterministicShortHash(`${pipelineName}:${nodeId}`);

  if (prefix) {
    return `${prefix}-${nodeId}-${shortHash}`;
  }

  return `flink-reactor-tap-${pipelineName}-${nodeId}-${shortHash}`;
}

/**
 * Simple deterministic hash function that produces an 8-char hex string.
 * Not cryptographic — just needs to be consistent and collision-resistant enough.
 */
function deterministicShortHash(input: string): string {
  let hash = 0x811c9dc5; // FNV-1a offset basis
  for (let i = 0; i < input.length; i++) {
    hash ^= input.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193); // FNV-1a prime
  }
  // Convert to unsigned 32-bit and then to 8-char hex
  return (hash >>> 0).toString(16).padStart(8, '0');
}

// ── Observation SQL generation ───────────────────────────────────────

/** Backtick-quote an identifier */
function q(identifier: string): string {
  return `\`${identifier}\``;
}

/**
 * Generate observation SQL for a single tapped node.
 * Returns a CREATE TEMPORARY TABLE DDL followed by a SELECT * query.
 */
export function buildObservationSql(
  nodeId: string,
  schema: Record<string, string>,
  connectorProperties: Record<string, string>,
  consumerGroupId: string,
  config: Required<TapConfig>,
): string {
  const tableName = `_tap_${nodeId}`;
  const connectorType = connectorProperties['connector'] ?? 'unknown';
  const strategy = resolveObservationStrategy(connectorType);

  // Build column definitions
  const columns = Object.entries(schema)
    .map(([name, type]) => `  ${q(name)} ${type}`)
    .join(',\n');

  // Build WITH clause with observation-specific overrides
  const withProps = buildObservationWithClause(
    connectorProperties,
    consumerGroupId,
    config,
    strategy,
  );

  const withClause = Object.entries(withProps)
    .map(([k, v]) => `  '${k}' = '${v}'`)
    .join(',\n');

  const ddl = `CREATE TEMPORARY TABLE ${q(tableName)} (\n${columns}\n) WITH (\n${withClause}\n);`;
  const query = `SELECT * FROM ${q(tableName)};`;

  return `${ddl}\n\n${query}`;
}

/**
 * Build the WITH clause properties for an observation table.
 * Clones connector properties with strategy-specific overrides.
 */
function buildObservationWithClause(
  connectorProperties: Record<string, string>,
  consumerGroupId: string,
  config: Required<TapConfig>,
  strategy: ObservationStrategy,
): Record<string, string> {
  const props = { ...connectorProperties };

  switch (strategy) {
    case 'consumer-group-clone': {
      // Override consumer group and startup mode
      props['properties.group.id'] = consumerGroupId;
      props['scan.startup.mode'] = mapOffsetMode(config.offsetMode);

      if (config.offsetMode === 'timestamp' && config.startTimestamp) {
        props['scan.startup.timestamp-millis'] = String(
          new Date(config.startTimestamp).getTime(),
        );
      }
      break;
    }
    case 'periodic-poll': {
      // JDBC: keep url and table-name, no special overrides needed
      // The SQL Gateway will re-execute the SELECT periodically
      break;
    }
    case 'incremental-read': {
      // Paimon/Iceberg: enable streaming read mode
      props['streaming-read-mode'] = 'latest';
      break;
    }
    case 'direct-read': {
      // Datagen: clone as-is, no overrides needed
      break;
    }
    case 'unsupported':
      // Should not reach here — unsupported connectors are filtered out
      break;
  }

  return props;
}

/** Map TapOffsetMode to Flink scan.startup.mode */
function mapOffsetMode(mode: TapOffsetMode): string {
  switch (mode) {
    case 'latest': return 'latest-offset';
    case 'earliest': return 'earliest-offset';
    case 'timestamp': return 'timestamp';
  }
}

// ── Connector property extraction ────────────────────────────────────

/**
 * Extract connector properties from a construct node's props.
 * Returns the properties needed to reconstruct a connector's WITH clause.
 */
function extractConnectorProperties(node: ConstructNode): Record<string, string> {
  const props = node.props;
  const result: Record<string, string> = {};

  switch (node.component) {
    case 'KafkaSource':
      result['connector'] = 'kafka';
      result['topic'] = props.topic as string;
      result['format'] = (props.format as string) ?? 'json';
      if (props.bootstrapServers) {
        result['properties.bootstrap.servers'] = props.bootstrapServers as string;
      }
      if (props.consumerGroup) {
        result['properties.group.id'] = props.consumerGroup as string;
      }
      break;

    case 'KafkaSink':
      result['connector'] = 'kafka';
      result['topic'] = props.topic as string;
      result['format'] = (props.format as string) ?? 'json';
      if (props.bootstrapServers) {
        result['properties.bootstrap.servers'] = props.bootstrapServers as string;
      }
      break;

    case 'JdbcSource':
      result['connector'] = 'jdbc';
      result['url'] = props.url as string;
      result['table-name'] = props.table as string;
      break;

    case 'JdbcSink':
      result['connector'] = 'jdbc';
      result['url'] = props.url as string;
      result['table-name'] = props.table as string;
      break;

    case 'GenericSource':
    case 'GenericSink':
      result['connector'] = props.connector as string;
      if (props.format) result['format'] = props.format as string;
      if (props.options) {
        Object.assign(result, props.options as Record<string, string>);
      }
      break;

    case 'FileSystemSink':
      result['connector'] = 'filesystem';
      result['path'] = props.path as string;
      if (props.format) result['format'] = props.format as string;
      break;

    case 'PaimonSink':
      result['connector'] = 'paimon';
      break;

    case 'IcebergSink':
      result['connector'] = 'iceberg';
      break;
  }

  return result;
}

/**
 * Extract schema from a construct node's props.
 * Returns field name → Flink type mapping.
 */
function extractNodeSchema(node: ConstructNode): Record<string, string> {
  const schema = node.props.schema as { fields: Record<string, string> } | undefined;
  if (schema?.fields) {
    return { ...schema.fields };
  }
  return {};
}

/**
 * Walk upstream through a node's children to find the nearest source or sink
 * with connector properties. Used for transforms, joins, and windows that
 * don't have their own connector.
 */
function findUpstreamConnectorNode(node: ConstructNode): ConstructNode | null {
  // If this node is a source or sink, it has connector properties
  if (node.kind === 'Source' || node.kind === 'Sink') {
    return node;
  }

  // Walk children (upstream in the construct tree) to find a connector node
  for (const child of node.children) {
    const found = findUpstreamConnectorNode(child);
    if (found) return found;
  }

  return null;
}

/**
 * Extract connector properties and schema from a node, potentially
 * walking upstream for transforms that don't have their own connector.
 */
function resolveConnectorContext(node: ConstructNode): {
  connectorProps: Record<string, string>;
  schema: Record<string, string>;
} {
  // Sources and sinks have their own connector properties
  if (node.kind === 'Source' || node.kind === 'Sink') {
    return {
      connectorProps: extractConnectorProperties(node),
      schema: extractNodeSchema(node),
    };
  }

  // Transforms, joins, windows: walk upstream to find the source connector
  const upstreamNode = findUpstreamConnectorNode(node);
  if (upstreamNode) {
    return {
      connectorProps: extractConnectorProperties(upstreamNode),
      schema: extractNodeSchema(upstreamNode),
    };
  }

  return { connectorProps: {}, schema: {} };
}

// ── Main metadata generation ─────────────────────────────────────────

/**
 * Walk the construct tree and generate tap metadata for all tapped nodes.
 *
 * In dev mode, all sinks are auto-tapped with default config.
 * In prod mode, only explicitly tapped operators produce metadata.
 */
export function generateTapMetadata(
  tree: ConstructNode,
  pipelineName: string,
  flinkVersion: string,
  devMode: boolean,
): { taps: TapMetadata[]; diagnostics: ValidationDiagnostic[] } {
  const taps: TapMetadata[] = [];
  const diagnostics: ValidationDiagnostic[] = [];

  function walk(node: ConstructNode): void {
    const tapProp = node.props.tap as boolean | TapConfig | undefined;
    const isDevAutoTap = devMode && node.kind === 'Sink' && !tapProp;

    if (tapProp || isDevAutoTap) {
      const tapValue = tapProp ?? true;
      const config = normalizeTapConfig(tapValue, node.component, node.id);

      // Validate config
      const configDiagnostics = validateTapConfig(config, node.id);
      diagnostics.push(...configDiagnostics);

      // Skip if there are validation errors
      if (configDiagnostics.some((d) => d.severity === 'error')) {
        // Still recurse children
        for (const child of node.children) {
          walk(child);
        }
        return;
      }

      // Extract connector properties and schema (walks upstream for transforms)
      const { connectorProps, schema } = resolveConnectorContext(node);
      const connectorType = connectorProps['connector'] ?? 'unknown';
      const strategy = resolveObservationStrategy(connectorType);

      // Handle unsupported connectors
      if (strategy === 'unsupported') {
        diagnostics.push({
          severity: 'warning',
          message: `Tap is not supported for ${connectorType === 'filesystem' ? 'FileSystem' : connectorType} connectors`,
          nodeId: node.id,
          component: node.component,
        });
        // Still recurse children
        for (const child of node.children) {
          walk(child);
        }
        return;
      }
      const consumerGroupId = buildConsumerGroupId(
        pipelineName,
        node.id,
        config.groupIdPrefix || undefined,
      );
      const observationSql = buildObservationSql(
        node.id,
        schema,
        connectorProps,
        consumerGroupId,
        config,
      );

      taps.push({
        nodeId: node.id,
        name: config.name,
        componentType: node.kind.toLowerCase(),
        componentName: node.component,
        schema,
        connectorType,
        observationSql,
        consumerGroupId,
        config,
        connectorProperties: connectorProps,
      });
    }

    // Recurse into children
    for (const child of node.children) {
      walk(child);
    }
  }

  walk(tree);

  return { taps, diagnostics };
}
