import type { ConstructNode, FlinkMajorVersion } from '../core/types.js';
import type { SchemaDefinition } from '../core/schema.js';
import { FlinkVersionCompat } from '../core/flink-compat.js';
import { SynthContext } from '../core/synth-context.js';

// ── Public API ──────────────────────────────────────────────────────

export interface GenerateSqlOptions {
  readonly flinkVersion?: FlinkMajorVersion;
}

export interface GenerateSqlResult {
  readonly statements: readonly string[];
  readonly sql: string;
}

/**
 * Generate Flink SQL from a Pipeline construct tree.
 *
 * Walks the construct tree, builds a DAG, then emits SQL in
 * deterministic order:
 *   1. SET statements (pipeline config)
 *   2. CREATE CATALOG
 *   3. CREATE TABLE (sources)
 *   4. CREATE TABLE (sinks)
 *   5. INSERT INTO / STATEMENT SET
 */
export function generateSql(
  pipelineNode: ConstructNode,
  options: GenerateSqlOptions = {},
): GenerateSqlResult {
  const version = options.flinkVersion ?? '2.0';

  // Build a node index for lookups by ID
  const nodeIndex = new Map<string, ConstructNode>();
  indexTree(pipelineNode, nodeIndex);

  const ctx = new SynthContext();
  ctx.buildFromTree(pipelineNode);

  const statements: string[] = [];

  // 1. SET statements from pipeline props
  statements.push(...generateSetStatements(pipelineNode, version));

  // 2. CREATE CATALOG
  const catalogs = ctx.getNodesByKind('Catalog');
  for (const cat of catalogs) {
    statements.push(generateCatalogDdl(cat));
  }

  // 2.5. CREATE FUNCTION (UDFs)
  const udfs = ctx.getNodesByKind('UDF');
  for (const udf of udfs) {
    statements.push(generateUdfDdl(udf));
  }

  // 3. CREATE TABLE (sources)
  const sources = ctx.getNodesByKind('Source');
  for (const src of sources) {
    const ddl = generateSourceDdl(src);
    if (ddl) statements.push(ddl);
  }

  // 4. CREATE TABLE (sinks)
  const sinks = ctx.getNodesByKind('Sink');
  for (const sink of sinks) {
    statements.push(generateSinkDdl(sink));
  }

  // 4.5. CREATE VIEW (only for View nodes that define a query, not references)
  const views = ctx.getNodesByKind('View');
  for (const view of views) {
    if (view.children.length > 0) {
      statements.push(generateViewDdl(view, nodeIndex));
    }
  }

  // 5. DML: INSERT INTO / STATEMENT SET
  const dmlStatements = generateDml(pipelineNode, nodeIndex);
  if (dmlStatements.length === 1) {
    statements.push(dmlStatements[0]);
  } else if (dmlStatements.length > 1) {
    statements.push(
      `EXECUTE STATEMENT SET BEGIN\n${dmlStatements.join('\n')}\nEND;`,
    );
  }

  return {
    statements,
    sql: statements.join('\n\n') + '\n',
  };
}

// ── Tree indexing ───────────────────────────────────────────────────

function indexTree(node: ConstructNode, index: Map<string, ConstructNode>): void {
  index.set(node.id, node);
  for (const child of node.children) {
    indexTree(child, index);
  }
}

// ── Identifier quoting ──────────────────────────────────────────────

function q(identifier: string): string {
  return `\`${identifier}\``;
}

// ── Duration parsing ────────────────────────────────────────────────

function toInterval(duration: string): string {
  const lower = duration.trim().toLowerCase();

  if (lower.startsWith('interval')) return duration;

  if (/^\d+$/.test(lower)) {
    const ms = parseInt(lower, 10);
    if (ms % 86400000 === 0) return `INTERVAL '${ms / 86400000}' DAY`;
    if (ms % 3600000 === 0) return `INTERVAL '${ms / 3600000}' HOUR`;
    if (ms % 60000 === 0) return `INTERVAL '${ms / 60000}' MINUTE`;
    return `INTERVAL '${ms / 1000}' SECOND`;
  }

  const match = lower.match(/^(\d+)\s*(s|sec|second|seconds|m|min|minute|minutes|h|hr|hour|hours|d|day|days)$/);
  if (match) {
    const value = match[1];
    const unit = match[2];
    if (unit.startsWith('s')) return `INTERVAL '${value}' SECOND`;
    if (unit.startsWith('m')) return `INTERVAL '${value}' MINUTE`;
    if (unit.startsWith('h')) return `INTERVAL '${value}' HOUR`;
    if (unit.startsWith('d')) return `INTERVAL '${value}' DAY`;
  }

  return `INTERVAL '${duration}'`;
}

function toMilliseconds(duration: string): number {
  const lower = duration.trim().toLowerCase();

  if (/^\d+$/.test(lower)) return parseInt(lower, 10);

  const match = lower.match(/^(\d+)\s*(ms|s|sec|second|seconds|m|min|minute|minutes|h|hr|hour|hours)$/);
  if (match) {
    const value = parseInt(match[1], 10);
    const unit = match[2];
    if (unit === 'ms') return value;
    if (unit.startsWith('s')) return value * 1000;
    if (unit.startsWith('m')) return value * 60000;
    if (unit.startsWith('h')) return value * 3600000;
  }

  return parseInt(lower, 10);
}

// ── SET statements ──────────────────────────────────────────────────

function generateSetStatements(
  pipeline: ConstructNode,
  version: FlinkMajorVersion,
): string[] {
  const config: Record<string, string> = {};
  const props = pipeline.props;

  if (props.mode) {
    config['execution.runtime-mode'] = props.mode as string;
  }

  if (props.parallelism !== undefined) {
    config['parallelism.default'] = String(props.parallelism);
  }

  const checkpoint = props.checkpoint as { interval: string; mode?: string } | undefined;
  if (checkpoint) {
    config['execution.checkpointing.interval'] = String(toMilliseconds(checkpoint.interval));
    if (checkpoint.mode) {
      config['execution.checkpointing.mode'] = checkpoint.mode;
    }
  }

  if (props.stateBackend) {
    config['state.backend.type'] = props.stateBackend as string;
  }

  if (props.stateTtl) {
    config['table.exec.state.ttl'] = String(toMilliseconds(props.stateTtl as string));
  }

  if (props.restartStrategy) {
    const rs = props.restartStrategy as { type: string; attempts?: number; delay?: string };
    config['restart-strategy.type'] = rs.type;
    if (rs.attempts !== undefined) {
      config['restart-strategy.fixed-delay.attempts'] = String(rs.attempts);
    }
    if (rs.delay) {
      config['restart-strategy.fixed-delay.delay'] = rs.delay;
    }
  }

  const userConfig = props.flinkConfig as Record<string, string> | undefined;
  if (userConfig) {
    Object.assign(config, userConfig);
  }

  const normalized = FlinkVersionCompat.normalizeConfig(config, version);

  return Object.entries(normalized).map(
    ([key, value]) => `SET '${key}' = '${value}';`,
  );
}

// ── CREATE CATALOG DDL ──────────────────────────────────────────────

function generateCatalogDdl(node: ConstructNode): string {
  const props = node.props;
  const name = props.name as string;
  const withProps: Record<string, string> = {};

  switch (node.component) {
    case 'PaimonCatalog':
      withProps['type'] = 'paimon';
      withProps['warehouse'] = props.warehouse as string;
      if (props.metastore) withProps['metastore'] = props.metastore as string;
      break;
    case 'IcebergCatalog':
      withProps['type'] = 'iceberg';
      withProps['catalog-type'] = props.catalogType as string;
      withProps['uri'] = props.uri as string;
      break;
    case 'HiveCatalog':
      withProps['type'] = 'hive';
      withProps['hive-conf-dir'] = props.hiveConfDir as string;
      break;
    case 'JdbcCatalog':
      withProps['type'] = 'jdbc';
      withProps['base-url'] = props.baseUrl as string;
      withProps['default-database'] = props.defaultDatabase as string;
      break;
    case 'GenericCatalog':
      withProps['type'] = props.type as string;
      if (props.options) {
        Object.assign(withProps, props.options as Record<string, string>);
      }
      break;
  }

  const withClause = Object.entries(withProps)
    .map(([k, v]) => `  '${k}' = '${v}'`)
    .join(',\n');

  return `CREATE CATALOG ${q(name)} WITH (\n${withClause}\n);`;
}

// ── CREATE FUNCTION DDL (UDFs) ──────────────────────────────────────

function generateUdfDdl(node: ConstructNode): string {
  const name = node.props.name as string;
  const className = node.props.className as string;
  return `CREATE FUNCTION ${q(name)} AS '${className}';`;
}

// ── CREATE TABLE DDL (Sources) ──────────────────────────────────────

function generateSourceDdl(node: ConstructNode): string | null {
  if (node.component === 'CatalogSource') return null;

  const props = node.props;
  const schema = props.schema as SchemaDefinition;
  const tableName = node.id;

  const columns = generateColumns(schema);
  const constraints = generateConstraints(schema, props);
  const withClause = generateSourceWithClause(node);

  const parts = [
    `CREATE TABLE ${q(tableName)} (`,
    columns,
  ];

  if (constraints.length > 0) {
    parts[parts.length - 1] += ',';
    parts.push(constraints);
  }

  parts.push(`) WITH (\n${withClause}\n);`);

  return parts.join('\n');
}

function generateColumns(schema: SchemaDefinition): string {
  const lines: string[] = [];

  for (const [name, type] of Object.entries(schema.fields)) {
    lines.push(`  ${q(name)} ${type}`);
  }

  for (const meta of schema.metadataColumns) {
    let line = `  ${q(meta.column)} ${meta.type} METADATA`;
    if (meta.from) line += ` FROM '${meta.from}'`;
    if (meta.isVirtual) line += ' VIRTUAL';
    lines.push(line);
  }

  if (schema.watermark) {
    lines.push(
      `  WATERMARK FOR ${q(schema.watermark.column)} AS ${schema.watermark.expression}`,
    );
  }

  return lines.join(',\n');
}

function generateConstraints(
  schema: SchemaDefinition,
  props: Record<string, unknown>,
): string {
  const parts: string[] = [];

  if (schema.primaryKey) {
    const cols = schema.primaryKey.columns.map(q).join(', ');
    parts.push(`  PRIMARY KEY (${cols}) NOT ENFORCED`);
  }

  if (!schema.primaryKey && props.primaryKey) {
    const cols = (props.primaryKey as readonly string[]).map(q).join(', ');
    parts.push(`  PRIMARY KEY (${cols}) NOT ENFORCED`);
  }

  return parts.join(',\n');
}

function generateSourceWithClause(node: ConstructNode): string {
  const props = node.props;
  const withProps: Record<string, string> = {};

  switch (node.component) {
    case 'KafkaSource':
      withProps['connector'] = 'kafka';
      withProps['topic'] = props.topic as string;
      withProps['format'] = (props.format as string) ?? 'json';
      if (props.bootstrapServers) {
        withProps['properties.bootstrap.servers'] = props.bootstrapServers as string;
      }
      if (props.startupMode) {
        withProps['scan.startup.mode'] = props.startupMode as string;
      }
      if (props.consumerGroup) {
        withProps['properties.group.id'] = props.consumerGroup as string;
      }
      break;
    case 'JdbcSource':
      withProps['connector'] = 'jdbc';
      withProps['url'] = props.url as string;
      withProps['table-name'] = props.table as string;
      if (props.lookupCache) {
        const cache = props.lookupCache as { maxRows: number; ttl: string };
        withProps['lookup.cache.max-rows'] = String(cache.maxRows);
        withProps['lookup.cache.ttl'] = cache.ttl;
      }
      break;
    case 'GenericSource':
      withProps['connector'] = props.connector as string;
      if (props.format) withProps['format'] = props.format as string;
      if (props.options) {
        Object.assign(withProps, props.options as Record<string, string>);
      }
      break;
  }

  return Object.entries(withProps)
    .map(([k, v]) => `  '${k}' = '${v}'`)
    .join(',\n');
}

// ── CREATE TABLE DDL (Sinks) ────────────────────────────────────────

function generateSinkDdl(node: ConstructNode): string {
  const props = node.props;
  const tableName = node.id;

  if (props.catalogName) {
    return `-- ${node.component} ${q(String(props.catalogName))}.${q(String(props.database))}.${q(String(props.table))} (catalog-managed)`;
  }

  const withClause = generateSinkWithClause(node);
  const parts: string[] = [];

  if (node.component === 'JdbcSink' && props.upsertMode && props.keyFields) {
    const cols = (props.keyFields as readonly string[]).map(q).join(', ');
    parts.push(
      `CREATE TABLE ${q(tableName)} (`,
      `  PRIMARY KEY (${cols}) NOT ENFORCED`,
      `) WITH (\n${withClause}\n);`,
    );
  } else if (node.component === 'FileSystemSink' && props.partitionBy) {
    const partCols = (props.partitionBy as readonly string[]).join(', ');
    parts.push(
      `CREATE TABLE ${q(tableName)} PARTITIONED BY (${partCols}) WITH (\n${withClause}\n);`,
    );
  } else {
    parts.push(`CREATE TABLE ${q(tableName)} WITH (\n${withClause}\n);`);
  }

  return parts.join('\n');
}

function generateSinkWithClause(node: ConstructNode): string {
  const props = node.props;
  const withProps: Record<string, string> = {};

  switch (node.component) {
    case 'KafkaSink':
      withProps['connector'] = 'kafka';
      withProps['topic'] = props.topic as string;
      withProps['format'] = (props.format as string) ?? 'json';
      if (props.bootstrapServers) {
        withProps['properties.bootstrap.servers'] = props.bootstrapServers as string;
      }
      break;
    case 'JdbcSink':
      withProps['connector'] = 'jdbc';
      withProps['url'] = props.url as string;
      withProps['table-name'] = props.table as string;
      break;
    case 'FileSystemSink':
      withProps['connector'] = 'filesystem';
      withProps['path'] = props.path as string;
      if (props.format) withProps['format'] = props.format as string;
      break;
    case 'GenericSink':
      withProps['connector'] = props.connector as string;
      if (props.options) {
        Object.assign(withProps, props.options as Record<string, string>);
      }
      break;
  }

  return Object.entries(withProps)
    .map(([k, v]) => `  '${k}' = '${v}'`)
    .join(',\n');
}

// ── DML generation ──────────────────────────────────────────────────

/*
 * The construct tree uses parent→child ordering where the DOWNSTREAM
 * node is the parent. For example:
 *
 *   Pipeline
 *     └── KafkaSink (children: [Filter])
 *           └── Filter (children: [KafkaSource])
 *                 └── KafkaSource
 *
 * To build SQL we walk from each Sink node DOWN through its children
 * (toward sources) to assemble the SELECT expression.
 */

function generateDml(
  pipelineNode: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
): string[] {
  const statements: string[] = [];

  // Collect all sinks from the pipeline's direct children
  collectSinkDml(pipelineNode, nodeIndex, statements);

  return statements;
}

function collectSinkDml(
  node: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
  statements: string[],
): void {
  if (node.kind === 'Sink') {
    const sinkRef = resolveSinkRef(node);
    // The sink's children are its upstream (transforms/sources)
    const upstream = node.children.length > 0
      ? buildQuery(node.children[0], nodeIndex)
      : 'SELECT * FROM unknown';
    statements.push(`INSERT INTO ${sinkRef}\n${upstream};`);
    // Recurse into upstream to find SideOutput/Validate side-path DML
    for (const child of node.children) {
      collectSideDml(child, nodeIndex, statements);
    }
    return;
  }

  // Route generates multiple INSERT statements (one per branch)
  if (node.component === 'Route') {
    collectRouteDml(node, nodeIndex, statements);
    return;
  }

  // SideOutput generates two INSERT statements (main + side)
  if (node.component === 'SideOutput') {
    collectSideOutputDml(node, nodeIndex, statements);
    return;
  }

  // Validate generates two INSERT statements (valid + rejected)
  if (node.component === 'Validate') {
    collectValidateDml(node, nodeIndex, statements);
    return;
  }

  // Recurse into children to find sinks
  for (const child of node.children) {
    collectSinkDml(child, nodeIndex, statements);
  }
}

function collectRouteDml(
  routeNode: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
  statements: string[],
): void {
  // Route's upstream is found by looking at its own children that are
  // NOT Route.Branch / Route.Default
  const upstreamChildren = routeNode.children.filter(
    (c) => c.component !== 'Route.Branch' && c.component !== 'Route.Default',
  );
  const branches = routeNode.children.filter(
    (c) => c.component === 'Route.Branch' || c.component === 'Route.Default',
  );

  // Find the source upstream of the route
  const sourceQuery = upstreamChildren.length > 0
    ? buildQuery(upstreamChildren[0], nodeIndex)
    : 'SELECT * FROM unknown';

  for (const branch of branches) {
    const condition = branch.props.condition as string | undefined;

    // Find sinks inside the branch
    for (const child of branch.children) {
      if (child.kind === 'Sink') {
        const sinkRef = resolveSinkRef(child);
        if (condition) {
          statements.push(`INSERT INTO ${sinkRef}\n${sourceQuery} WHERE ${condition};`);
        } else {
          statements.push(`INSERT INTO ${sinkRef}\n${sourceQuery};`);
        }
      }
    }
  }
}

function resolveSinkRef(sink: ConstructNode): string {
  if (sink.props.catalogName) {
    return `${q(String(sink.props.catalogName))}.${q(String(sink.props.database))}.${q(String(sink.props.table))}`;
  }
  return q(sink.id);
}

// ── Query building ──────────────────────────────────────────────────

/*
 * buildQuery walks a node and its children (toward sources) to
 * produce a SQL SELECT expression. Each transform type adds its
 * own SQL clause on top of the upstream query.
 */

function buildQuery(
  node: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
): string {
  switch (node.component) {
    // Sources — terminal nodes
    case 'KafkaSource':
    case 'JdbcSource':
    case 'GenericSource':
      return `SELECT * FROM ${q(node.id)}`;
    case 'CatalogSource':
      return `SELECT * FROM ${q(String(node.props.catalogName))}.${q(String(node.props.database))}.${q(String(node.props.table))}`;

    // Transforms
    case 'Filter':
      return buildFilterQuery(node, nodeIndex);
    case 'Map':
      return buildMapQuery(node, nodeIndex);
    case 'FlatMap':
      return buildFlatMapQuery(node, nodeIndex);
    case 'Aggregate':
      return buildAggregateQuery(node, nodeIndex);
    case 'Union':
      return buildUnionQuery(node, nodeIndex);
    case 'Deduplicate':
      return buildDeduplicateQuery(node, nodeIndex);
    case 'TopN':
      return buildTopNQuery(node, nodeIndex);

    // Joins
    case 'Join':
      return buildJoinQuery(node, nodeIndex);
    case 'TemporalJoin':
      return buildTemporalJoinQuery(node, nodeIndex);
    case 'LookupJoin':
      return buildLookupJoinQuery(node, nodeIndex);
    case 'IntervalJoin':
      return buildIntervalJoinQuery(node, nodeIndex);

    // Windows
    case 'TumbleWindow':
    case 'SlideWindow':
    case 'SessionWindow':
      return buildWindowQuery(node, nodeIndex);

    // Escape hatches
    case 'Query':
      return buildQueryComponentQuery(node, nodeIndex);
    case 'RawSQL':
      return buildRawSqlQuery(node);

    // CEP
    case 'MatchRecognize':
      return buildMatchRecognizeQuery(node, nodeIndex);

    // Validate — main path (valid records)
    case 'Validate':
      return buildValidateQuery(node, nodeIndex);

    // View — reference by name
    case 'View':
      return `SELECT * FROM ${q(node.props.name as string)}`;

    // SideOutput — main path (non-matching records)
    case 'SideOutput':
      return buildSideOutputQuery(node, nodeIndex);

    // LateralJoin — LATERAL TABLE TVF join
    case 'LateralJoin':
      return buildLateralJoinQuery(node, nodeIndex);

    default:
      // Unknown — try first child
      if (node.children.length > 0) {
        return buildQuery(node.children[0], nodeIndex);
      }
      return `SELECT * FROM ${q(node.id)}`;
  }
}

/**
 * Get the upstream SQL and source table reference from a node's children.
 */
function getUpstream(node: ConstructNode, nodeIndex: Map<string, ConstructNode>): {
  sql: string;
  sourceRef: string;
  isSimple: boolean;
} {
  if (node.children.length === 0) {
    return { sql: 'SELECT * FROM unknown', sourceRef: 'unknown', isSimple: false };
  }

  const child = node.children[0];

  // If the child is a source, return a simple reference
  if (child.kind === 'Source') {
    const ref = child.component === 'CatalogSource'
      ? `${q(String(child.props.catalogName))}.${q(String(child.props.database))}.${q(String(child.props.table))}`
      : q(child.id);
    return { sql: `SELECT * FROM ${ref}`, sourceRef: ref, isSimple: true };
  }

  // Otherwise build the child query
  const childSql = buildQuery(child, nodeIndex);
  return { sql: childSql, sourceRef: child.id, isSimple: false };
}

// ── Filter ──────────────────────────────────────────────────────────

function buildFilterQuery(node: ConstructNode, nodeIndex: Map<string, ConstructNode>): string {
  const condition = node.props.condition as string;
  const upstream = getUpstream(node, nodeIndex);

  if (upstream.isSimple) {
    return `SELECT * FROM ${upstream.sourceRef} WHERE ${condition}`;
  }
  return `SELECT * FROM (\n${upstream.sql}\n) WHERE ${condition}`;
}

// ── Map ─────────────────────────────────────────────────────────────

function buildMapQuery(node: ConstructNode, nodeIndex: Map<string, ConstructNode>): string {
  const select = node.props.select as Record<string, string>;
  const projections = Object.entries(select)
    .map(([alias, expr]) => `${expr} AS ${q(alias)}`)
    .join(', ');

  const upstream = getUpstream(node, nodeIndex);

  if (upstream.isSimple) {
    return `SELECT ${projections} FROM ${upstream.sourceRef}`;
  }
  return `SELECT ${projections} FROM (\n${upstream.sql}\n)`;
}

// ── FlatMap ─────────────────────────────────────────────────────────

function buildFlatMapQuery(node: ConstructNode, nodeIndex: Map<string, ConstructNode>): string {
  const unnestField = node.props.unnest as string;
  const asFields = node.props.as as Record<string, string>;
  const aliases = Object.keys(asFields).map(q).join(', ');

  const upstream = getUpstream(node, nodeIndex);
  const ref = upstream.sourceRef;

  return `SELECT ${q(ref)}.*, ${aliases} FROM ${upstream.isSimple ? ref : `(\n${upstream.sql}\n) AS ${q(ref)}`} CROSS JOIN UNNEST(${q(ref)}.${q(unnestField)}) AS T(${aliases})`;
}

// ── Aggregate ───────────────────────────────────────────────────────

function buildAggregateQuery(node: ConstructNode, nodeIndex: Map<string, ConstructNode>): string {
  const groupBy = node.props.groupBy as readonly string[];
  const select = node.props.select as Record<string, string>;

  const groupCols = groupBy.map(q).join(', ');
  const projections = [
    ...groupBy.map(q),
    ...Object.entries(select).map(([alias, expr]) => `${expr} AS ${q(alias)}`),
  ].join(', ');

  const upstream = getUpstream(node, nodeIndex);

  if (upstream.isSimple) {
    return `SELECT ${projections} FROM ${upstream.sourceRef} GROUP BY ${groupCols}`;
  }
  return `SELECT ${projections} FROM (\n${upstream.sql}\n) GROUP BY ${groupCols}`;
}

// ── Union ───────────────────────────────────────────────────────────

function buildUnionQuery(node: ConstructNode, nodeIndex: Map<string, ConstructNode>): string {
  const parts: string[] = [];

  for (const child of node.children) {
    parts.push(buildQuery(child, nodeIndex));
  }

  if (parts.length === 0) return 'SELECT * FROM unknown';
  return parts.join('\nUNION ALL\n');
}

// ── Deduplicate ─────────────────────────────────────────────────────

function buildDeduplicateQuery(node: ConstructNode, nodeIndex: Map<string, ConstructNode>): string {
  const key = node.props.key as readonly string[];
  const order = node.props.order as string;
  const keep = node.props.keep as 'first' | 'last';

  const partitionBy = key.map(q).join(', ');
  const orderDir = keep === 'first' ? 'ASC' : 'DESC';

  const upstream = getUpstream(node, nodeIndex);
  const fromClause = upstream.isSimple ? upstream.sourceRef : `(\n${upstream.sql}\n)`;

  return [
    'SELECT * FROM (',
    `  SELECT *, ROW_NUMBER() OVER (PARTITION BY ${partitionBy} ORDER BY ${q(order)} ${orderDir}) AS rownum`,
    `  FROM ${fromClause}`,
    ') WHERE rownum = 1',
  ].join('\n');
}

// ── TopN ────────────────────────────────────────────────────────────

function buildTopNQuery(node: ConstructNode, nodeIndex: Map<string, ConstructNode>): string {
  const partitionBy = (node.props.partitionBy as readonly string[]).map(q).join(', ');
  const orderBy = node.props.orderBy as Record<string, 'ASC' | 'DESC'>;
  const n = node.props.n as number;

  const orderClause = Object.entries(orderBy)
    .map(([field, dir]) => `${q(field)} ${dir}`)
    .join(', ');

  const upstream = getUpstream(node, nodeIndex);
  const fromClause = upstream.isSimple ? upstream.sourceRef : `(\n${upstream.sql}\n)`;

  return [
    'SELECT * FROM (',
    `  SELECT *, ROW_NUMBER() OVER (PARTITION BY ${partitionBy} ORDER BY ${orderClause}) AS rownum`,
    `  FROM ${fromClause}`,
    `) WHERE rownum <= ${n}`,
  ].join('\n');
}

// ── Join ────────────────────────────────────────────────────────────

function buildJoinQuery(node: ConstructNode, nodeIndex: Map<string, ConstructNode>): string {
  const joinType = (node.props.type as string) ?? 'inner';
  const onCondition = node.props.on as string;
  const hints = node.props.hints as { broadcast?: 'left' | 'right' } | undefined;
  const leftId = node.props.left as string;
  const rightId = node.props.right as string;

  const leftRef = resolveRef(leftId, nodeIndex);
  const rightRef = resolveRef(rightId, nodeIndex);

  if (joinType === 'anti') {
    return `SELECT ${leftRef}.* FROM ${leftRef} WHERE NOT EXISTS (\n  SELECT 1 FROM ${rightRef} WHERE ${onCondition}\n)`;
  }

  if (joinType === 'semi') {
    return `SELECT ${leftRef}.* FROM ${leftRef} WHERE EXISTS (\n  SELECT 1 FROM ${rightRef} WHERE ${onCondition}\n)`;
  }

  const sqlJoinType = joinType === 'full' ? 'FULL OUTER' : joinType.toUpperCase();

  const hintClause = hints?.broadcast
    ? `/*+ BROADCAST(${resolveRef(hints.broadcast === 'left' ? leftId : rightId, nodeIndex)}) */ `
    : '';

  return `SELECT ${hintClause}* FROM ${leftRef} ${sqlJoinType} JOIN ${rightRef} ON ${onCondition}`;
}

// ── Temporal Join ───────────────────────────────────────────────────

function buildTemporalJoinQuery(node: ConstructNode, nodeIndex: Map<string, ConstructNode>): string {
  const onCondition = node.props.on as string;
  const asOf = node.props.asOf as string;
  const streamId = node.props.stream as string;
  const temporalId = node.props.temporal as string;

  const streamRef = resolveRef(streamId, nodeIndex);
  const temporalRef = resolveRef(temporalId, nodeIndex);

  return `SELECT * FROM ${streamRef} LEFT JOIN ${temporalRef} FOR SYSTEM_TIME AS OF ${streamRef}.${q(asOf)} ON ${onCondition}`;
}

// ── Lookup Join ─────────────────────────────────────────────────────

function buildLookupJoinQuery(node: ConstructNode, nodeIndex: Map<string, ConstructNode>): string {
  const onCondition = node.props.on as string;
  const inputId = node.props.input as string;
  const table = node.props.table as string;
  const select = node.props.select as Record<string, string> | undefined;

  const inputRef = resolveRef(inputId, nodeIndex);

  const projections = select
    ? Object.entries(select).map(([alias, expr]) => `${expr} AS ${q(alias)}`).join(', ')
    : '*';

  return `SELECT ${projections} FROM ${inputRef} LEFT JOIN ${q(table)} FOR SYSTEM_TIME AS OF ${inputRef}.proc_time ON ${onCondition}`;
}

// ── Interval Join ───────────────────────────────────────────────────

function buildIntervalJoinQuery(node: ConstructNode, nodeIndex: Map<string, ConstructNode>): string {
  const joinType = (node.props.type as string) ?? 'inner';
  const onCondition = node.props.on as string;
  const interval = node.props.interval as { from: string; to: string };
  const leftId = node.props.left as string;
  const rightId = node.props.right as string;

  const leftRef = resolveRef(leftId, nodeIndex);
  const rightRef = resolveRef(rightId, nodeIndex);

  const sqlJoinType = joinType === 'inner' ? '' : `${joinType.toUpperCase()} `;

  return `SELECT * FROM ${leftRef} ${sqlJoinType}JOIN ${rightRef} ON ${onCondition} AND ${leftRef}.${interval.from} BETWEEN ${rightRef}.${interval.from} AND ${rightRef}.${interval.to}`;
}

// ── Window ──────────────────────────────────────────────────────────

function buildWindowQuery(node: ConstructNode, nodeIndex: Map<string, ConstructNode>): string {
  const windowCol = node.props.on as string;

  // Find the Aggregate child (if windowed aggregation)
  const aggChild = node.children.find((c) => c.component === 'Aggregate');

  // Find the source feeding the window (non-Aggregate child)
  const sourceChild = node.children.find((c) => c.component !== 'Aggregate');
  const upstream = sourceChild ? getUpstream({ children: [sourceChild] } as ConstructNode, nodeIndex) : getUpstream(node, nodeIndex);
  const sourceRef = upstream.isSimple ? upstream.sourceRef : `(\n${upstream.sql}\n)`;

  const tvf = buildWindowTvf(node, sourceRef, windowCol);

  if (!aggChild) {
    return `SELECT * FROM TABLE(\n  ${tvf}\n)`;
  }

  const groupBy = aggChild.props.groupBy as readonly string[];
  const select = aggChild.props.select as Record<string, string>;

  const groupCols = [
    ...groupBy.map(q),
    'window_start',
    'window_end',
  ].join(', ');

  const projections = [
    ...groupBy.map(q),
    ...Object.entries(select).map(([alias, expr]) => `${expr} AS ${q(alias)}`),
    'window_start',
    'window_end',
  ].join(', ');

  return `SELECT ${projections} FROM TABLE(\n  ${tvf}\n) GROUP BY ${groupCols}`;
}

function buildWindowTvf(node: ConstructNode, sourceRef: string, windowCol: string): string {
  switch (node.component) {
    case 'TumbleWindow': {
      const size = toInterval(node.props.size as string);
      return `TUMBLE(TABLE ${sourceRef}, DESCRIPTOR(${q(windowCol)}), ${size})`;
    }
    case 'SlideWindow': {
      const size = toInterval(node.props.size as string);
      const slide = toInterval(node.props.slide as string);
      return `HOP(TABLE ${sourceRef}, DESCRIPTOR(${q(windowCol)}), ${slide}, ${size})`;
    }
    case 'SessionWindow': {
      const gap = toInterval(node.props.gap as string);
      return `SESSION(TABLE ${sourceRef}, DESCRIPTOR(${q(windowCol)}), ${gap})`;
    }
    default:
      return `UNKNOWN_WINDOW(TABLE ${sourceRef})`;
  }
}

// ── Query component ─────────────────────────────────────────────────

const QUERY_CLAUSE_TYPES = new Set([
  'Query.Select',
  'Query.Where',
  'Query.GroupBy',
  'Query.Having',
  'Query.OrderBy',
]);

function buildQueryComponentQuery(
  node: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
): string {
  // Separate clause children from upstream data children
  const clauses = node.children.filter((c) => QUERY_CLAUSE_TYPES.has(c.component));
  const upstreamChildren = node.children.filter((c) => !QUERY_CLAUSE_TYPES.has(c.component));

  // Resolve upstream
  const upstream = upstreamChildren.length > 0
    ? getUpstream({ children: upstreamChildren } as ConstructNode, nodeIndex)
    : { sql: 'SELECT * FROM unknown', sourceRef: 'unknown', isSimple: false };

  const fromClause = upstream.isSimple ? upstream.sourceRef : `(\n${upstream.sql}\n)`;

  // Find clause nodes
  const selectNode = clauses.find((c) => c.component === 'Query.Select');
  const whereNode = clauses.find((c) => c.component === 'Query.Where');
  const groupByNode = clauses.find((c) => c.component === 'Query.GroupBy');
  const havingNode = clauses.find((c) => c.component === 'Query.Having');
  const orderByNode = clauses.find((c) => c.component === 'Query.OrderBy');

  // Build SELECT projections
  const columns = selectNode!.props.columns as Record<string, string | { func: string; args?: readonly (string | number)[]; window?: string; over?: { partitionBy?: readonly string[]; orderBy?: Record<string, 'ASC' | 'DESC'> } }>;
  const windows = selectNode!.props.windows as Record<string, { partitionBy?: readonly string[]; orderBy?: Record<string, 'ASC' | 'DESC'> }> | undefined;

  const projections = Object.entries(columns)
    .map(([alias, expr]) => {
      if (typeof expr === 'string') {
        return `${expr} AS ${q(alias)}`;
      }
      return `${buildWindowFunctionExpr(expr)} AS ${q(alias)}`;
    })
    .join(', ');

  const parts: string[] = [`SELECT ${projections} FROM ${fromClause}`];

  // WHERE
  if (whereNode) {
    parts.push(`WHERE ${whereNode.props.condition as string}`);
  }

  // GROUP BY
  if (groupByNode) {
    const groupCols = (groupByNode.props.columns as readonly string[]).map(q).join(', ');
    parts.push(`GROUP BY ${groupCols}`);
  }

  // HAVING
  if (havingNode) {
    parts.push(`HAVING ${havingNode.props.condition as string}`);
  }

  // ORDER BY
  if (orderByNode) {
    const orderCols = orderByNode.props.columns as Record<string, 'ASC' | 'DESC'>;
    const orderClause = Object.entries(orderCols)
      .map(([col, dir]) => `${q(col)} ${dir}`)
      .join(', ');
    parts.push(`ORDER BY ${orderClause}`);
  }

  // WINDOW clause (named windows)
  if (windows && Object.keys(windows).length > 0) {
    const windowDefs = Object.entries(windows)
      .map(([name, spec]) => `${q(name)} AS (${buildWindowSpecSql(spec)})`)
      .join(', ');
    parts.push(`WINDOW ${windowDefs}`);
  }

  return parts.join('\n');
}

function buildWindowFunctionExpr(expr: {
  func: string;
  args?: readonly (string | number)[];
  window?: string;
  over?: { partitionBy?: readonly string[]; orderBy?: Record<string, 'ASC' | 'DESC'> };
}): string {
  const args = expr.args ? expr.args.join(', ') : '';
  const funcCall = `${expr.func}(${args})`;

  if (expr.window) {
    return `${funcCall} OVER ${q(expr.window)}`;
  }

  if (expr.over) {
    return `${funcCall} OVER (${buildWindowSpecSql(expr.over)})`;
  }

  return funcCall;
}

function buildWindowSpecSql(spec: {
  partitionBy?: readonly string[];
  orderBy?: Record<string, 'ASC' | 'DESC'>;
}): string {
  const parts: string[] = [];

  if (spec.partitionBy && spec.partitionBy.length > 0) {
    parts.push(`PARTITION BY ${spec.partitionBy.map(q).join(', ')}`);
  }

  if (spec.orderBy) {
    const orderClause = Object.entries(spec.orderBy)
      .map(([col, dir]) => `${q(col)} ${dir}`)
      .join(', ');
    parts.push(`ORDER BY ${orderClause}`);
  }

  return parts.join(' ');
}

// ── RawSQL ──────────────────────────────────────────────────────────

function buildRawSqlQuery(node: ConstructNode): string {
  const sql = node.props.sql as string;
  return sql;
}

// ── MatchRecognize (CEP) ────────────────────────────────────────────

function buildMatchRecognizeQuery(
  node: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
): string {
  const pattern = node.props.pattern as string;
  const define = node.props.define as Record<string, string>;
  const measures = node.props.measures as Record<string, string>;
  const after = node.props.after as string | undefined;
  const partitionBy = node.props.partitionBy as readonly string[] | undefined;
  const orderBy = node.props.orderBy as string | undefined;

  const upstream = getUpstream(node, nodeIndex);
  const fromClause = upstream.isSimple ? upstream.sourceRef : `(\n${upstream.sql}\n)`;

  const parts: string[] = [];

  if (partitionBy && partitionBy.length > 0) {
    parts.push(`  PARTITION BY ${partitionBy.map(q).join(', ')}`);
  }

  if (orderBy) {
    parts.push(`  ORDER BY ${q(orderBy)}`);
  }

  const measuresClause = Object.entries(measures)
    .map(([alias, expr]) => `    ${expr} AS ${q(alias)}`)
    .join(',\n');
  parts.push(`  MEASURES\n${measuresClause}`);

  if (after) {
    const strategy = after === 'NEXT_ROW'
      ? 'SKIP TO NEXT ROW'
      : 'SKIP PAST LAST ROW';
    parts.push(`  AFTER MATCH ${strategy}`);
  }

  parts.push(`  PATTERN (${pattern})`);

  const defineClause = Object.entries(define)
    .map(([variable, condition]) => `    ${variable} AS ${condition}`)
    .join(',\n');
  parts.push(`  DEFINE\n${defineClause}`);

  return `SELECT *\nFROM ${fromClause}\nMATCH_RECOGNIZE (\n${parts.join('\n')}\n)`;
}

// ── Side-path DML collector ──────────────────────────────────────────

/**
 * Recursively walk upstream nodes to find SideOutput/Validate nodes
 * that need to emit their side-path INSERT statements.
 */
function collectSideDml(
  node: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
  statements: string[],
): void {
  if (node.component === 'SideOutput') {
    collectSideOutputDml(node, nodeIndex, statements);
  } else if (node.component === 'Validate') {
    collectValidateDml(node, nodeIndex, statements);
  }

  // Continue recursing to find nested SideOutput/Validate
  for (const child of node.children) {
    collectSideDml(child, nodeIndex, statements);
  }
}

// ── SideOutput ──────────────────────────────────────────────────────

function buildSideOutputQuery(
  node: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
): string {
  const condition = node.props.condition as string;

  // Upstream is found from non-SideOutput.Sink children
  const upstreamChildren = node.children.filter(
    (c) => c.component !== 'SideOutput.Sink',
  );
  const upstream = upstreamChildren.length > 0
    ? getUpstream({ children: upstreamChildren } as ConstructNode, nodeIndex)
    : { sql: 'SELECT * FROM unknown', sourceRef: 'unknown', isSimple: false };

  if (upstream.isSimple) {
    return `SELECT * FROM ${upstream.sourceRef} WHERE NOT (${condition})`;
  }
  return `SELECT * FROM (\n${upstream.sql}\n) WHERE NOT (${condition})`;
}

function collectSideOutputDml(
  sideOutputNode: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
  statements: string[],
): void {
  const condition = sideOutputNode.props.condition as string;
  const tag = sideOutputNode.props.tag as string | undefined;

  // Find the SideOutput.Sink child and its sink
  const sideSinkWrapper = sideOutputNode.children.find(
    (c) => c.component === 'SideOutput.Sink',
  );

  // Find upstream children (not SideOutput.Sink)
  const upstreamChildren = sideOutputNode.children.filter(
    (c) => c.component !== 'SideOutput.Sink',
  );

  const upstream = upstreamChildren.length > 0
    ? getUpstream({ children: upstreamChildren } as ConstructNode, nodeIndex)
    : { sql: 'SELECT * FROM unknown', sourceRef: 'unknown', isSimple: false };

  const fromClause = upstream.isSimple ? upstream.sourceRef : `(\n${upstream.sql}\n)`;

  // Side path: matching records to the side sink
  if (sideSinkWrapper) {
    for (const child of sideSinkWrapper.children) {
      if (child.kind === 'Sink') {
        const sinkRef = resolveSinkRef(child);
        const metaCols: string[] = [];
        metaCols.push("CURRENT_TIMESTAMP AS `_side_ts`");
        if (tag) {
          metaCols.push(`'${tag}' AS \`_side_tag\``);
        }
        const selectList = metaCols.length > 0
          ? `*, ${metaCols.join(', ')}`
          : '*';
        statements.push(`INSERT INTO ${sinkRef}\nSELECT ${selectList} FROM ${fromClause} WHERE (${condition});`);
      }
    }
  }

  // Main path continues — handled by the parent collectSinkDml
  // traversal finding the enclosing sink above the SideOutput.
  // We don't emit the main path INSERT here because the parent
  // sink will call buildQuery on SideOutput, which emits
  // SELECT ... WHERE NOT (condition).
}

// ── Validate ────────────────────────────────────────────────────────

function buildValidateQuery(
  node: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
): string {
  const rules = node.props.rules as {
    notNull?: readonly string[];
    range?: Record<string, [number, number]>;
    expression?: Record<string, string>;
  };

  // Upstream is found from non-Validate.Reject children
  const upstreamChildren = node.children.filter(
    (c) => c.component !== 'Validate.Reject',
  );
  const upstream = upstreamChildren.length > 0
    ? getUpstream({ children: upstreamChildren } as ConstructNode, nodeIndex)
    : { sql: 'SELECT * FROM unknown', sourceRef: 'unknown', isSimple: false };

  const validCondition = buildValidateCondition(rules);
  const fromClause = upstream.isSimple ? upstream.sourceRef : `(\n${upstream.sql}\n)`;

  return `SELECT * FROM ${fromClause}\nWHERE ${validCondition}`;
}

function buildValidateCondition(rules: {
  notNull?: readonly string[];
  range?: Record<string, [number, number]>;
  expression?: Record<string, string>;
}): string {
  const conditions: string[] = [];

  if (rules.notNull) {
    for (const col of rules.notNull) {
      conditions.push(`${q(col)} IS NOT NULL`);
    }
  }

  if (rules.range) {
    for (const [col, [min, max]] of Object.entries(rules.range)) {
      conditions.push(`${q(col)} >= ${min} AND ${q(col)} <= ${max}`);
    }
  }

  if (rules.expression) {
    for (const expr of Object.values(rules.expression)) {
      conditions.push(expr);
    }
  }

  return conditions.join('\n    AND ');
}

function buildValidateErrorCase(rules: {
  notNull?: readonly string[];
  range?: Record<string, [number, number]>;
  expression?: Record<string, string>;
}): string {
  const cases: string[] = [];

  if (rules.notNull) {
    for (const col of rules.notNull) {
      cases.push(`WHEN ${q(col)} IS NULL THEN 'notNull:${col}'`);
    }
  }

  if (rules.range) {
    for (const [col, [min, max]] of Object.entries(rules.range)) {
      cases.push(`WHEN ${q(col)} < ${min} OR ${q(col)} > ${max} THEN 'range:${col}[${min},${max}]'`);
    }
  }

  if (rules.expression) {
    for (const [name, expr] of Object.entries(rules.expression)) {
      cases.push(`WHEN NOT (${expr}) THEN 'expression:${name}'`);
    }
  }

  return cases.map((c) => `      ${c}`).join('\n');
}

function collectValidateDml(
  validateNode: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
  statements: string[],
): void {
  const rules = validateNode.props.rules as {
    notNull?: readonly string[];
    range?: Record<string, [number, number]>;
    expression?: Record<string, string>;
  };

  // Find the Validate.Reject child and its sink
  const rejectWrapper = validateNode.children.find(
    (c) => c.component === 'Validate.Reject',
  );

  // Find upstream children (not Validate.Reject)
  const upstreamChildren = validateNode.children.filter(
    (c) => c.component !== 'Validate.Reject',
  );

  const upstream = upstreamChildren.length > 0
    ? getUpstream({ children: upstreamChildren } as ConstructNode, nodeIndex)
    : { sql: 'SELECT * FROM unknown', sourceRef: 'unknown', isSimple: false };

  const fromClause = upstream.isSimple ? upstream.sourceRef : `(\n${upstream.sql}\n)`;
  const validCondition = buildValidateCondition(rules);

  // Reject path: invalid records to the reject sink
  if (rejectWrapper) {
    for (const child of rejectWrapper.children) {
      if (child.kind === 'Sink') {
        const sinkRef = resolveSinkRef(child);
        const errorCase = buildValidateErrorCase(rules);
        statements.push(
          `INSERT INTO ${sinkRef}\nSELECT *,\n    CASE\n${errorCase}\n    END AS \`_validation_error\`,\n    CURRENT_TIMESTAMP AS \`_validated_at\`\nFROM ${fromClause}\nWHERE NOT (\n    ${validCondition}\n);`,
        );
      }
    }
  }

  // Valid path continues — handled by the parent collectSinkDml
  // traversal finding the enclosing sink above the Validate.
}

// ── View ────────────────────────────────────────────────────────────

function generateViewDdl(node: ConstructNode, nodeIndex: Map<string, ConstructNode>): string {
  const name = node.props.name as string;

  // The view's children define the upstream query
  const upstream = node.children.length > 0
    ? buildQuery(node.children[0], nodeIndex)
    : 'SELECT * FROM unknown';

  return `CREATE VIEW ${q(name)} AS\n${upstream};`;
}

// ── LateralJoin ─────────────────────────────────────────────────────

function buildLateralJoinQuery(
  node: ConstructNode,
  nodeIndex: Map<string, ConstructNode>,
): string {
  const funcName = node.props.function as string;
  const args = node.props.args as readonly (string | number)[];
  const asFields = node.props.as as Record<string, string>;
  const joinType = (node.props.type as string) ?? 'cross';
  const inputId = node.props.input as string;

  const inputRef = resolveRef(inputId, nodeIndex);
  const aliases = Object.keys(asFields).map(q).join(', ');
  const argList = args.map((a) => typeof a === 'string' ? a : String(a)).join(', ');

  const sqlJoinType = joinType === 'left' ? 'LEFT JOIN' : 'JOIN';

  return `SELECT ${inputRef}.*, T.${Object.keys(asFields).map(q).join(`, T.`)} FROM ${inputRef} ${sqlJoinType} LATERAL TABLE(${funcName}(${argList})) AS T(${aliases})`;
}

// ── Ref resolution ──────────────────────────────────────────────────

function resolveRef(nodeId: string, nodeIndex: Map<string, ConstructNode>): string {
  const node = nodeIndex.get(nodeId);
  if (!node) return q(nodeId);

  if (node.component === 'CatalogSource') {
    return `${q(String(node.props.catalogName))}.${q(String(node.props.database))}.${q(String(node.props.table))}`;
  }

  return q(nodeId);
}
