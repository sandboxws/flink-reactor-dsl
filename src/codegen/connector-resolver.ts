import type { FlinkMajorVersion, ConstructNode } from '../core/types.js';
import {
  type MavenArtifact,
  resolveConnectorArtifacts,
  resolveJdbcDialectArtifacts,
  resolveFormatArtifacts,
  artifactToMavenUrl,
  artifactToJarName,
} from './connector-registry.js';

// ── Types ───────────────────────────────────────────────────────────

/** A resolved JAR with provenance tracking */
export interface ResolvedJar {
  readonly artifact: MavenArtifact;
  readonly jarName: string;
  readonly downloadUrl: string;
  /** Which component(s) required this JAR */
  readonly provenance: readonly string[];
}

/** Options for the connector resolver */
export interface ResolverOptions {
  readonly flinkVersion: FlinkMajorVersion;
  /** Override Maven base URL for air-gapped environments */
  readonly mavenMirror?: string;
  /** Additional user-provided connector artifacts */
  readonly customArtifacts?: readonly MavenArtifact[];
}

/** A connector usage collected from the construct tree */
interface ConnectorUsage {
  readonly connectorId: string;
  readonly format?: string;
  readonly jdbcUrl?: string;
  readonly sourceNodeId: string;
}

// ── Component → Connector mapping ───────────────────────────────────

const COMPONENT_CONNECTOR_MAP: ReadonlyMap<string, string> = new Map([
  ['KafkaSource', 'kafka'],
  ['KafkaSink', 'kafka'],
  ['JdbcSource', 'jdbc'],
  ['JdbcSink', 'jdbc'],
  ['FileSystemSink', 'filesystem'],
  ['GenericSource', '__generic'],
  ['GenericSink', '__generic'],
]);

// ── Tree walking ────────────────────────────────────────────────────

function collectUsages(node: ConstructNode): ConnectorUsage[] {
  const usages: ConnectorUsage[] = [];

  function walk(n: ConstructNode): void {
    const connectorId = COMPONENT_CONNECTOR_MAP.get(n.component);

    if (connectorId && connectorId !== '__generic') {
      const format = (n.props.format as string | undefined) ?? undefined;
      const jdbcUrl = (n.props.url as string | undefined) ?? undefined;
      usages.push({ connectorId, format, jdbcUrl, sourceNodeId: n.id });
    } else if (connectorId === '__generic') {
      // GenericSource/GenericSink: connector is specified in props
      const customConnector = n.props.connector as string | undefined;
      if (customConnector) {
        const format = (n.props.format as string | undefined) ?? undefined;
        usages.push({
          connectorId: customConnector,
          format,
          sourceNodeId: n.id,
        });
      }
    }

    for (const child of n.children) {
      walk(child);
    }
  }

  walk(node);
  return usages;
}

// ── De-duplication key ──────────────────────────────────────────────

function usageKey(usage: ConnectorUsage): string {
  const dialect = usage.jdbcUrl ? detectDialectFromUrl(usage.jdbcUrl) : '';
  return `${usage.connectorId}|${usage.format ?? ''}|${dialect}`;
}

function detectDialectFromUrl(url: string): string {
  if (url.startsWith('jdbc:mysql:')) return 'mysql';
  if (url.startsWith('jdbc:postgresql:')) return 'postgres';
  if (url.startsWith('jdbc:oracle:')) return 'oracle';
  if (url.startsWith('jdbc:sqlserver:')) return 'sqlserver';
  if (url.startsWith('jdbc:db2:')) return 'db2';
  return '';
}

// ── Artifact de-duplication ─────────────────────────────────────────

function artifactKey(a: MavenArtifact): string {
  return `${a.groupId}:${a.artifactId}:${a.version}`;
}

/** Detect version conflicts: same groupId:artifactId with different versions */
export interface VersionConflict {
  readonly groupId: string;
  readonly artifactId: string;
  readonly versions: readonly string[];
}

function detectConflicts(artifacts: readonly MavenArtifact[]): readonly VersionConflict[] {
  const byGa = new Map<string, Set<string>>();

  for (const a of artifacts) {
    const ga = `${a.groupId}:${a.artifactId}`;
    const existing = byGa.get(ga);
    if (existing) {
      existing.add(a.version);
    } else {
      byGa.set(ga, new Set([a.version]));
    }
  }

  const conflicts: VersionConflict[] = [];
  for (const [ga, versions] of byGa) {
    if (versions.size > 1) {
      const [groupId, artifactId] = ga.split(':');
      conflicts.push({
        groupId,
        artifactId,
        versions: [...versions].sort(),
      });
    }
  }

  return conflicts;
}

// ── Public API ──────────────────────────────────────────────────────

export interface ResolverResult {
  readonly jars: readonly ResolvedJar[];
  readonly conflicts: readonly VersionConflict[];
}

/**
 * Walk the construct tree rooted at `pipelineNode`, collect all connector
 * and format usages, de-duplicate, and resolve to Maven JAR coordinates.
 */
export function resolveConnectors(
  pipelineNode: ConstructNode,
  options: ResolverOptions,
): ResolverResult {
  const { flinkVersion, mavenMirror, customArtifacts } = options;
  const mavenBase = mavenMirror ?? 'https://repo1.maven.org/maven2';

  // 1. Collect all connector usages from the tree
  const usages = collectUsages(pipelineNode);

  // 2. De-duplicate by (connectorId, format, dialect)
  const deduped = new Map<string, { usage: ConnectorUsage; nodeIds: string[] }>();
  for (const usage of usages) {
    const key = usageKey(usage);
    const existing = deduped.get(key);
    if (existing) {
      existing.nodeIds.push(usage.sourceNodeId);
    } else {
      deduped.set(key, { usage, nodeIds: [usage.sourceNodeId] });
    }
  }

  // 3. Resolve each unique usage to Maven artifacts
  const allArtifacts: MavenArtifact[] = [];
  const provenanceMap = new Map<string, string[]>();

  function trackArtifact(artifact: MavenArtifact, nodeIds: readonly string[]): void {
    const key = artifactKey(artifact);
    allArtifacts.push(artifact);
    const existing = provenanceMap.get(key);
    if (existing) {
      for (const id of nodeIds) {
        if (!existing.includes(id)) existing.push(id);
      }
    } else {
      provenanceMap.set(key, [...nodeIds]);
    }
  }

  for (const { usage, nodeIds } of deduped.values()) {
    // Resolve connector artifacts
    const connectorArtifacts = resolveConnectorArtifacts(usage.connectorId, flinkVersion);
    for (const a of connectorArtifacts) {
      trackArtifact(a, nodeIds);
    }

    // Resolve JDBC dialect artifacts if applicable
    if (usage.connectorId === 'jdbc' && usage.jdbcUrl) {
      const dialectArtifacts = resolveJdbcDialectArtifacts(usage.jdbcUrl, flinkVersion);
      for (const a of dialectArtifacts) {
        trackArtifact(a, nodeIds);
      }
    }

    // Resolve format artifacts
    if (usage.format) {
      const formatArtifacts = resolveFormatArtifacts(usage.format);
      for (const a of formatArtifacts) {
        trackArtifact(a, nodeIds);
      }
    }
  }

  // Add custom artifacts
  if (customArtifacts) {
    for (const a of customArtifacts) {
      trackArtifact(a, ['user-config']);
    }
  }

  // 4. De-duplicate artifacts and build final ResolvedJar list
  const seen = new Set<string>();
  const jars: ResolvedJar[] = [];

  for (const artifact of allArtifacts) {
    const key = artifactKey(artifact);
    if (seen.has(key)) continue;
    seen.add(key);

    jars.push({
      artifact,
      jarName: artifactToJarName(artifact),
      downloadUrl: artifactToMavenUrl(artifact, mavenBase),
      provenance: provenanceMap.get(key) ?? [],
    });
  }

  // 5. Detect version conflicts
  const conflicts = detectConflicts(allArtifacts);

  return { jars, conflicts };
}
