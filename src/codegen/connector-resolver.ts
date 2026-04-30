import type { ConstructNode, FlinkMajorVersion } from "@/core/types.js"
import {
  artifactToJarName,
  artifactToMavenUrl,
  lookupConnector,
  type MavenArtifact,
  resolveConnectorArtifacts,
  resolveFormatArtifacts,
  resolveJdbcDialectArtifacts,
} from "./connector-registry.js"
import { hasPipelineConnectorSource } from "./sql-generator.js"

// ── Types ───────────────────────────────────────────────────────────

/** A resolved JAR with provenance tracking */
export interface ResolvedJar {
  readonly artifact: MavenArtifact
  readonly jarName: string
  readonly downloadUrl: string
  /** Which component(s) required this JAR */
  readonly provenance: readonly string[]
}

/** Options for the connector resolver */
export interface ResolverOptions {
  readonly flinkVersion: FlinkMajorVersion
  /** Override Maven base URL for air-gapped environments */
  readonly mavenMirror?: string
  /** Additional user-provided connector artifacts */
  readonly customArtifacts?: readonly MavenArtifact[]
}

/** A connector usage collected from the construct tree */
interface ConnectorUsage {
  readonly connectorId: string
  readonly format?: string
  readonly jdbcUrl?: string
  readonly sourceNodeId: string
}

// ── Component → Connector mapping ───────────────────────────────────
//
// Components that participate on a single synthesis branch map to a single
// connector ID. Components that exist on both branches (today only
// `FlussSink`) list both connector IDs; the resolver filters by the active
// branch using `branchAffinity` from the registry.

const COMPONENT_CONNECTOR_MAP: ReadonlyMap<string, readonly string[]> = new Map(
  [
    ["KafkaSource", ["kafka"]],
    ["KafkaSink", ["kafka"]],
    ["JdbcSource", ["jdbc"]],
    ["JdbcSink", ["jdbc"]],
    ["FileSystemSink", ["filesystem"]],
    ["GenericSource", ["__generic"]],
    ["GenericSink", ["__generic"]],
    ["PostgresCdcPipelineSource", ["postgres-cdc-pipeline"]],
    ["IcebergSink", ["iceberg"]],
    // FlussSink renders on both branches: the Flink-SQL connector for the
    // SQL branch and the Pipeline Connector for the Pipeline-YAML branch.
    // Resolution selects the active artifact via `branchAffinity`.
    ["FlussSink", ["fluss", "fluss-cdc-pipeline"]],
    ["FlussSource", ["fluss"]],
  ],
)

// ── Tree walking ────────────────────────────────────────────────────

function collectUsages(
  node: ConstructNode,
  isPipelineYamlBranch: boolean,
): ConnectorUsage[] {
  const usages: ConnectorUsage[] = []
  const activeBranch: "sql" | "pipeline-yaml" = isPipelineYamlBranch
    ? "pipeline-yaml"
    : "sql"

  function pushIfBranchMatches(
    connectorId: string,
    format: string | undefined,
    jdbcUrl: string | undefined,
    sourceNodeId: string,
  ): void {
    const entry = lookupConnector(connectorId)
    if (entry?.branchAffinity && entry.branchAffinity !== activeBranch) {
      return
    }
    usages.push({ connectorId, format, jdbcUrl, sourceNodeId })
  }

  function walk(n: ConstructNode): void {
    const connectorIds = COMPONENT_CONNECTOR_MAP.get(n.component)

    if (connectorIds) {
      const format = (n.props.format as string | undefined) ?? undefined
      const jdbcUrl = (n.props.url as string | undefined) ?? undefined
      for (const connectorId of connectorIds) {
        if (connectorId === "__generic") {
          // GenericSource/GenericSink: connector is specified in props
          const customConnector = n.props.connector as string | undefined
          if (customConnector) {
            pushIfBranchMatches(customConnector, format, undefined, n.id)
          }
        } else {
          pushIfBranchMatches(connectorId, format, jdbcUrl, n.id)
        }
      }
    }

    for (const child of n.children) {
      walk(child)
    }
  }

  walk(node)
  return usages
}

// ── UDF JAR collection ──────────────────────────────────────────────

interface UdfJarUsage {
  readonly jarPath: string
  readonly nodeId: string
}

function collectUdfJars(node: ConstructNode): UdfJarUsage[] {
  const usages: UdfJarUsage[] = []

  function walk(n: ConstructNode): void {
    if (n.component === "UDF" && n.props.jarPath) {
      usages.push({
        jarPath: n.props.jarPath as string,
        nodeId: n.id,
      })
    }
    for (const child of n.children) {
      walk(child)
    }
  }

  walk(node)
  return usages
}

// ── De-duplication key ──────────────────────────────────────────────

function usageKey(usage: ConnectorUsage): string {
  const dialect = usage.jdbcUrl ? detectDialectFromUrl(usage.jdbcUrl) : ""
  return `${usage.connectorId}|${usage.format ?? ""}|${dialect}`
}

function detectDialectFromUrl(url: string): string {
  if (url.startsWith("jdbc:mysql:")) return "mysql"
  if (url.startsWith("jdbc:postgresql:")) return "postgres"
  if (url.startsWith("jdbc:oracle:")) return "oracle"
  if (url.startsWith("jdbc:sqlserver:")) return "sqlserver"
  if (url.startsWith("jdbc:db2:")) return "db2"
  return ""
}

// ── Artifact de-duplication ─────────────────────────────────────────

function artifactKey(a: MavenArtifact): string {
  return `${a.groupId}:${a.artifactId}:${a.version}`
}

/** Detect version conflicts: same groupId:artifactId with different versions */
export interface VersionConflict {
  readonly groupId: string
  readonly artifactId: string
  readonly versions: readonly string[]
}

function detectConflicts(
  artifacts: readonly MavenArtifact[],
): readonly VersionConflict[] {
  const byGa = new Map<string, Set<string>>()

  for (const a of artifacts) {
    const ga = `${a.groupId}:${a.artifactId}`
    const existing = byGa.get(ga)
    if (existing) {
      existing.add(a.version)
    } else {
      byGa.set(ga, new Set([a.version]))
    }
  }

  const conflicts: VersionConflict[] = []
  for (const [ga, versions] of byGa) {
    if (versions.size > 1) {
      const [groupId, artifactId] = ga.split(":")
      conflicts.push({
        groupId,
        artifactId,
        versions: [...versions].sort(),
      })
    }
  }

  return conflicts
}

// ── Public API ──────────────────────────────────────────────────────

export interface ResolverResult {
  readonly jars: readonly ResolvedJar[]
  readonly conflicts: readonly VersionConflict[]
}

/**
 * Walk the construct tree rooted at `pipelineNode`, collect all connector
 * and format usages, de-duplicate, and resolve to Maven JAR coordinates.
 */
export function resolveConnectors(
  pipelineNode: ConstructNode,
  options: ResolverOptions,
): ResolverResult {
  const { flinkVersion, mavenMirror, customArtifacts } = options
  const mavenBase = mavenMirror ?? "https://repo1.maven.org/maven2"

  // 1. Collect all connector usages from the tree. The synthesis branch is
  //    determined by whether the tree contains a Pipeline Connector source —
  //    when it does, only `pipeline-yaml` and unscoped connectors resolve;
  //    otherwise, only `sql` and unscoped connectors resolve.
  const isPipelineYamlBranch = hasPipelineConnectorSource(pipelineNode)
  const usages = collectUsages(pipelineNode, isPipelineYamlBranch)

  // 2. De-duplicate by (connectorId, format, dialect)
  const deduped = new Map<
    string,
    { usage: ConnectorUsage; nodeIds: string[] }
  >()
  for (const usage of usages) {
    const key = usageKey(usage)
    const existing = deduped.get(key)
    if (existing) {
      existing.nodeIds.push(usage.sourceNodeId)
    } else {
      deduped.set(key, { usage, nodeIds: [usage.sourceNodeId] })
    }
  }

  // 3. Resolve each unique usage to Maven artifacts
  const allArtifacts: MavenArtifact[] = []
  const provenanceMap = new Map<string, string[]>()

  function trackArtifact(
    artifact: MavenArtifact,
    nodeIds: readonly string[],
  ): void {
    const key = artifactKey(artifact)
    allArtifacts.push(artifact)
    const existing = provenanceMap.get(key)
    if (existing) {
      for (const id of nodeIds) {
        if (!existing.includes(id)) existing.push(id)
      }
    } else {
      provenanceMap.set(key, [...nodeIds])
    }
  }

  for (const { usage, nodeIds } of deduped.values()) {
    // Resolve connector artifacts
    const connectorArtifacts = resolveConnectorArtifacts(
      usage.connectorId,
      flinkVersion,
    )
    for (const a of connectorArtifacts) {
      trackArtifact(a, nodeIds)
    }

    // Resolve JDBC dialect artifacts if applicable
    if (usage.connectorId === "jdbc" && usage.jdbcUrl) {
      const dialectArtifacts = resolveJdbcDialectArtifacts(
        usage.jdbcUrl,
        flinkVersion,
      )
      for (const a of dialectArtifacts) {
        trackArtifact(a, nodeIds)
      }
    }

    // Resolve format artifacts
    if (usage.format) {
      const formatArtifacts = resolveFormatArtifacts(usage.format, flinkVersion)
      for (const a of formatArtifacts) {
        trackArtifact(a, nodeIds)
      }
    }
  }

  // Add custom artifacts
  if (customArtifacts) {
    for (const a of customArtifacts) {
      trackArtifact(a, ["user-config"])
    }
  }

  // 3.5. Collect UDF JARs from the tree
  const udfJars = collectUdfJars(pipelineNode)
  for (const udf of udfJars) {
    const jarName = udf.jarPath.split("/").pop() ?? udf.jarPath
    const udfArtifact: MavenArtifact = {
      groupId: "local",
      artifactId: jarName.replace(/\.jar$/, ""),
      version: "local",
    }
    trackArtifact(udfArtifact, [udf.nodeId])
  }

  // 4. De-duplicate artifacts and build final ResolvedJar list
  const seen = new Set<string>()
  const jars: ResolvedJar[] = []

  // Also index UDF local JARs by path for download URL resolution
  const udfJarPaths = new Map<string, string>()
  for (const udf of udfJars) {
    const jarName = udf.jarPath.split("/").pop() ?? udf.jarPath
    const artId = jarName.replace(/\.jar$/, "")
    udfJarPaths.set(`local:${artId}:local`, udf.jarPath)
  }

  for (const artifact of allArtifacts) {
    const key = artifactKey(artifact)
    if (seen.has(key)) continue
    seen.add(key)

    const localPath = udfJarPaths.get(key)
    jars.push({
      artifact,
      jarName: localPath
        ? (localPath.split("/").pop() ?? artifactToJarName(artifact))
        : artifactToJarName(artifact),
      downloadUrl: localPath
        ? `file://${localPath}`
        : artifactToMavenUrl(artifact, mavenBase),
      provenance: provenanceMap.get(key) ?? [],
    })
  }

  // 5. Detect version conflicts
  const conflicts = detectConflicts(allArtifacts)

  return { jars, conflicts }
}
