import type { FlinkMajorVersion } from "@/core/types.js"

// ── Types ───────────────────────────────────────────────────────────

/** A single Maven artifact coordinate */
export interface MavenArtifact {
  readonly groupId: string
  readonly artifactId: string
  readonly version: string
}

/** Artifacts resolved for a specific Flink major version range */
export interface VersionedArtifacts {
  /** Minimum Flink version (inclusive) this entry applies to */
  readonly minVersion: FlinkMajorVersion
  /** Maximum Flink version (inclusive), undefined means "latest" */
  readonly maxVersion?: FlinkMajorVersion
  readonly artifacts: readonly MavenArtifact[]
}

/** A built-in connector has no external JARs */
export interface ConnectorRegistryEntry {
  readonly connectorId: string
  /** If true, no JARs are needed (e.g., FileSystem) */
  readonly builtIn: boolean
  readonly versions: readonly VersionedArtifacts[]
  /**
   * Synthesis-branch affinity. When set, the connector is only resolved on
   * the matching branch:
   *   • `"sql"` — Flink-SQL connectors (e.g. `fluss-connector-flink`).
   *   • `"pipeline-yaml"` — Flink CDC 3.6 Pipeline Connectors.
   * When undefined the connector applies on either branch.
   */
  readonly branchAffinity?: "sql" | "pipeline-yaml"
}

/** JDBC dialect entry mapping database type to dialect module + driver JAR */
export interface JdbcDialectEntry {
  readonly dialect: string
  /** URL prefix pattern used to detect this dialect */
  readonly urlPattern: string
  /** Flink dialect module (2.0+ only, not used for 1.20 single JAR) */
  readonly dialectArtifact: (version: FlinkMajorVersion) => MavenArtifact
  /** Vendor JDBC driver JAR */
  readonly driverArtifact: MavenArtifact
}

/** Format dependency entry */
export interface FormatEntry {
  readonly formatId: string
  /** If true, no extra JARs needed (json, csv) */
  readonly builtIn: boolean
  /** Artifact coordinates split by Flink major version range. */
  readonly versions: readonly VersionedArtifacts[]
}

// ── Version helpers ─────────────────────────────────────────────────

const VERSION_ORDER: readonly FlinkMajorVersion[] = [
  "1.20",
  "2.0",
  "2.1",
  "2.2",
]

function versionIndex(v: FlinkMajorVersion): number {
  return VERSION_ORDER.indexOf(v)
}

function versionGte(a: FlinkMajorVersion, b: FlinkMajorVersion): boolean {
  return versionIndex(a) >= versionIndex(b)
}

function versionLte(a: FlinkMajorVersion, b: FlinkMajorVersion): boolean {
  return versionIndex(a) <= versionIndex(b)
}

// ── Connector Registry ──────────────────────────────────────────────

const CONNECTOR_REGISTRY: readonly ConnectorRegistryEntry[] = [
  // Kafka: SQL fat JAR
  {
    connectorId: "kafka",
    builtIn: false,
    versions: [
      {
        minVersion: "1.20",
        maxVersion: "1.20",
        artifacts: [
          {
            groupId: "org.apache.flink",
            artifactId: "flink-sql-connector-kafka",
            version: "3.3.0-1.20",
          },
        ],
      },
      {
        minVersion: "2.0",
        artifacts: [
          {
            groupId: "org.apache.flink",
            artifactId: "flink-sql-connector-kafka",
            version: "4.0.1-2.0",
          },
        ],
      },
    ],
  },

  // JDBC: single JAR for 1.20, modular core for 2.0+
  {
    connectorId: "jdbc",
    builtIn: false,
    versions: [
      {
        minVersion: "1.20",
        maxVersion: "1.20",
        artifacts: [
          {
            groupId: "org.apache.flink",
            artifactId: "flink-connector-jdbc",
            version: "3.2.0-1.20",
          },
        ],
      },
      {
        minVersion: "2.0",
        artifacts: [
          {
            groupId: "org.apache.flink",
            artifactId: "flink-connector-jdbc-core",
            version: "3.2.0-2.0",
          },
        ],
      },
    ],
  },

  // Elasticsearch 7
  {
    connectorId: "elasticsearch-7",
    builtIn: false,
    versions: [
      {
        minVersion: "1.20",
        maxVersion: "1.20",
        artifacts: [
          {
            groupId: "org.apache.flink",
            artifactId: "flink-sql-connector-elasticsearch7",
            version: "4.0.0-1.20",
          },
        ],
      },
      {
        minVersion: "2.0",
        artifacts: [
          {
            groupId: "org.apache.flink",
            artifactId: "flink-sql-connector-elasticsearch7",
            version: "4.0.0-2.0",
          },
        ],
      },
    ],
  },

  // FileSystem: built-in, no extra JARs
  {
    connectorId: "filesystem",
    builtIn: true,
    versions: [],
  },

  // Postgres CDC Pipeline Connector (Flink CDC 3.6, version-independent of
  // Flink core — the same artifact covers both the Flink 1.20 and 2.x ranges)
  {
    connectorId: "postgres-cdc-pipeline",
    builtIn: false,
    branchAffinity: "pipeline-yaml",
    versions: [
      {
        minVersion: "1.20",
        artifacts: [
          {
            groupId: "org.apache.flink",
            artifactId: "flink-cdc-pipeline-connector-postgres",
            version: "3.6.0",
          },
        ],
      },
    ],
  },

  // Fluss CDC Pipeline Connector (Flink CDC 3.6). Distinct from the Flink-SQL
  // Fluss connector below — Pipeline Connectors and SQL connectors are
  // separate artifacts under upstream Flink CDC's release model. Single
  // version range covers both Flink 1.20 and Flink 2.x because the Pipeline
  // Connector versions independently of Flink core.
  {
    connectorId: "fluss-cdc-pipeline",
    builtIn: false,
    branchAffinity: "pipeline-yaml",
    versions: [
      {
        minVersion: "1.20",
        artifacts: [
          {
            groupId: "org.apache.flink",
            artifactId: "flink-cdc-pipeline-connector-fluss",
            version: "3.6.0",
          },
        ],
      },
    ],
  },

  // Apache Iceberg Flink runtime. The iceberg-flink-runtime artifact is
  // versioned against the Flink minor (1.20 → iceberg-flink-runtime-1.20;
  // 2.x → iceberg-flink-runtime-2.0). Pinned to Iceberg 1.6.0 — the first
  // release that ships the Flink 2.0 runtime alongside 1.20.
  {
    connectorId: "iceberg",
    builtIn: false,
    versions: [
      {
        minVersion: "1.20",
        maxVersion: "1.20",
        artifacts: [
          {
            groupId: "org.apache.iceberg",
            artifactId: "iceberg-flink-runtime-1.20",
            version: "1.6.0",
          },
        ],
      },
      {
        minVersion: "2.0",
        artifacts: [
          {
            groupId: "org.apache.iceberg",
            artifactId: "iceberg-flink-runtime-2.0",
            version: "1.6.0",
          },
        ],
      },
    ],
  },

  // Apache Fluss Flink connector. Single artifact spans the full Flink
  // 1.20 → 2.2 range under the Apache Fluss 0.9.0-incubating release.
  // SQL-branch only — the Pipeline-YAML branch uses `fluss-cdc-pipeline`
  // instead.
  //
  // Apache groupId migration: Fluss became an Apache Incubator project in
  // June 2025 and the canonical artifact moved from `com.alibaba.fluss` to
  // `org.apache.fluss`. See https://fluss.apache.org/ for upstream context.
  {
    connectorId: "fluss",
    builtIn: false,
    branchAffinity: "sql",
    versions: [
      {
        minVersion: "1.20",
        artifacts: [
          {
            groupId: "org.apache.fluss",
            artifactId: "fluss-connector-flink",
            version: "0.9.0-incubating",
          },
        ],
      },
    ],
  },

  // Apache Paimon Flink connector. Mirrors the Iceberg pattern: one artifact
  // per Flink major (1.20 → paimon-flink-1.20; 2.x → paimon-flink-2.0). Pinned
  // to Paimon 1.0.0 — the first release that ships the Flink 2.0 runtime
  // alongside 1.20. See https://paimon.apache.org/.
  {
    connectorId: "paimon",
    builtIn: false,
    versions: [
      {
        minVersion: "1.20",
        maxVersion: "1.20",
        artifacts: [
          {
            groupId: "org.apache.paimon",
            artifactId: "paimon-flink-1.20",
            version: "1.0.0",
          },
        ],
      },
      {
        minVersion: "2.0",
        artifacts: [
          {
            groupId: "org.apache.paimon",
            artifactId: "paimon-flink-2.0",
            version: "1.0.0",
          },
        ],
      },
    ],
  },
]

// ── JDBC Dialect Sub-Registry ───────────────────────────────────────

const JDBC_DIALECT_REGISTRY: readonly JdbcDialectEntry[] = [
  {
    dialect: "mysql",
    urlPattern: "jdbc:mysql:",
    dialectArtifact: (v) => ({
      groupId: "org.apache.flink",
      artifactId: "flink-connector-jdbc-mysql",
      version: `3.2.0-${versionGte(v, "2.0") ? "2.0" : v}`,
    }),
    driverArtifact: {
      groupId: "com.mysql",
      artifactId: "mysql-connector-j",
      version: "8.3.0",
    },
  },
  {
    dialect: "postgres",
    urlPattern: "jdbc:postgresql:",
    dialectArtifact: (v) => ({
      groupId: "org.apache.flink",
      artifactId: "flink-connector-jdbc-postgres",
      version: `3.2.0-${versionGte(v, "2.0") ? "2.0" : v}`,
    }),
    driverArtifact: {
      groupId: "org.postgresql",
      artifactId: "postgresql",
      version: "42.7.3",
    },
  },
  {
    dialect: "oracle",
    urlPattern: "jdbc:oracle:",
    dialectArtifact: (v) => ({
      groupId: "org.apache.flink",
      artifactId: "flink-connector-jdbc-oracle",
      version: `3.2.0-${versionGte(v, "2.0") ? "2.0" : v}`,
    }),
    driverArtifact: {
      groupId: "com.oracle.database.jdbc",
      artifactId: "ojdbc11",
      version: "23.3.0.23.09",
    },
  },
  {
    dialect: "sqlserver",
    urlPattern: "jdbc:sqlserver:",
    dialectArtifact: (v) => ({
      groupId: "org.apache.flink",
      artifactId: "flink-connector-jdbc-sqlserver",
      version: `3.2.0-${versionGte(v, "2.0") ? "2.0" : v}`,
    }),
    driverArtifact: {
      groupId: "com.microsoft.sqlserver",
      artifactId: "mssql-jdbc",
      version: "12.6.1.jre11",
    },
  },
  {
    dialect: "db2",
    urlPattern: "jdbc:db2:",
    dialectArtifact: (v) => ({
      groupId: "org.apache.flink",
      artifactId: "flink-connector-jdbc-db2",
      version: `3.2.0-${versionGte(v, "2.0") ? "2.0" : v}`,
    }),
    driverArtifact: {
      groupId: "com.ibm.db2",
      artifactId: "jcc",
      version: "11.5.9.0",
    },
  },
]

// ── Format Dependencies ─────────────────────────────────────────────

const FORMAT_REGISTRY: readonly FormatEntry[] = [
  {
    formatId: "json",
    builtIn: true,
    versions: [],
  },
  {
    formatId: "csv",
    builtIn: true,
    versions: [],
  },
  {
    formatId: "avro",
    builtIn: false,
    versions: [
      {
        minVersion: "1.20",
        maxVersion: "1.20",
        artifacts: [
          {
            groupId: "org.apache.flink",
            artifactId: "flink-sql-avro",
            version: "1.20.0",
          },
        ],
      },
      {
        minVersion: "2.0",
        artifacts: [
          {
            groupId: "org.apache.flink",
            artifactId: "flink-sql-avro",
            version: "2.0.0",
          },
        ],
      },
    ],
  },
  {
    formatId: "parquet",
    builtIn: false,
    versions: [
      {
        minVersion: "1.20",
        maxVersion: "1.20",
        artifacts: [
          {
            groupId: "org.apache.flink",
            artifactId: "flink-sql-parquet",
            version: "1.20.0",
          },
        ],
      },
      {
        minVersion: "2.0",
        artifacts: [
          {
            groupId: "org.apache.flink",
            artifactId: "flink-sql-parquet",
            version: "2.0.0",
          },
        ],
      },
    ],
  },
  // CDC formats that ship with the Kafka connector (no extra jars)
  {
    formatId: "debezium-json",
    builtIn: true,
    versions: [],
  },
  {
    formatId: "canal-json",
    builtIn: true,
    versions: [],
  },
  {
    formatId: "maxwell-json",
    builtIn: true,
    versions: [],
  },
  {
    formatId: "debezium-avro",
    builtIn: false,
    versions: [
      {
        minVersion: "1.20",
        maxVersion: "1.20",
        artifacts: [
          {
            groupId: "org.apache.flink",
            artifactId: "flink-sql-avro-confluent-registry",
            version: "1.20.0",
          },
        ],
      },
      {
        minVersion: "2.0",
        artifacts: [
          {
            groupId: "org.apache.flink",
            artifactId: "flink-sql-avro-confluent-registry",
            version: "2.0.0",
          },
        ],
      },
    ],
  },
  {
    formatId: "debezium-protobuf",
    builtIn: false,
    versions: [
      {
        minVersion: "1.20",
        maxVersion: "1.20",
        artifacts: [
          {
            groupId: "org.apache.flink",
            artifactId: "flink-sql-protobuf-confluent-registry",
            version: "1.20.0",
          },
        ],
      },
      {
        minVersion: "2.0",
        artifacts: [
          {
            groupId: "org.apache.flink",
            artifactId: "flink-sql-protobuf-confluent-registry",
            version: "2.0.0",
          },
        ],
      },
    ],
  },
]

// ── Public API ──────────────────────────────────────────────────────

/**
 * Look up a connector by its identifier.
 * Returns undefined if the connector is not in the registry.
 */
export function lookupConnector(
  connectorId: string,
): ConnectorRegistryEntry | undefined {
  return CONNECTOR_REGISTRY.find((e) => e.connectorId === connectorId)
}

/**
 * Resolve Maven artifacts for a connector at a specific Flink version.
 * Returns an empty array for built-in connectors or unknown connectors.
 */
export function resolveConnectorArtifacts(
  connectorId: string,
  flinkVersion: FlinkMajorVersion,
): readonly MavenArtifact[] {
  const entry = lookupConnector(connectorId)
  if (!entry || entry.builtIn) return []

  for (const v of entry.versions) {
    const meetsMin = versionGte(flinkVersion, v.minVersion)
    const meetsMax =
      v.maxVersion === undefined || versionLte(flinkVersion, v.maxVersion)
    if (meetsMin && meetsMax) {
      return v.artifacts
    }
  }

  return []
}

/**
 * Detect the JDBC dialect from a JDBC URL string.
 * Returns undefined if no matching dialect is found.
 */
export function detectJdbcDialect(
  jdbcUrl: string,
): JdbcDialectEntry | undefined {
  return JDBC_DIALECT_REGISTRY.find((d) => jdbcUrl.startsWith(d.urlPattern))
}

/**
 * Resolve JDBC dialect artifacts (dialect module + driver) for a Flink version.
 * For Flink 1.20 (single JAR mode), only the driver is returned
 * since the dialect is bundled in the single connector JAR.
 */
export function resolveJdbcDialectArtifacts(
  jdbcUrl: string,
  flinkVersion: FlinkMajorVersion,
): readonly MavenArtifact[] {
  const dialect = detectJdbcDialect(jdbcUrl)
  if (!dialect) return []

  if (!versionGte(flinkVersion, "2.0")) {
    // 1.20: single fat JAR includes all dialects, only need the driver
    return [dialect.driverArtifact]
  }

  // 2.0+: modular → dialect module + driver
  return [dialect.dialectArtifact(flinkVersion), dialect.driverArtifact]
}

/**
 * Resolve format dependencies. Returns artifacts needed for the given format
 * at the target Flink major version. Built-in formats (json, csv, debezium-json)
 * return an empty array. Unknown formats or Flink versions outside every
 * registered range also return an empty array.
 */
export function resolveFormatArtifacts(
  formatId: string,
  flinkVersion: FlinkMajorVersion,
): readonly MavenArtifact[] {
  const entry = FORMAT_REGISTRY.find((f) => f.formatId === formatId)
  if (!entry || entry.builtIn) return []

  for (const v of entry.versions) {
    const meetsMin = versionGte(flinkVersion, v.minVersion)
    const meetsMax =
      v.maxVersion === undefined || versionLte(flinkVersion, v.maxVersion)
    if (meetsMin && meetsMax) {
      return v.artifacts
    }
  }

  return []
}

/**
 * Convert a Maven artifact to a standard Maven Central URL path.
 */
export function artifactToMavenUrl(
  artifact: MavenArtifact,
  baseUrl: string = "https://repo1.maven.org/maven2",
): string {
  const groupPath = artifact.groupId.replace(/\./g, "/")
  const jarName = `${artifact.artifactId}-${artifact.version}.jar`
  return `${baseUrl}/${groupPath}/${artifact.artifactId}/${artifact.version}/${jarName}`
}

/**
 * Convert a Maven artifact to a JAR filename.
 */
export function artifactToJarName(artifact: MavenArtifact): string {
  return `${artifact.artifactId}-${artifact.version}.jar`
}
