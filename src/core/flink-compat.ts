import type { FlinkMajorVersion } from "./types.js"

// ── Config key renames (1.20 legacy → 2.0+ canonical) ────────────────

/**
 * Maps canonical 2.0+ config keys to their deprecated 1.20 equivalents.
 * Only keys that changed between versions are listed.
 */
const CONFIG_KEY_RENAMES: ReadonlyMap<string, string> = new Map([
  ["execution.runtime-mode", "execution.runtime-mode"],
  ["table.exec.source.idle-timeout", "table.exec.source.idle-timeout"],
  ["pipeline.operator-chaining.enabled", "pipeline.operator-chaining"],
  ["restart-strategy.type", "restart-strategy"],
  ["execution.checkpointing.interval", "execution.checkpointing.interval"],
  ["execution.checkpointing.mode", "execution.checkpointing.mode"],
  ["state.backend.type", "state.backend"],
  ["state.checkpoints.dir", "state.checkpoints.dir"],
])

// ── Feature gating ───────────────────────────────────────────────────

interface GatedFeature {
  readonly name: string
  readonly minVersion: FlinkMajorVersion
  readonly description: string
}

const GATED_FEATURES: readonly GatedFeature[] = [
  {
    name: "CREATE_MODEL",
    minVersion: "2.1",
    description: "CREATE MODEL statement",
  },
  {
    name: "VECTOR_SEARCH",
    minVersion: "2.2",
    description: "VECTOR_SEARCH function",
  },
  {
    name: "MATERIALIZED_TABLE",
    minVersion: "2.0",
    description: "Materialized tables",
  },
  {
    name: "MATERIALIZED_TABLE_BUCKETING",
    minVersion: "2.2",
    description: "Materialized table bucketing",
  },
]

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

// ── JDBC connector structure ─────────────────────────────────────────

export interface JdbcConnectorInfo {
  readonly style: "single" | "modular"
  readonly jars: readonly string[]
}

// ── FlinkVersionCompat ───────────────────────────────────────────────

export interface FeatureGateError {
  readonly feature: string
  readonly requiredVersion: FlinkMajorVersion
  readonly currentVersion: FlinkMajorVersion
  readonly message: string
}

export const FlinkVersionCompat = {
  /**
   * Normalize config keys for the given Flink version.
   * In 1.20, some canonical 2.0+ keys are mapped to their legacy equivalents.
   */
  normalizeConfig(
    config: Record<string, string>,
    version: FlinkMajorVersion,
  ): Record<string, string> {
    if (versionGte(version, "2.0")) {
      return { ...config }
    }

    const result: Record<string, string> = {}
    for (const [key, value] of Object.entries(config)) {
      const legacyKey = CONFIG_KEY_RENAMES.get(key)
      result[legacyKey ?? key] = value
    }
    return result
  },

  /**
   * Check whether a named feature is supported on the given version.
   * Returns null if supported, or a FeatureGateError if not.
   */
  checkFeature(
    featureName: string,
    version: FlinkMajorVersion,
  ): FeatureGateError | null {
    const feature = GATED_FEATURES.find((f) => f.name === featureName)
    if (!feature) return null // unknown features are assumed supported

    if (versionGte(version, feature.minVersion)) return null

    return {
      feature: feature.name,
      requiredVersion: feature.minVersion,
      currentVersion: version,
      message: `${feature.description} requires Flink ${feature.minVersion} or later (current: ${version})`,
    }
  },

  /**
   * Resolve JDBC connector JARs for the given Flink version.
   * Flink 1.20 uses a single fat JAR; 2.0+ uses modular JARs.
   */
  resolveJdbcConnector(
    version: FlinkMajorVersion,
    dialect: string,
  ): JdbcConnectorInfo {
    if (!versionGte(version, "2.0")) {
      return {
        style: "single",
        jars: [`flink-connector-jdbc-3.2.0-1.20.jar`],
      }
    }

    return {
      style: "modular",
      jars: [
        `flink-connector-jdbc-3.2.0-${version}.jar`,
        `flink-connector-jdbc-${dialect}-3.2.0-${version}.jar`,
      ],
    }
  },

  /** List all gated features with their minimum versions */
  listGatedFeatures(): readonly GatedFeature[] {
    return GATED_FEATURES
  },

  /** Check if version A is >= version B */
  isVersionAtLeast(
    version: FlinkMajorVersion,
    minVersion: FlinkMajorVersion,
  ): boolean {
    return versionGte(version, minVersion)
  },
} as const
