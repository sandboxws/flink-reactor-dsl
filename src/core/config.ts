import type { EnvVarRef } from "./env-var.js"
import type { FlinkReactorPlugin } from "./plugin.js"
import type { FlinkMajorVersion } from "./types.js"

// ── Connector configuration ──────────────────────────────────────────

export type DeliveryStrategy = "init-container" | "custom-image"

export interface ConnectorConfig {
  readonly delivery?: DeliveryStrategy
  readonly mavenMirrors?: readonly string[]
  readonly custom?: ReadonlyArray<{
    readonly name: string
    readonly groupId: string
    readonly artifactId: string
    readonly version: string
  }>
  readonly versionOverrides?: Record<string, string>
}

// ── InfraConfig ──────────────────────────────────────────────────────

export interface InfraConfig {
  readonly kafka?: {
    readonly bootstrapServers?: string
  }
  readonly kubernetes?: {
    readonly namespace?: string
    readonly image?: string
  }
  readonly connectors?: ConnectorConfig
  readonly flink?: Record<string, string>
}

// ── Pipeline overrides ──────────────────────────────────────────────

export interface PipelineOverrides {
  readonly parallelism?: number
  readonly [key: string]: unknown
}

// ── Cluster configuration ───────────────────────────────────────────

export interface ClusterConfig {
  readonly url?: string | EnvVarRef
  readonly displayName?: string
}

// ── Dashboard configuration sections ────────────────────────────────

export interface DashboardAuthConfig {
  readonly type?: "none" | "basic" | "token"
  readonly username?: string | EnvVarRef
  readonly password?: string | EnvVarRef
  readonly token?: string | EnvVarRef
}

export interface DashboardSslConfig {
  readonly enabled?: boolean
  readonly caPath?: string | EnvVarRef
}

export interface DashboardRbacConfig {
  readonly enabled?: boolean
  readonly provider?: "basic" | "oidc"
  readonly roles?: Record<string, string[]>
}

export interface DashboardObservabilityConfig {
  readonly prometheus?: string | EnvVarRef
  readonly alertWebhook?: string | EnvVarRef
}

export interface DashboardSection {
  readonly port?: number
  readonly pollIntervalMs?: number
  readonly logBufferSize?: number
  readonly mockMode?: boolean
  readonly auth?: DashboardAuthConfig
  readonly ssl?: DashboardSslConfig
  readonly rbac?: DashboardRbacConfig
  readonly observability?: DashboardObservabilityConfig
}

// ── Console configuration ────────────────────────────────────────────

export interface ConsoleConfig {
  readonly url?: string | EnvVarRef
}

// ── Simulation init configuration ───────────────────────────────────

export interface KafkaTableDatagenFieldOptions {
  /** DataGen generator kind. Maps to `fields.<col>.kind` on the Flink datagen connector. */
  readonly kind: "random" | "sequence"
  /** Min value for numeric fields. Maps to `fields.<col>.min`. */
  readonly min?: number
  /** Max value for numeric fields. Maps to `fields.<col>.max`. */
  readonly max?: number
  /** String length. Maps to `fields.<col>.length`. */
  readonly length?: number
  /** Sequence start value. Maps to `fields.<col>.start`. */
  readonly start?: number
  /** Sequence end value. Maps to `fields.<col>.end`. */
  readonly end?: number
}

export interface KafkaTableDefinition {
  /** Flink SQL table name (e.g. 'orders', 'transactions') */
  readonly table: string
  /** Kafka topic name (e.g. 'ecom.orders') */
  readonly topic: string
  /** Column definitions: field name → Flink SQL type (e.g. { orderId: 'STRING', amount: 'DOUBLE' }) */
  readonly columns: Record<string, string>
  /** Watermark definition for event-time processing */
  readonly watermark?: {
    readonly column: string
    readonly expression: string
  }
  /** Primary key columns (generates PRIMARY KEY (...) NOT ENFORCED) */
  readonly primaryKey?: readonly string[]
  /** Kafka message format (default: 'json') */
  readonly format?: string
  /** Kafka consumer startup mode (default: 'earliest-offset') */
  readonly scanStartupMode?: string
  /** DataGen rows-per-second for continuous seeding (default: 10, 0 = skip seeding) */
  readonly rowsPerSecond?: number
  /** Per-column DataGen options (kind, min/max for numeric, length for strings) */
  readonly fields?: Record<string, KafkaTableDatagenFieldOptions>
}

export interface KafkaCatalogDefinition {
  /** Catalog name in Flink SQL (e.g. 'ecom', 'banking', 'iot') */
  readonly name: string
  /** Tables registered within this catalog */
  readonly tables: readonly KafkaTableDefinition[]
}

export interface JdbcCatalogDefinition {
  /** Catalog name in Flink SQL (e.g. 'pagila', 'flink_sink') */
  readonly name: string
  /** JDBC base URL (e.g. 'jdbc:postgresql://postgres:5432/') */
  readonly baseUrl: string
  /** Default database for the catalog */
  readonly defaultDatabase?: string
  /** Database username (default: 'reactor') */
  readonly username?: string
  /** Database password (default: 'reactor') */
  readonly password?: string
}

export interface SimInitConfig {
  readonly iceberg?: {
    readonly databases?: readonly string[]
  }
  readonly kafka?: {
    /** Just create Kafka topics (no Flink SQL table registration) */
    readonly topics?: readonly string[]
    /** Per-domain Kafka catalogs with full table schemas */
    readonly catalogs?: readonly KafkaCatalogDefinition[]
  }
  readonly jdbc?: {
    /** JDBC catalogs for auto-discovering PostgreSQL tables */
    readonly catalogs?: readonly JdbcCatalogDefinition[]
  }
}

export interface SimConfig {
  readonly init?: SimInitConfig
}

// ── Runtime ─────────────────────────────────────────────────────────

/**
 * Where a pipeline's infrastructure runs. Drives CLI command dispatch
 * (`fr up`, `fr deploy`) and determines which adapter handles lifecycle.
 */
export type Runtime = "docker" | "minikube" | "homebrew" | "kubernetes"

export const SUPPORTED_RUNTIMES: readonly Runtime[] = [
  "docker",
  "minikube",
  "homebrew",
  "kubernetes",
]

// ── Environment entry ───────────────────────────────────────────────

export interface EnvironmentEntry {
  /**
   * Runtime this environment targets. Omitted → defaulted by the resolver
   * based on env name (`development`/`local` → `docker`, `test` → `minikube`,
   * `staging`/`production` → `kubernetes`, otherwise → `docker`).
   */
  readonly runtime?: Runtime
  /**
   * Additional runtimes this env can be invoked with via `--runtime=<name>`.
   * Enables patterns like "development defaults to docker but can be
   * overridden to exercise the minikube lane locally".
   */
  readonly supportedRuntimes?: readonly Runtime[]
  readonly cluster?: ClusterConfig
  readonly kubernetes?: {
    readonly namespace?: string
    readonly image?: string
  }
  /** kubectl context used when runtime is `minikube` or `kubernetes`. */
  readonly kubectl?: {
    readonly context?: string
  }
  /** SQL Gateway URL used when runtime is `docker` for pipeline submission. */
  readonly sqlGateway?: {
    readonly url?: string
  }
  /**
   * Local Flink install root used when runtime is `homebrew`. Overrides
   * `$FLINK_HOME` / `brew --prefix apache-flink` auto-detection.
   */
  readonly flinkHome?: string
  readonly kafka?: {
    readonly bootstrapServers?: string
  }
  readonly connectors?: ConnectorConfig
  readonly dashboard?: DashboardSection
  readonly console?: ConsoleConfig
  readonly pipelines?: Record<string, PipelineOverrides>
  /** Simulation stack configuration (used by `flink-reactor sim up`) */
  readonly sim?: SimConfig
}

// ── FlinkReactorConfig ───────────────────────────────────────────────

export interface FlinkReactorConfig {
  readonly flink?: {
    readonly version?: FlinkMajorVersion
  }
  readonly kubernetes?: {
    readonly namespace?: string
    readonly image?: string
  }
  readonly kafka?: {
    readonly bootstrapServers?: string
  }
  readonly connectors?: ConnectorConfig
  /** Plugins to apply during synthesis */
  readonly plugins?: readonly FlinkReactorPlugin[]

  /** Default cluster connection settings */
  readonly cluster?: ClusterConfig
  /** Default dashboard settings */
  readonly dashboard?: DashboardSection
  /** Console (reactor-server) connection for tap manifest push */
  readonly console?: ConsoleConfig
  /** Named environments with overrides */
  readonly environments?: Record<string, EnvironmentEntry>

  /** Internal: convert this config to InfraConfig for app synthesis */
  toInfraConfig?(): InfraConfig
}

// ── defineConfig ─────────────────────────────────────────────────────

/**
 * Helper for creating a typed flink-reactor.config.ts.
 *
 * Validates the config shape and returns a frozen config object.
 * Provides compile-time type checking for flink.version, kubernetes,
 * kafka, connector settings, and environment overrides.
 *
 * @example
 * ```ts
 * import { defineConfig, env } from '@flink-reactor/dsl';
 *
 * export default defineConfig({
 *   flink: { version: '2.0' },
 *   dashboard: { pollIntervalMs: 5000 },
 *   environments: {
 *     development: {
 *       cluster: { url: 'http://localhost:8081' },
 *       dashboard: { mockMode: true },
 *     },
 *     production: {
 *       cluster: { url: env('FLINK_REST_URL') },
 *       dashboard: {
 *         auth: { type: 'basic', password: env('FLINK_AUTH_PASSWORD') },
 *       },
 *     },
 *   },
 * });
 * ```
 */
export function defineConfig(
  config: Omit<FlinkReactorConfig, "toInfraConfig">,
): FlinkReactorConfig {
  // Validate flink version if provided
  if (config.flink?.version !== undefined) {
    const validVersions: readonly FlinkMajorVersion[] = [
      "1.20",
      "2.0",
      "2.1",
      "2.2",
    ]
    if (!validVersions.includes(config.flink.version)) {
      throw new Error(
        `Unsupported Flink version '${config.flink.version}'. Supported: ${validVersions.join(", ")}`,
      )
    }
  }

  // Validate plugin name uniqueness
  if (config.plugins && config.plugins.length > 0) {
    const seen = new Set<string>()
    for (const plugin of config.plugins) {
      if (seen.has(plugin.name)) {
        throw new Error(
          `Duplicate plugin name '${plugin.name}' in config.plugins`,
        )
      }
      seen.add(plugin.name)
    }
  }

  // Validate delivery strategy if provided
  if (config.connectors?.delivery !== undefined) {
    const validStrategies: readonly DeliveryStrategy[] = [
      "init-container",
      "custom-image",
    ]
    if (!validStrategies.includes(config.connectors.delivery)) {
      throw new Error(
        `Unsupported connector delivery strategy '${config.connectors.delivery}'. Supported: ${validStrategies.join(", ")}`,
      )
    }
  }

  // Validate environment names if provided
  if (config.environments) {
    for (const envName of Object.keys(config.environments)) {
      if (!/^[a-zA-Z][a-zA-Z0-9_-]*$/.test(envName)) {
        throw new Error(
          `Invalid environment name '${envName}'. Must start with a letter and contain only letters, digits, hyphens, and underscores.`,
        )
      }
    }
  }

  const result: FlinkReactorConfig = {
    ...config,
    toInfraConfig() {
      return {
        kafka: config.kafka,
        kubernetes: config.kubernetes,
        connectors: config.connectors,
      }
    },
  }

  return Object.freeze(result)
}
