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

// ── Environment entry ───────────────────────────────────────────────

export interface EnvironmentEntry {
  readonly cluster?: ClusterConfig
  readonly kubernetes?: {
    readonly namespace?: string
    readonly image?: string
  }
  readonly kafka?: {
    readonly bootstrapServers?: string
  }
  readonly connectors?: ConnectorConfig
  readonly dashboard?: DashboardSection
  readonly pipelines?: Record<string, PipelineOverrides>
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
 * import { defineConfig, env } from 'flink-reactor';
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

    // Validate dashboard auth constraints within each environment
    for (const [envName, envEntry] of Object.entries(config.environments)) {
      if (envEntry.dashboard?.auth?.type === "basic") {
        if (
          !envEntry.dashboard.auth.username ||
          !envEntry.dashboard.auth.password
        ) {
          throw new Error(
            `Environment '${envName}': dashboard auth type 'basic' requires both username and password`,
          )
        }
      }
      if (envEntry.dashboard?.auth?.type === "token") {
        if (!envEntry.dashboard.auth.token) {
          throw new Error(
            `Environment '${envName}': dashboard auth type 'token' requires a token`,
          )
        }
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
