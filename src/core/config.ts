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

  /** Internal: convert this config to InfraConfig for app synthesis */
  toInfraConfig?(): InfraConfig
}

// ── defineConfig ─────────────────────────────────────────────────────

/**
 * Helper for creating a typed flink-reactor.config.ts.
 *
 * Validates the config shape and returns a frozen config object.
 * Provides compile-time type checking for flink.version, kubernetes,
 * kafka, and connector settings.
 *
 * @example
 * ```ts
 * export default defineConfig({
 *   flink: { version: '2.0' },
 *   kubernetes: { namespace: 'flink-prod', image: 'my-registry/flink:2.0' },
 *   kafka: { bootstrapServers: 'kafka-prod:9092' },
 *   connectors: { delivery: 'init-container' },
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
