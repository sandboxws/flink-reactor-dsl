// ── Config resolution ────────────────────────────────────────────────
// Merges common config + named environment, resolves env() markers.

import type {
  ClusterConfig,
  ConnectorConfig,
  DashboardSection,
  FlinkReactorConfig,
  InfraConfig,
  PipelineOverrides,
} from "./config.js"
import type { Resolved } from "./env-var.js"
import { resolveEnvVars } from "./env-var.js"
import type { FlinkMajorVersion } from "./types.js"

// ── ResolvedConfig ──────────────────────────────────────────────────

/**
 * A fully resolved config — all env() markers replaced with string
 * values, common + environment merged, and defaults applied.
 */
export interface ResolvedConfig {
  readonly flink: {
    readonly version: FlinkMajorVersion
  }
  readonly cluster: Resolved<ClusterConfig>
  readonly kubernetes: {
    readonly namespace: string
    readonly image?: string
  }
  readonly kafka: {
    readonly bootstrapServers?: string
  }
  readonly connectors?: ConnectorConfig
  readonly dashboard: Resolved<DashboardSection>
  readonly pipelines: Record<string, PipelineOverrides>
  readonly environmentName?: string
}

// ── Deep merge utility ──────────────────────────────────────────────

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}

/**
 * Deep-merge two objects. "Last writer wins" for primitives and arrays.
 * Recursive merge for plain objects.
 */
function deepMerge<T extends Record<string, unknown>>(
  base: T,
  override: Record<string, unknown>,
): T {
  const result: Record<string, unknown> = { ...base }

  for (const [key, value] of Object.entries(override)) {
    if (value === undefined) continue

    const baseValue = result[key]
    if (isPlainObject(baseValue) && isPlainObject(value)) {
      result[key] = deepMerge(baseValue, value)
    } else {
      result[key] = value
    }
  }

  return result as T
}

// ── resolveConfig ───────────────────────────────────────────────────

/**
 * Resolve a FlinkReactorConfig with an optional environment name.
 *
 * 1. Extracts common settings from the top-level config
 * 2. Deep-merges the named environment's overrides on top
 * 3. Resolves all env() markers via process.env
 * 4. Applies defaults for missing values
 */
export function resolveConfig(
  config: FlinkReactorConfig,
  envName?: string,
): ResolvedConfig {
  // Start with common/top-level values
  const common: Record<string, unknown> = {
    flink: config.flink ?? {},
    cluster: config.cluster ?? {},
    kubernetes: config.kubernetes ?? {},
    kafka: config.kafka ?? {},
    connectors: config.connectors,
    dashboard: config.dashboard ?? {},
    pipelines: {},
  }

  // Merge environment overrides if specified
  let merged = common
  if (envName && config.environments) {
    const envEntry = config.environments[envName]
    if (!envEntry) {
      const available = Object.keys(config.environments).join(", ")
      throw new Error(
        `Unknown environment '${envName}'. Available: ${available}`,
      )
    }

    const envOverrides: Record<string, unknown> = {}
    if (envEntry.cluster) envOverrides.cluster = envEntry.cluster
    if (envEntry.kubernetes) envOverrides.kubernetes = envEntry.kubernetes
    if (envEntry.kafka) envOverrides.kafka = envEntry.kafka
    if (envEntry.connectors) envOverrides.connectors = envEntry.connectors
    if (envEntry.dashboard) envOverrides.dashboard = envEntry.dashboard
    if (envEntry.pipelines) envOverrides.pipelines = envEntry.pipelines

    merged = deepMerge(common, envOverrides)
  }

  // Resolve env() markers
  const resolved = resolveEnvVars(merged)

  // Apply defaults
  const flink = resolved.flink as Record<string, unknown> | undefined
  const cluster = resolved.cluster as Record<string, unknown> | undefined
  const kubernetes = resolved.kubernetes as Record<string, unknown> | undefined
  const kafka = resolved.kafka as Record<string, unknown> | undefined
  const dashboard = resolved.dashboard as Record<string, unknown> | undefined

  return {
    flink: {
      version: ((flink?.version as string) ??
        config.flink?.version ??
        "2.0") as FlinkMajorVersion,
    },
    cluster: {
      url: cluster?.url as string | undefined,
      displayName: (cluster?.displayName as string) ?? undefined,
    },
    kubernetes: {
      namespace: (kubernetes?.namespace as string) ?? "default",
      image: kubernetes?.image as string | undefined,
    },
    kafka: {
      bootstrapServers: kafka?.bootstrapServers as string | undefined,
    },
    connectors: resolved.connectors as ConnectorConfig | undefined,
    dashboard: {
      port: (dashboard?.port as number) ?? undefined,
      pollIntervalMs: (dashboard?.pollIntervalMs as number) ?? undefined,
      logBufferSize: (dashboard?.logBufferSize as number) ?? undefined,
      mockMode: (dashboard?.mockMode as boolean) ?? undefined,
      auth: dashboard?.auth as Resolved<DashboardSection>["auth"],
      ssl: dashboard?.ssl as Resolved<DashboardSection>["ssl"],
      rbac: dashboard?.rbac as Resolved<DashboardSection>["rbac"],
      observability:
        dashboard?.observability as Resolved<DashboardSection>["observability"],
    },
    pipelines: (resolved.pipelines as Record<string, PipelineOverrides>) ?? {},
    environmentName: envName,
  }
}

// ── InfraConfig extraction ──────────────────────────────────────────

/**
 * Extract InfraConfig from a ResolvedConfig for use in synthesizeApp().
 */
export function toInfraConfigFromResolved(
  resolved: ResolvedConfig,
): InfraConfig {
  return {
    kafka: resolved.kafka,
    kubernetes: resolved.kubernetes,
    connectors: resolved.connectors,
  }
}
