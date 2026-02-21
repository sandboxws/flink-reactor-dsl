// ── Config resolution ────────────────────────────────────────────────
// Merges common config + named environment, resolves env() markers,
// and produces flat dashboard JSON for cross-process consumption.

import type { FlinkMajorVersion } from './types.js';
import type {
  FlinkReactorConfig,
  InfraConfig,
  ClusterConfig,
  DashboardSection,
  PipelineOverrides,
  ConnectorConfig,
  EnvironmentEntry,
} from './config.js';
import type { Resolved } from './env-var.js';
import { resolveEnvVars } from './env-var.js';

// ── ResolvedConfig ──────────────────────────────────────────────────

/**
 * A fully resolved config — all env() markers replaced with string
 * values, common + environment merged, and defaults applied.
 */
export interface ResolvedConfig {
  readonly flink: {
    readonly version: FlinkMajorVersion;
  };
  readonly cluster: Resolved<ClusterConfig>;
  readonly kubernetes: {
    readonly namespace: string;
    readonly image?: string;
  };
  readonly kafka: {
    readonly bootstrapServers?: string;
  };
  readonly connectors?: ConnectorConfig;
  readonly dashboard: Resolved<DashboardSection>;
  readonly pipelines: Record<string, PipelineOverrides>;
  readonly environmentName?: string;
}

// ── Deep merge utility ──────────────────────────────────────────────

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/**
 * Deep-merge two objects. "Last writer wins" for primitives and arrays.
 * Recursive merge for plain objects.
 */
function deepMerge<T extends Record<string, unknown>>(base: T, override: Record<string, unknown>): T {
  const result: Record<string, unknown> = { ...base };

  for (const [key, value] of Object.entries(override)) {
    if (value === undefined) continue;

    const baseValue = result[key];
    if (isPlainObject(baseValue) && isPlainObject(value)) {
      result[key] = deepMerge(baseValue, value);
    } else {
      result[key] = value;
    }
  }

  return result as T;
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
  };

  // Merge environment overrides if specified
  let merged = common;
  if (envName && config.environments) {
    const envEntry = config.environments[envName];
    if (!envEntry) {
      const available = Object.keys(config.environments).join(', ');
      throw new Error(
        `Unknown environment '${envName}'. Available: ${available}`,
      );
    }

    const envOverrides: Record<string, unknown> = {};
    if (envEntry.cluster) envOverrides.cluster = envEntry.cluster;
    if (envEntry.kubernetes) envOverrides.kubernetes = envEntry.kubernetes;
    if (envEntry.kafka) envOverrides.kafka = envEntry.kafka;
    if (envEntry.connectors) envOverrides.connectors = envEntry.connectors;
    if (envEntry.dashboard) envOverrides.dashboard = envEntry.dashboard;
    if (envEntry.pipelines) envOverrides.pipelines = envEntry.pipelines;

    merged = deepMerge(common, envOverrides);
  }

  // Resolve env() markers
  const resolved = resolveEnvVars(merged);

  // Apply defaults
  const flink = resolved.flink as Record<string, unknown> | undefined;
  const cluster = resolved.cluster as Record<string, unknown> | undefined;
  const kubernetes = resolved.kubernetes as Record<string, unknown> | undefined;
  const kafka = resolved.kafka as Record<string, unknown> | undefined;
  const dashboard = resolved.dashboard as Record<string, unknown> | undefined;

  return {
    flink: {
      version: ((flink?.version as string) ?? config.flink?.version ?? '2.0') as FlinkMajorVersion,
    },
    cluster: {
      url: cluster?.url as string | undefined,
      displayName: (cluster?.displayName as string) ?? undefined,
    },
    kubernetes: {
      namespace: (kubernetes?.namespace as string) ?? 'default',
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
      auth: dashboard?.auth as Resolved<DashboardSection>['auth'],
      ssl: dashboard?.ssl as Resolved<DashboardSection>['ssl'],
      rbac: dashboard?.rbac as Resolved<DashboardSection>['rbac'],
      observability: dashboard?.observability as Resolved<DashboardSection>['observability'],
    },
    pipelines: (resolved.pipelines as Record<string, PipelineOverrides>) ?? {},
    environmentName: envName,
  };
}

// ── Resolved dashboard JSON ─────────────────────────────────────────

/**
 * Shape written to .flink-reactor/resolved-dashboard.json.
 * This is what the dashboard reads — flat keys matching DashboardConfig.
 */
export interface ResolvedDashboardJson {
  readonly _version: 1;
  readonly flinkRestUrl?: string;
  readonly dashboardPort?: number;
  readonly authType?: string;
  readonly authUsername?: string;
  readonly authPassword?: string;
  readonly authToken?: string;
  readonly sslEnabled?: boolean;
  readonly sslCaPath?: string;
  readonly pollIntervalMs?: number;
  readonly logBufferSize?: number;
  readonly clusterDisplayName?: string;
  readonly mockMode?: boolean;
  readonly rbacEnabled?: boolean;
  readonly rbacProvider?: string;
  readonly rbacRoles?: Record<string, string[]>;
  readonly prometheusUrl?: string;
  readonly prometheusEnabled?: boolean;
  readonly alertWebhookUrl?: string;
  readonly alertWebhookEnabled?: boolean;
}

/**
 * Map a ResolvedConfig to the flat dashboard JSON shape.
 */
export function buildResolvedDashboardJson(
  resolved: ResolvedConfig,
): ResolvedDashboardJson {
  // At this point all env() markers have been resolved to strings.
  // Cast to plain types to satisfy TypeScript's conditional type limitations.
  const cluster = resolved.cluster as { url?: string; displayName?: string };
  const dashboard = resolved.dashboard as {
    port?: number;
    pollIntervalMs?: number;
    logBufferSize?: number;
    mockMode?: boolean;
    auth?: { type?: string; username?: string; password?: string; token?: string };
    ssl?: { enabled?: boolean; caPath?: string };
    rbac?: { enabled?: boolean; provider?: string; roles?: Record<string, string[]> };
    observability?: { prometheus?: string; alertWebhook?: string };
  };

  return {
    _version: 1,
    flinkRestUrl: cluster.url,
    dashboardPort: dashboard.port,
    authType: dashboard.auth?.type,
    authUsername: dashboard.auth?.username,
    authPassword: dashboard.auth?.password,
    authToken: dashboard.auth?.token,
    sslEnabled: dashboard.ssl?.enabled,
    sslCaPath: dashboard.ssl?.caPath,
    pollIntervalMs: dashboard.pollIntervalMs,
    logBufferSize: dashboard.logBufferSize,
    clusterDisplayName: cluster.displayName,
    mockMode: dashboard.mockMode,
    rbacEnabled: dashboard.rbac?.enabled,
    rbacProvider: dashboard.rbac?.provider,
    rbacRoles: dashboard.rbac?.roles,
    prometheusUrl: dashboard.observability?.prometheus,
    prometheusEnabled: dashboard.observability?.prometheus != null,
    alertWebhookUrl: dashboard.observability?.alertWebhook,
    alertWebhookEnabled: dashboard.observability?.alertWebhook != null,
  };
}

// ── InfraConfig extraction ──────────────────────────────────────────

/**
 * Extract InfraConfig from a ResolvedConfig for use in synthesizeApp().
 */
export function toInfraConfigFromResolved(resolved: ResolvedConfig): InfraConfig {
  return {
    kafka: resolved.kafka,
    kubernetes: resolved.kubernetes,
    connectors: resolved.connectors,
  };
}
