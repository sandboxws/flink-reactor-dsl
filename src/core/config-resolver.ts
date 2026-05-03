// ── Config resolution ────────────────────────────────────────────────
// Merges common config + named environment, resolves env() markers.

import type {
  ClusterConfig,
  ConnectorConfig,
  DashboardSection,
  FlinkReactorConfig,
  InfraConfig,
  PipelineOverrides,
  Runtime,
  ServicesConfig,
} from "./config.js"
import { SUPPORTED_RUNTIMES } from "./config.js"
import type { Resolved } from "./env-var.js"
import { resolveEnvVars } from "./env-var.js"
import type { FlinkMajorVersion } from "./types.js"

export type ResolvedServicesConfig = Resolved<ServicesConfig>

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
  /**
   * Resolved infrastructure services for this environment. Empty object
   * `{}` means "always-on services only"; absent entries are not started.
   * `cluster up` reads this directly to pick Compose profiles.
   */
  readonly services: ResolvedServicesConfig
  readonly connectors?: ConnectorConfig
  readonly dashboard: Resolved<DashboardSection>
  readonly console: {
    readonly url?: string
  }
  readonly pipelines: Record<string, PipelineOverrides>
  readonly environmentName?: string
  /** Resolved runtime for this env. Always set after resolution. */
  readonly runtime: Runtime
  /** Runtimes reachable via `--runtime=<name>` override for this env. */
  readonly supportedRuntimes: readonly Runtime[]
  /**
   * Container engine pin for the docker lane. `undefined` means auto-detect
   * at call time. The `--container-engine` flag and `FR_CONTAINER_ENGINE`
   * env var still override this at the call site.
   */
  readonly containerEngine?: "auto" | "docker" | "podman"
  readonly kubectl: {
    readonly context?: string
  }
  readonly sqlGateway: {
    readonly url?: string
  }
  readonly flinkHome?: string
}

/**
 * Default runtime picked when an env doesn't declare one. Privileged env
 * names map to the runtime the platform docs position as canonical for that
 * tier; unknown names fall back to `docker` (matches the docs' "easiest"
 * positioning).
 */
export function defaultRuntimeForEnv(envName?: string): Runtime {
  switch (envName) {
    case "development":
    case "local":
      return "docker"
    case "test":
      return "minikube"
    case "staging":
    case "production":
      return "kubernetes"
    default:
      return "docker"
  }
}

function defaultKubectlContextForRuntime(runtime: Runtime): string | undefined {
  if (runtime === "minikube") return "minikube"
  // For `kubernetes` runtime, no sensible default exists — the adapter
  // errors at deploy time with a pointer to config if a context isn't set.
  return undefined
}

// ── One-shot warnings ───────────────────────────────────────────────
// Deprecation warnings should fire once per process, not per
// resolveConfig() call. The CLI calls resolveConfig multiple times
// across the same project (status, up, deploy) so we de-dupe by key.

const emittedWarnings = new Set<string>()

function warnOnce(key: string, message: string): void {
  if (emittedWarnings.has(key)) return
  emittedWarnings.add(key)
  // Direct console use: we deliberately bypass any logger here so the
  // warning surfaces even in code paths that haven't wired logging yet.
  // biome-ignore lint/suspicious/noConsole: deprecation warnings need to surface
  console.warn(`[flink-reactor] ${message}`)
}

/**
 * @internal Shared between the imperative and Effect-based resolvers so
 * the same warning never fires twice across both paths in one process.
 */
export const __warnOnceForResolvers = warnOnce

/** @internal Test-only hook to reset the warning de-dup set. */
export function __resetDeprecationWarningsForTesting(): void {
  emittedWarnings.clear()
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
    console: config.console ?? {},
    pipelines: {},
    // Tracking presence vs `{}` separately matters for legacy back-fill:
    // a project with no `services` block at all may still have a legacy
    // `kafka.bootstrapServers` we want to honor, but a project that
    // explicitly declared `services: {}` opted out and should be left alone.
    services: config.services ?? undefined,
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
    if (envEntry.console) envOverrides.console = envEntry.console
    if (envEntry.pipelines) envOverrides.pipelines = envEntry.pipelines
    if (envEntry.runtime) envOverrides.runtime = envEntry.runtime
    if (envEntry.supportedRuntimes)
      envOverrides.supportedRuntimes = envEntry.supportedRuntimes
    if (envEntry.kubectl) envOverrides.kubectl = envEntry.kubectl
    if (envEntry.sqlGateway) envOverrides.sqlGateway = envEntry.sqlGateway
    if (envEntry.flinkHome) envOverrides.flinkHome = envEntry.flinkHome
    if (envEntry.containerEngine)
      envOverrides.containerEngine = envEntry.containerEngine
    if (envEntry.services) envOverrides.services = envEntry.services

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
  const console_ = resolved.console as Record<string, unknown> | undefined
  const kubectl = resolved.kubectl as Record<string, unknown> | undefined
  const sqlGateway = resolved.sqlGateway as Record<string, unknown> | undefined

  const declaredRuntime = resolved.runtime as Runtime | undefined
  if (declaredRuntime && !SUPPORTED_RUNTIMES.includes(declaredRuntime)) {
    throw new Error(
      `Unknown runtime '${declaredRuntime}' in environment '${envName ?? "<default>"}'. ` +
        `Supported: ${SUPPORTED_RUNTIMES.join(", ")}`,
    )
  }
  const runtime = declaredRuntime ?? defaultRuntimeForEnv(envName)

  const declaredSupported = resolved.supportedRuntimes as
    | readonly Runtime[]
    | undefined
  const supportedRuntimes: readonly Runtime[] = declaredSupported ?? [runtime]
  for (const r of supportedRuntimes) {
    if (!SUPPORTED_RUNTIMES.includes(r)) {
      throw new Error(
        `Unknown runtime '${r}' in supportedRuntimes for '${envName ?? "<default>"}'. ` +
          `Supported: ${SUPPORTED_RUNTIMES.join(", ")}`,
      )
    }
  }
  if (!supportedRuntimes.includes(runtime)) {
    throw new Error(
      `Runtime '${runtime}' is not listed in supportedRuntimes [${supportedRuntimes.join(", ")}] ` +
        `for environment '${envName ?? "<default>"}'. Add it or remove supportedRuntimes.`,
    )
  }

  // ── Services resolution + legacy back-fill ────────────────────────
  // Three cases for `services`:
  //   1. User declared a `services` block (possibly `{}`) → use it as-is.
  //      The legacy `kafka.bootstrapServers` is ignored; warn if both set
  //      since the legacy field will silently lose to the new one.
  //   2. `services` is absent AND legacy `kafka.bootstrapServers` is set
  //      → synthesize `services.kafka` from it; emit deprecation warning.
  //   3. Both absent → `services: {}` (no infra services this project).
  const declaredServices = resolved.services as ServicesConfig | undefined
  const legacyKafkaBootstrap = kafka?.bootstrapServers as string | undefined
  let services: ResolvedServicesConfig
  if (declaredServices !== undefined) {
    if (legacyKafkaBootstrap !== undefined) {
      warnOnce(
        "kafka.bootstrapServers-with-services",
        "`kafka.bootstrapServers` is set alongside `services` — the legacy " +
          "field is ignored. Move the value into `services.kafka.bootstrapServers`.",
      )
    }
    services = declaredServices as ResolvedServicesConfig
  } else if (legacyKafkaBootstrap !== undefined) {
    warnOnce(
      "kafka.bootstrapServers-deprecated",
      "`kafka.bootstrapServers` is deprecated. Move it under " +
        "`services.kafka.bootstrapServers` in flink-reactor.config.ts. " +
        "Auto-migrating for now.",
    )
    services = { kafka: { bootstrapServers: legacyKafkaBootstrap } }
  } else {
    services = {}
  }

  return {
    flink: {
      version: ((flink?.version as string) ??
        config.flink?.version ??
        "2.2") as FlinkMajorVersion,
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
      bootstrapServers:
        // Prefer the resolved services entry; fall back to legacy. This
        // keeps `resolved.kafka.bootstrapServers` accurate for callers
        // that haven't migrated to reading `resolved.services` yet.
        // Truthy check rejects both `undefined` (absent) and `false` (disabled).
        (services.kafka
          ? (services.kafka.bootstrapServers as string | undefined)
          : undefined) ?? (kafka?.bootstrapServers as string | undefined),
    },
    services,
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
    console: {
      url: console_?.url as string | undefined,
    },
    pipelines: (resolved.pipelines as Record<string, PipelineOverrides>) ?? {},
    environmentName: envName,
    runtime,
    supportedRuntimes,
    kubectl: {
      context:
        (kubectl?.context as string | undefined) ??
        defaultKubectlContextForRuntime(runtime),
    },
    sqlGateway: {
      url: sqlGateway?.url as string | undefined,
    },
    flinkHome: resolved.flinkHome as string | undefined,
    containerEngine: resolved.containerEngine as
      | "auto"
      | "docker"
      | "podman"
      | undefined,
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
