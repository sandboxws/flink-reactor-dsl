// ── Effect-wrapped config & env resolution ──────────────────────────
// Pure Effect versions of config resolution and env var lookup that
// use the ProcessEnv service instead of direct process.env access.

import { Effect } from "effect"
import type { FlinkReactorConfig, Runtime, ServicesConfig } from "./config.js"
import { SUPPORTED_RUNTIMES } from "./config.js"
import type {
  ResolvedConfig,
  ResolvedServicesConfig,
} from "./config-resolver.js"
import {
  defaultRuntimeForEnv,
  __warnOnceForResolvers as warnOnce,
} from "./config-resolver.js"
import type { EnvVarRef } from "./env-var.js"
import { isEnvVarRef } from "./env-var.js"
import { ConfigError } from "./errors.js"
import { ProcessEnv } from "./services.js"

// ── Single env var resolution ───────────────────────────────────────

/**
 * Resolve a single environment variable via the ProcessEnv service.
 *
 * Fails with `ConfigError` when the variable is undefined (and no
 * fallback is provided) or when it is empty and `allowEmpty` is false.
 */
export function resolveEnvVarEffect(
  varName: string,
  options?: { readonly fallback?: string; readonly allowEmpty?: boolean },
): Effect.Effect<string, ConfigError, ProcessEnv> {
  return Effect.gen(function* () {
    const env = yield* ProcessEnv
    const value = yield* env.get(varName)

    if (value !== undefined) {
      if (value === "" && options?.allowEmpty === false) {
        return yield* Effect.fail(
          new ConfigError({
            reason: "missing_env_var",
            message: `Environment variable '${varName}' is defined but empty`,
            varName,
          }),
        )
      }
      return value
    }

    if (options?.fallback !== undefined) {
      return options.fallback
    }

    return yield* Effect.fail(
      new ConfigError({
        reason: "missing_env_var",
        message: `Missing required environment variable '${varName}'`,
        varName,
      }),
    )
  })
}

// ── Config resolution with ProcessEnv ───────────────────────────────

/**
 * Walk an object tree and resolve all EnvVarRef markers using the
 * ProcessEnv service (instead of direct process.env access).
 */
function resolveEnvVarsEffect(
  obj: unknown,
  path: string,
): Effect.Effect<unknown, ConfigError, ProcessEnv> {
  return Effect.gen(function* () {
    if (isEnvVarRef(obj)) {
      const ref = obj as EnvVarRef
      const env = yield* ProcessEnv
      const value = yield* env.get(ref.varName)
      if (value !== undefined) return value
      if (ref.fallback !== undefined) return ref.fallback
      return yield* Effect.fail(
        new ConfigError({
          reason: "missing_env_var",
          message: `Missing required environment variable '${ref.varName}' at config path '${path}'`,
          varName: ref.varName,
          path,
        }),
      )
    }

    if (Array.isArray(obj)) {
      const results: unknown[] = []
      for (let i = 0; i < obj.length; i++) {
        results.push(yield* resolveEnvVarsEffect(obj[i], `${path}[${i}]`))
      }
      return results
    }

    if (typeof obj === "object" && obj !== null) {
      const result: Record<string, unknown> = {}
      for (const [key, val] of Object.entries(obj)) {
        result[key] = yield* resolveEnvVarsEffect(
          val,
          path ? `${path}.${key}` : key,
        )
      }
      return result
    }

    return obj
  })
}

/**
 * Resolve a FlinkReactorConfig with an optional environment name,
 * using the ProcessEnv service for env var resolution.
 *
 * This mirrors `resolveConfig()` from config-resolver.ts but threads
 * env var access through the service layer for testability.
 */
export function resolveConfigEffect(
  config: FlinkReactorConfig,
  envName?: string,
): Effect.Effect<ResolvedConfig, ConfigError, ProcessEnv> {
  return Effect.gen(function* () {
    // Build merged config (same logic as imperative version)
    const common: Record<string, unknown> = {
      flink: config.flink ?? {},
      cluster: config.cluster ?? {},
      kubernetes: config.kubernetes ?? {},
      kafka: config.kafka ?? {},
      connectors: config.connectors,
      dashboard: config.dashboard ?? {},
      console: config.console ?? {},
      pipelines: {},
      services: config.services ?? undefined,
    }

    let merged = common
    if (envName && config.environments) {
      const envEntry = config.environments[envName]
      if (!envEntry) {
        const available = Object.keys(config.environments).join(", ")
        return yield* Effect.fail(
          new ConfigError({
            reason: "unknown_environment",
            message: `Unknown environment '${envName}'. Available: ${available}`,
            environmentName: envName,
          }),
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
      if (envEntry.services) envOverrides.services = envEntry.services

      merged = deepMerge(common, envOverrides)
    }

    // Resolve env() markers via the ProcessEnv service
    const resolved = yield* resolveEnvVarsEffect(merged, "")

    // Apply defaults (same logic as imperative version)
    const r = resolved as Record<string, unknown>
    const flink = r.flink as Record<string, unknown> | undefined
    const cluster = r.cluster as Record<string, unknown> | undefined
    const kubernetes = r.kubernetes as Record<string, unknown> | undefined
    const kafka = r.kafka as Record<string, unknown> | undefined
    const dashboard = r.dashboard as Record<string, unknown> | undefined
    const console_ = r.console as Record<string, unknown> | undefined
    const kubectl = r.kubectl as Record<string, unknown> | undefined
    const sqlGateway = r.sqlGateway as Record<string, unknown> | undefined

    const declaredRuntime = r.runtime as Runtime | undefined
    if (declaredRuntime && !SUPPORTED_RUNTIMES.includes(declaredRuntime)) {
      return yield* Effect.fail(
        new ConfigError({
          reason: "unknown_environment",
          message: `Unknown runtime '${declaredRuntime}' in environment '${envName ?? "<default>"}'. Supported: ${SUPPORTED_RUNTIMES.join(", ")}`,
          environmentName: envName,
        }),
      )
    }
    const runtime: Runtime = declaredRuntime ?? defaultRuntimeForEnv(envName)

    const declaredSupported = r.supportedRuntimes as
      | readonly Runtime[]
      | undefined
    const supportedRuntimes: readonly Runtime[] = declaredSupported ?? [runtime]
    for (const rt of supportedRuntimes) {
      if (!SUPPORTED_RUNTIMES.includes(rt)) {
        return yield* Effect.fail(
          new ConfigError({
            reason: "unknown_environment",
            message: `Unknown runtime '${rt}' in supportedRuntimes for '${envName ?? "<default>"}'. Supported: ${SUPPORTED_RUNTIMES.join(", ")}`,
            environmentName: envName,
          }),
        )
      }
    }
    if (!supportedRuntimes.includes(runtime)) {
      return yield* Effect.fail(
        new ConfigError({
          reason: "unknown_environment",
          message: `Runtime '${runtime}' is not listed in supportedRuntimes [${supportedRuntimes.join(", ")}] for environment '${envName ?? "<default>"}'.`,
          environmentName: envName,
        }),
      )
    }

    const defaultContext = runtime === "minikube" ? "minikube" : undefined

    // Mirrors the services resolution + legacy back-fill in resolveConfig().
    // See that function for case-by-case rationale.
    const declaredServices = r.services as ServicesConfig | undefined
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
          "2.2") as ResolvedConfig["flink"]["version"],
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
          (services.kafka
            ? (services.kafka.bootstrapServers as string | undefined)
            : undefined) ?? (kafka?.bootstrapServers as string | undefined),
      },
      services,
      connectors: r.connectors as ResolvedConfig["connectors"],
      dashboard: {
        port: (dashboard?.port as number) ?? undefined,
        pollIntervalMs: (dashboard?.pollIntervalMs as number) ?? undefined,
        logBufferSize: (dashboard?.logBufferSize as number) ?? undefined,
        mockMode: (dashboard?.mockMode as boolean) ?? undefined,
        auth: dashboard?.auth as ResolvedConfig["dashboard"]["auth"],
        ssl: dashboard?.ssl as ResolvedConfig["dashboard"]["ssl"],
        rbac: dashboard?.rbac as ResolvedConfig["dashboard"]["rbac"],
        observability:
          dashboard?.observability as ResolvedConfig["dashboard"]["observability"],
      },
      console: {
        url: console_?.url as string | undefined,
      },
      pipelines:
        (r.pipelines as Record<string, ResolvedConfig["pipelines"][string]>) ??
        {},
      environmentName: envName,
      runtime,
      supportedRuntimes,
      kubectl: {
        context: (kubectl?.context as string | undefined) ?? defaultContext,
      },
      sqlGateway: {
        url: sqlGateway?.url as string | undefined,
      },
      flinkHome: r.flinkHome as string | undefined,
    } satisfies ResolvedConfig
  })
}

// ── Deep merge utility (duplicated from config-resolver.ts) ─────────

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}

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
