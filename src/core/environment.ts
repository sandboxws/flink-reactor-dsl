// ── Environment configuration ────────────────────────────────────────

// Re-export PipelineOverrides from config.ts (canonical location)
export type { PipelineOverrides } from "./config.js"

export interface EnvironmentConfig {
  readonly name: string
  readonly infra?: {
    readonly kafka?: {
      readonly bootstrapServers?: string
    }
    readonly kubernetes?: {
      readonly namespace?: string
      readonly image?: string
    }
  }
  /** Pipeline-specific overrides. Use '*' for wildcard (all pipelines). */
  readonly pipelineOverrides?: Record<
    string,
    import("./config.js").PipelineOverrides
  >
}

// ── Resolution ───────────────────────────────────────────────────────

/**
 * Resolve the effective overrides for a named pipeline from an EnvironmentConfig.
 *
 * Named pipeline overrides take priority over wildcard ('*') overrides.
 * Properties from both are merged, with the named override winning on conflicts.
 *
 * @deprecated Use `resolveConfig()` from `./config-resolver.js` with inline
 * `environments` in `defineConfig()` instead of separate `env/*.ts` files.
 */
export function resolveEnvironment(
  pipelineName: string,
  env: EnvironmentConfig,
): Record<string, unknown> {
  const result: Record<string, unknown> = {}

  // Apply infra-level overrides
  if (env.infra?.kafka?.bootstrapServers) {
    result.bootstrapServers = env.infra.kafka.bootstrapServers
  }
  if (env.infra?.kubernetes?.namespace) {
    result.namespace = env.infra.kubernetes.namespace
  }

  const overrides = env.pipelineOverrides
  if (!overrides) return result

  // Apply wildcard overrides first (lower priority)
  const wildcard = overrides["*"]
  if (wildcard) {
    for (const [key, value] of Object.entries(wildcard)) {
      if (value !== undefined) {
        result[key] = value
      }
    }
  }

  // Apply named pipeline overrides (higher priority, overwrites wildcard)
  const named = overrides[pipelineName]
  if (named) {
    for (const [key, value] of Object.entries(named)) {
      if (value !== undefined) {
        result[key] = value
      }
    }
  }

  return result
}

// ── Auto-discovery ───────────────────────────────────────────────────

/**
 * Discover environment config files from a directory.
 * Each .ts file in the directory is treated as an environment.
 * The filename (without extension) becomes the environment name.
 *
 * Returns an array of { name, path } entries (does NOT import them).
 *
 * @deprecated Use inline `environments` in `defineConfig()` instead of
 * separate `env/*.ts` files.
 */
export function discoverEnvironments(
  envDir: string,
  files: readonly string[],
): Array<{ name: string; path: string }> {
  return files
    .filter((f) => f.endsWith(".ts") && !f.endsWith(".d.ts"))
    .map((f) => ({
      name: f.replace(/\.ts$/, ""),
      path: `${envDir}/${f}`,
    }))
}

// ── defineEnvironment ───────────────────────────────────────────────

/**
 * Helper for creating a typed environment config file (env/*.ts).
 *
 * Returns a frozen EnvironmentConfig.
 *
 * @deprecated Use inline `environments` in `defineConfig()` instead.
 */
export function defineEnvironment(
  config: EnvironmentConfig,
): EnvironmentConfig {
  return Object.freeze(config)
}
