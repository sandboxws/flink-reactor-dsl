// ── Environment variable markers ────────────────────────────────────
// env() creates branded marker objects that survive the jiti boundary.
// resolveEnvVars() walks a config tree and replaces markers with
// actual process.env values at resolution time.

/**
 * Global symbol brand — survives cross-realm jiti imports because
 * Symbol.for() returns the same symbol in every realm.
 */
const ENV_VAR_BRAND = Symbol.for('flink-reactor.env-var');

/**
 * A marker object representing a deferred environment variable reference.
 * Created by `env()`, resolved by `resolveEnvVars()`.
 */
export interface EnvVarRef {
  readonly [brand: symbol]: true;
  readonly varName: string;
  readonly fallback?: string;
}

/**
 * Create a deferred environment variable reference.
 *
 * Use inside `defineConfig()` for secrets and environment-specific values
 * that should only be resolved at runtime (not at config-load time).
 *
 * @example
 * ```ts
 * defineConfig({
 *   environments: {
 *     production: {
 *       cluster: { url: env('FLINK_REST_URL') },
 *       dashboard: {
 *         auth: { password: env('FLINK_AUTH_PASSWORD') },
 *       },
 *     },
 *   },
 * });
 * ```
 */
export function env(varName: string, fallback?: string): EnvVarRef {
  if (!varName || typeof varName !== 'string') {
    throw new Error('env() requires a non-empty variable name');
  }
  const ref: EnvVarRef = {
    [ENV_VAR_BRAND]: true as const,
    varName,
    ...(fallback !== undefined ? { fallback } : {}),
  };
  return Object.freeze(ref);
}

/**
 * Type guard: is this value an EnvVarRef marker?
 */
export function isEnvVarRef(value: unknown): value is EnvVarRef {
  return (
    typeof value === 'object' &&
    value !== null &&
    (value as Record<symbol, unknown>)[ENV_VAR_BRAND] === true &&
    typeof (value as EnvVarRef).varName === 'string'
  );
}

// ── Resolution ──────────────────────────────────────────────────────

/**
 * Deep-walk an object tree and replace every EnvVarRef marker with the
 * corresponding `process.env` value.
 *
 * Throws with a descriptive config path when a required variable
 * (no fallback) is undefined.
 */
export function resolveEnvVars<T>(obj: T, pathPrefix?: string): Resolved<T> {
  return walk(obj, pathPrefix ?? '') as Resolved<T>;
}

function walk(value: unknown, path: string): unknown {
  if (isEnvVarRef(value)) {
    const envValue = process.env[value.varName];
    if (envValue !== undefined) return envValue;
    if (value.fallback !== undefined) return value.fallback;
    throw new Error(
      `Missing required environment variable '${value.varName}' at config path '${path}'`,
    );
  }

  if (Array.isArray(value)) {
    return value.map((item, i) => walk(item, `${path}[${i}]`));
  }

  if (typeof value === 'object' && value !== null) {
    const result: Record<string, unknown> = {};
    for (const [key, val] of Object.entries(value)) {
      result[key] = walk(val, path ? `${path}.${key}` : key);
    }
    return result;
  }

  return value;
}

// ── Resolved<T> mapped type ─────────────────────────────────────────

/**
 * Recursively strips EnvVarRef from a type, replacing with `string`.
 *
 * This allows downstream code to work with fully-resolved configs
 * without needing to handle env-var markers.
 */
export type Resolved<T> =
  T extends EnvVarRef
    ? string
    : T extends ReadonlyArray<infer U>
      ? Resolved<U>[]
      : T extends Record<string, unknown>
        ? { [K in keyof T]: Resolved<T[K]> }
        : T;
