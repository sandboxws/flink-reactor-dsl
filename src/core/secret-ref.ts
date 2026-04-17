// ── Kubernetes Secret reference primitive ───────────────────────────
// secretRef() creates a branded marker object that represents a reference
// to a Kubernetes Secret. At synthesis time it expands into:
//   • a `${env:<envName>}` placeholder in the emitted pipeline YAML
//     (Flink CDC 3.6's native env-substitution syntax), and
//   • an individual `env` entry in the FlinkDeployment podTemplate with
//     `valueFrom.secretKeyRef.{name, key}`.
//
// The DSL never sees the cleartext secret value — the k8s Secret itself
// is managed by the user out-of-band (kubectl / External Secrets Operator
// / Vault Secrets Operator / etc.). The DSL only emits the plumbing.

/**
 * Global symbol brand — survives cross-realm jiti imports because
 * Symbol.for() returns the same symbol in every realm.
 */
const SECRET_REF_BRAND = Symbol.for("flink-reactor.secret-ref")

/**
 * A marker object representing a reference to a Kubernetes Secret.
 * Created by `secretRef()`.
 */
export interface SecretRef {
  readonly [brand: symbol]: true
  /** Kubernetes Secret name (e.g. `pg-primary-password`). */
  readonly name: string
  /** Key within the Secret data (defaults to `password`). */
  readonly key: string
  /**
   * Env-var name inside the container. Populated by `secretKeyRef` in the
   * pod spec and referenced from the pipeline YAML via `${env:<envName>}`.
   */
  readonly envName: string
}

/**
 * Derive a default env-var name from a Secret `name` + `key`.
 *
 * Rules:
 *   • Uppercase the Secret `name`, replacing `-`, `.`, `/` with `_`.
 *   • When `key !== 'password'`, append `_<uppercase(key)>` so references
 *     to multi-key secrets don't collide with one another.
 *   • Strip any character that isn't a valid env-var identifier char.
 */
export function deriveEnvName(name: string, key: string): string {
  const sanitize = (s: string) =>
    s
      .toUpperCase()
      .replace(/[^A-Z0-9_]+/g, "_")
      .replace(/^_+|_+$/g, "")
      .replace(/_+/g, "_")

  const base = sanitize(name)
  if (key === "password") return base
  return `${base}_${sanitize(key)}`
}

/**
 * Create a reference to a Kubernetes Secret.
 *
 * @param name   Kubernetes Secret name (e.g. `pg-primary-password`).
 * @param key    Key within the Secret (defaults to `password`).
 * @param envName Optional explicit env-var name inside the container.
 *                Defaults to a deterministic uppercase-snake derivation
 *                from `(name, key)`.
 *
 * @example
 * ```ts
 * password: secretRef('pg-primary-password')
 * //   → { name: 'pg-primary-password', key: 'password',
 * //       envName: 'PG_PRIMARY_PASSWORD' }
 * //   YAML:  password: ${env:PG_PRIMARY_PASSWORD}
 * //   CRD :  env: [{ name: PG_PRIMARY_PASSWORD,
 * //                  valueFrom: { secretKeyRef: {
 * //                    name: pg-primary-password, key: password } } }]
 * ```
 */
export function secretRef(
  name: string,
  key: string = "password",
  envName?: string,
): SecretRef {
  if (!name || typeof name !== "string") {
    throw new Error("secretRef() requires a non-empty Secret name")
  }
  if (!key || typeof key !== "string") {
    throw new Error("secretRef() requires a non-empty key")
  }
  const resolved = envName ?? deriveEnvName(name, key)
  if (!/^[A-Z_][A-Z0-9_]*$/.test(resolved)) {
    throw new Error(
      `secretRef() envName '${resolved}' is not a valid env-var identifier`,
    )
  }
  const ref: SecretRef = {
    [SECRET_REF_BRAND]: true as const,
    name,
    key,
    envName: resolved,
  }
  return Object.freeze(ref)
}

/**
 * Type guard: is this value a `SecretRef` marker?
 */
export function isSecretRef(value: unknown): value is SecretRef {
  return (
    typeof value === "object" &&
    value !== null &&
    (value as Record<symbol, unknown>)[SECRET_REF_BRAND] === true &&
    typeof (value as SecretRef).name === "string" &&
    typeof (value as SecretRef).envName === "string"
  )
}

/**
 * Render the YAML placeholder string Flink CDC 3.6 substitutes at runtime.
 */
export function renderSecretPlaceholder(ref: SecretRef): string {
  return `\${env:${ref.envName}}`
}
