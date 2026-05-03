// ── Service selection: ResolvedConfig → Compose profile names ───────
//
// Pure translator. Reads `resolved.services` and decides which Docker
// Compose profiles `cluster up` should activate. Also surfaces the
// per-service env-var overrides that the compose file consumes via
// `${VAR:-default}` placeholders.
//
// Compose-side coupling lives here, not in the connector registry: the
// fact that Iceberg/Lakekeeper backs onto Postgres is a property of the
// compose file, not of the iceberg connector itself.

import type { PostgresServiceConfig, ServicesConfig } from "@/core/config.js"
import type { ResolvedConfig } from "@/core/config-resolver.js"

/**
 * Every Compose profile we know how to activate. Used by `cluster down`
 * (always pass all of these so dormant services drop too) and by the
 * `--all` escape hatch on `cluster up`.
 *
 * Note: there are *two* postgres profiles — exactly one is ever active
 * at a time, controlled by `services.postgres.flavor`. Both are listed
 * here so `cluster down` is exhaustive.
 */
export const ALL_PROFILES = [
  "kafka",
  "iceberg",
  "fluss",
  "timescaledb",
  "postgres-plain",
] as const

export type ComposeProfile = (typeof ALL_PROFILES)[number]

/**
 * Compose-side dependency map: declaring `iceberg` implicitly pulls in
 * `postgres` because Lakekeeper uses Postgres as its catalog backing
 * store. Kept here (not in the connector registry) because it's a
 * compose-deployment fact, not a connector fact.
 */
const PROFILE_REQUIRES: Readonly<
  Partial<Record<ComposeProfile, ComposeProfile[]>>
> = {
  // 'iceberg' implies a postgres flavor; the actual flavor name is
  // resolved by `selectPostgresProfile` based on user config.
}

/**
 * Translate the resolved `services` block to the list of Compose profiles
 * that `cluster up` should activate.
 *
 * Behavior:
 * - Each truthy entry in `services` activates the corresponding profile.
 * - `iceberg` implicitly activates a postgres profile (Lakekeeper backing
 *   store) — flavor follows `services.postgres.flavor` if set, else
 *   defaults to `timescaledb`.
 * - `services.postgres.flavor: 'plain'` selects `postgres-plain`; default
 *   selects `timescaledb`.
 * - Entries set to `false` are subtracted (no profile).
 * - An empty `services` block returns `[]` — only always-on services run.
 */
export function profilesFromConfig(resolved: ResolvedConfig): ComposeProfile[] {
  const services = resolved.services
  const out = new Set<ComposeProfile>()

  if (services.kafka) out.add("kafka")
  if (services.fluss) out.add("fluss")
  if (services.iceberg) out.add("iceberg")

  // Postgres is the only multi-flavor profile. We resolve it after
  // looking at iceberg, so iceberg's implicit postgres dependency uses
  // the user's chosen flavor (if any).
  const wantsPostgres = !!services.postgres || out.has("iceberg")
  if (wantsPostgres) {
    const flavor = selectPostgresProfile(services.postgres)
    out.add(flavor)
  }

  // Apply transitive closure for any future PROFILE_REQUIRES entries.
  // We deliberately keep the loop here even though the table is
  // currently empty — adding a future profile dependency means one
  // edit to PROFILE_REQUIRES, no logic change.
  let changed = true
  while (changed) {
    changed = false
    for (const p of Array.from(out)) {
      const deps = PROFILE_REQUIRES[p]
      if (!deps) continue
      for (const d of deps) {
        if (!out.has(d)) {
          out.add(d)
          changed = true
        }
      }
    }
  }

  // Preserve a stable order matching ALL_PROFILES so `--profile` args
  // are deterministic across runs (helps with snapshot tests + diffing).
  return ALL_PROFILES.filter((p) => out.has(p))
}

function selectPostgresProfile(
  postgres: ResolvedConfig["services"]["postgres"],
): ComposeProfile {
  if (postgres === false || postgres === undefined) return "timescaledb"
  const flavor = (postgres as PostgresServiceConfig).flavor
  return flavor === "plain" ? "postgres-plain" : "timescaledb"
}

/**
 * Build the env-var map that compose interpolates into per-service
 * `${VAR:-default}` placeholders. Only sets vars when the user
 * overrode the default — leaving the env unset preserves the compose
 * file's bake-in default.
 */
export function buildComposeEnv(
  services: ServicesConfig | undefined,
): Record<string, string> {
  const env: Record<string, string> = {}
  if (!services) return env

  const kafka = services.kafka
  if (kafka) {
    if (kafka.externalPort !== undefined) {
      env.KAFKA_EXTERNAL_PORT = String(kafka.externalPort)
    }
    if (kafka.image !== undefined) {
      env.KAFKA_IMAGE = kafka.image
    }
  }

  const postgres = services.postgres
  if (postgres) {
    if (postgres.externalPort !== undefined) {
      env.POSTGRES_PORT = String(postgres.externalPort)
    }
  }

  const fluss = services.fluss
  if (fluss) {
    if (fluss.externalPort !== undefined) {
      env.FLUSS_COORDINATOR_PORT = String(fluss.externalPort)
    }
  }

  const iceberg = services.iceberg
  if (iceberg) {
    if (iceberg.externalPort !== undefined) {
      env.ICEBERG_REST_PORT = String(iceberg.externalPort)
    }
  }

  return env
}
