/**
 * Apache Fluss catalog + database provisioning DDL — shared between
 * `cluster up` (Docker, change 52) and `sim up` (minikube, this change).
 * Both lanes resolve the bootstrap servers via service DNS (minikube)
 * or compose network aliases (Docker), defaulting to `fluss-coordinator:9123`.
 *
 * Mirrors the shape of `iceberg-init.ts` so both helpers are consumed
 * identically by `runInit()` in `sim.ts` and the parallel branch in
 * `cluster.ts`.
 */

const DEFAULT_BOOTSTRAP_SERVERS = "fluss-coordinator:9123"

function createCatalogDdl(bootstrapServers: string): string {
  return `CREATE CATALOG IF NOT EXISTS fluss_catalog WITH ('type' = 'fluss', 'bootstrap.servers' = '${bootstrapServers}')`
}

/**
 * Returns DDL statements to register the `fluss_catalog` Fluss catalog and
 * create every database in `databases`. Returns an empty array when
 * `databases` is empty so callers can skip setup for non-Fluss projects.
 */
export function flussInitStatements(
  databases: readonly string[],
  bootstrapServers: string = DEFAULT_BOOTSTRAP_SERVERS,
): string[] {
  if (databases.length === 0) return []

  const stmts = [
    createCatalogDdl(bootstrapServers),
    "USE CATALOG fluss_catalog",
  ]
  for (const db of databases) {
    stmts.push(`CREATE DATABASE IF NOT EXISTS \`${db}\``)
  }
  return stmts
}
