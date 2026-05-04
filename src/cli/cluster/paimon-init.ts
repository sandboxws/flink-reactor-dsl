/**
 * Apache Paimon catalog + database provisioning DDL — shared between
 * `cluster up` (Docker) and `sim up` (minikube). Both lanes resolve
 * `seaweedfs.flink-demo.svc:8333` via service DNS (minikube) or compose
 * network aliases (Docker), and both default the warehouse to a
 * sub-prefix of the SeaweedFS bucket already created by
 * `06-seaweedfs-init.yaml` — no separate bucket is needed.
 *
 * Mirrors the shape of `iceberg-init.ts` and `fluss-init.ts` so all three
 * helpers are consumed identically by `runInit()` in both `sim.ts` and
 * `cluster.ts`.
 */

// `s3://` (not `s3a://`) routes through Paimon's bundled S3FileIO via the
// `paimon-s3` plugin in /opt/flink/lib. The Hadoop-native `s3a://` path
// would require `hadoop-aws` on the user classpath, which isn't present.
const DEFAULT_WAREHOUSE = "s3://flink-state/paimon"
const S3_ENDPOINT = "http://seaweedfs.flink-demo.svc:8333"

function createCatalogDdl(warehouse: string): string {
  return `CREATE CATALOG IF NOT EXISTS paimon_catalog WITH ('type' = 'paimon', 'warehouse' = '${warehouse}', 's3.endpoint' = '${S3_ENDPOINT}', 's3.path-style-access' = 'true', 's3.access-key' = 'admin', 's3.secret-key' = 'admin')`
}

/**
 * Returns DDL statements to register the `paimon_catalog` Paimon catalog
 * and create every database in `databases`. Returns an empty array when
 * `databases` is empty so callers can skip setup for non-Paimon projects.
 */
export function paimonInitStatements(
  databases: readonly string[],
  warehouse: string = DEFAULT_WAREHOUSE,
): string[] {
  if (databases.length === 0) return []

  const stmts = [createCatalogDdl(warehouse), "USE CATALOG paimon_catalog"]
  for (const db of databases) {
    stmts.push(`CREATE DATABASE IF NOT EXISTS \`${db}\``)
  }
  return stmts
}
