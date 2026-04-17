/**
 * Iceberg REST catalog + database provisioning DDL — shared between
 * `cluster up` (Docker) and `sim up` (minikube). Both lanes resolve
 * `iceberg-rest:8181` and `seaweedfs.flink-demo.svc:8333` via service DNS
 * (minikube) or compose network aliases (Docker).
 */

const CATALOG_URI = "http://iceberg-rest:8181"
const WAREHOUSE = "s3://flink-state/warehouse"
const S3_ENDPOINT = "http://seaweedfs.flink-demo.svc:8333"

function createCatalogDdl(): string {
  return `CREATE CATALOG IF NOT EXISTS lakehouse WITH ('type' = 'iceberg', 'catalog-type' = 'rest', 'uri' = '${CATALOG_URI}', 'warehouse' = '${WAREHOUSE}', 's3.endpoint' = '${S3_ENDPOINT}', 's3.path-style-access' = 'true', 's3.access-key' = 'admin', 's3.secret-key' = 'admin')`
}

/**
 * Returns DDL statements to register the `lakehouse` Iceberg catalog and
 * create every database in `databases`. Returns an empty array when
 * `databases` is empty so callers can skip setup for non-Iceberg projects.
 */
export function icebergInitStatements(databases: readonly string[]): string[] {
  if (databases.length === 0) return []

  const stmts = [createCatalogDdl(), "USE CATALOG lakehouse"]
  for (const db of databases) {
    stmts.push(`CREATE DATABASE IF NOT EXISTS \`${db}\``)
  }
  return stmts
}
