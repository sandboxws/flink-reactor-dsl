import type { ConstructNode } from "@/core/types.js"
import { quoteIdentifier as q } from "./sql/sql-identifiers.js"

/**
 * Catalog and UDF DDL emission. Pure helpers — both take a node and
 * return the DDL statement; no synthesis state is involved.
 */

export function generateCatalogDdl(node: ConstructNode): string {
  const props = node.props
  const name = props.name as string
  const withProps: Record<string, string> = {}

  switch (node.component) {
    case "PaimonCatalog":
      withProps.type = "paimon"
      withProps.warehouse = props.warehouse as string
      if (props.metastore) withProps.metastore = props.metastore as string
      if (props.s3Endpoint)
        withProps["s3.endpoint"] = props.s3Endpoint as string
      if (props.s3AccessKey)
        withProps["s3.access-key"] = props.s3AccessKey as string
      if (props.s3SecretKey)
        withProps["s3.secret-key"] = props.s3SecretKey as string
      if (props.s3PathStyleAccess !== undefined)
        withProps["s3.path-style-access"] = String(props.s3PathStyleAccess)
      break
    case "IcebergCatalog":
      withProps.type = "iceberg"
      withProps["catalog-type"] = props.catalogType as string
      withProps.uri = props.uri as string
      if (props.warehouse) withProps.warehouse = props.warehouse as string
      break
    case "HiveCatalog":
      withProps.type = "hive"
      withProps["hive-conf-dir"] = props.hiveConfDir as string
      break
    case "JdbcCatalog":
      withProps.type = "jdbc"
      withProps["base-url"] = props.baseUrl as string
      withProps["default-database"] = props.defaultDatabase as string
      break
    case "GenericCatalog":
      withProps.type = props.type as string
      if (props.options) {
        Object.assign(withProps, props.options as Record<string, string>)
      }
      break
    case "FlussCatalog":
      withProps.type = "fluss"
      withProps["bootstrap.servers"] = props.bootstrapServers as string
      break
  }

  const withClause = Object.entries(withProps)
    .map(([k, v]) => `  '${k}' = '${v}'`)
    .join(",\n")

  return `CREATE CATALOG ${q(name)} WITH (\n${withClause}\n);`
}

export function generateUdfDdl(node: ConstructNode): string {
  const name = node.props.name as string
  const className = node.props.className as string
  return `CREATE FUNCTION ${q(name)} AS '${className}';`
}
