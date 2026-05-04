import { createElement } from "@/core/jsx-runtime.js"
import type { SecretRef } from "@/core/secret-ref.js"
import type { BaseComponentProps, ConstructNode } from "@/core/types.js"

// ── Catalog handle ──────────────────────────────────────────────────

/**
 * A lightweight reference to a registered catalog.
 * Passed to CatalogSource, PaimonSink, IcebergSink, etc.
 * to form catalog-qualified table names (catalog.database.table).
 */
export interface CatalogHandle {
  readonly _tag: "CatalogHandle"
  readonly catalogName: string
  readonly nodeId: string
}

function createCatalogHandle(name: string, nodeId: string): CatalogHandle {
  return { _tag: "CatalogHandle", catalogName: name, nodeId }
}

// ── Catalog result ──────────────────────────────────────────────────

export interface CatalogResult {
  readonly node: ConstructNode
  readonly handle: CatalogHandle
}

// ── PaimonCatalog ───────────────────────────────────────────────────

export interface PaimonCatalogProps extends BaseComponentProps {
  readonly name: string
  readonly warehouse: string
  readonly metastore?: "filesystem" | "hive"
  /**
   * S3 endpoint for `s3a://`/`s3://` warehouses (e.g. `http://seaweedfs:8333`).
   * Required when using a non-AWS S3-compatible store; without it Paimon
   * defaults to AWS endpoints. Ignored when warehouse uses `file://` etc.
   */
  readonly s3Endpoint?: string
  /**
   * S3 access/secret keys are passed as plain strings: SQL `CREATE CATALOG`
   * DDL is sent verbatim to the gateway, and Flink does not resolve
   * `${env:VAR}` placeholders inside catalog options. For real deployments,
   * inject the value at synth time via `process.env.S3_KEY` rather than a
   * `secretRef()`.
   */
  readonly s3AccessKey?: string
  readonly s3SecretKey?: string
  /**
   * Whether to use path-style addressing (`http://endpoint/bucket/key`)
   * instead of virtual-hosted-style (`http://bucket.endpoint/key`).
   * Required for SeaweedFS, MinIO, and most non-AWS S3 stores.
   */
  readonly s3PathStyleAccess?: boolean
  readonly children?: ConstructNode | ConstructNode[]
}

/**
 * Paimon catalog: registers an Apache Paimon catalog backed by a warehouse path.
 *
 * The optional `metastore` prop controls whether Paimon uses the filesystem
 * (default) or a Hive metastore for metadata management.
 *
 * For `s3a://`/`s3://` warehouses, supply `s3Endpoint`/`s3AccessKey`/
 * `s3SecretKey`/`s3PathStyleAccess` so the emitted CREATE CATALOG DDL can
 * reach a non-AWS S3 store. Without these, Paimon's HadoopFileIO falls back
 * to the default AWS credentials/endpoint chain at deploy time.
 */
export function PaimonCatalog(props: PaimonCatalogProps): CatalogResult {
  const { children, ...rest } = props
  const childArray =
    children == null ? [] : Array.isArray(children) ? children : [children]

  const node = createElement("PaimonCatalog", { ...rest }, ...childArray)
  const handle = createCatalogHandle(props.name, node.id)
  return { node, handle }
}

// ── IcebergCatalog ──────────────────────────────────────────────────

export type IcebergCatalogType = "hive" | "hadoop" | "rest"

export interface IcebergCatalogProps extends BaseComponentProps {
  readonly name: string
  readonly catalogType: IcebergCatalogType
  readonly uri: string
  /**
   * Warehouse identifier passed to the catalog server. Required by REST
   * catalogs that host multiple warehouses (e.g. Lakekeeper) — the value is
   * the registered warehouse name there. Optional for single-warehouse
   * servers (e.g. tabulario/iceberg-rest).
   */
  readonly warehouse?: string
  readonly children?: ConstructNode | ConstructNode[]
}

/**
 * Iceberg catalog: registers an Apache Iceberg catalog.
 *
 * `catalogType` selects the catalog backend (Hive Metastore, Hadoop, REST).
 * `uri` is the connection URI for the catalog service.
 */
export function IcebergCatalog(props: IcebergCatalogProps): CatalogResult {
  const { children, ...rest } = props
  const childArray =
    children == null ? [] : Array.isArray(children) ? children : [children]

  const node = createElement("IcebergCatalog", { ...rest }, ...childArray)
  const handle = createCatalogHandle(props.name, node.id)
  return { node, handle }
}

// ── HiveCatalog ─────────────────────────────────────────────────────

export interface HiveCatalogProps extends BaseComponentProps {
  readonly name: string
  readonly hiveConfDir: string
  readonly children?: ConstructNode | ConstructNode[]
}

/**
 * Hive catalog: registers a Hive Metastore catalog.
 *
 * `hiveConfDir` points to the directory containing hive-site.xml.
 */
export function HiveCatalog(props: HiveCatalogProps): CatalogResult {
  const { children, ...rest } = props
  const childArray =
    children == null ? [] : Array.isArray(children) ? children : [children]

  const node = createElement("HiveCatalog", { ...rest }, ...childArray)
  const handle = createCatalogHandle(props.name, node.id)
  return { node, handle }
}

// ── JdbcCatalog ─────────────────────────────────────────────────────

export interface JdbcCatalogProps extends BaseComponentProps {
  readonly name: string
  readonly baseUrl: string
  readonly defaultDatabase: string
  readonly children?: ConstructNode | ConstructNode[]
}

/**
 * JDBC catalog: registers a JDBC-based catalog (e.g., PostgreSQL, MySQL).
 *
 * `baseUrl` is the JDBC connection URL without the database name.
 * `defaultDatabase` is the initial database to use.
 */
export function JdbcCatalog(props: JdbcCatalogProps): CatalogResult {
  const { children, ...rest } = props
  const childArray =
    children == null ? [] : Array.isArray(children) ? children : [children]

  const node = createElement("JdbcCatalog", { ...rest }, ...childArray)
  const handle = createCatalogHandle(props.name, node.id)
  return { node, handle }
}

// ── FlussCatalog ────────────────────────────────────────────────────

export interface FlussCatalogProps extends BaseComponentProps {
  readonly name: string
  /** Fluss coordinator/server bootstrap addresses, e.g. `host:9123,host2:9123`. */
  readonly bootstrapServers: string
  /**
   * Optional SASL credentials. When set, the synthesis engine forwards these
   * onto the connector configuration as `client.security.*` properties. The
   * password may be supplied as a `SecretRef` to render an `${env:VAR}`
   * placeholder in YAML and a matching `secretKeyRef` env entry in the CRD.
   */
  readonly securityProtocol?: string
  readonly saslMechanism?: string
  readonly saslUsername?: string
  readonly saslPassword?: string | SecretRef
  readonly children?: ConstructNode | ConstructNode[]
}

/**
 * Fluss catalog: registers an Apache Fluss catalog reachable via the Fluss
 * coordinator/server `bootstrap.servers` endpoint.
 *
 * Fluss tables come in two flavors:
 *
 *   • **Log table** — append-only, no `primaryKey`; reads produce an
 *     `'append-only'` changelog stream.
 *   • **PrimaryKey table** — declared with `primaryKey`; reads produce a
 *     `'retract'`/upsert changelog stream and writes are upsert by key.
 *
 * The `bootstrapServers` value is also threaded into `FlussSource` /
 * `FlussSink` via the catalog handle so downstream connector DDL inherits a
 * consistent connection target.
 */
export function FlussCatalog(props: FlussCatalogProps): CatalogResult {
  const { children, ...rest } = props
  const childArray =
    children == null ? [] : Array.isArray(children) ? children : [children]

  const node = createElement("FlussCatalog", { ...rest }, ...childArray)
  const handle = createCatalogHandle(props.name, node.id)
  return { node, handle }
}

// ── GenericCatalog ──────────────────────────────────────────────────

export interface GenericCatalogProps extends BaseComponentProps {
  readonly name: string
  readonly type: string
  readonly options?: Record<string, string>
  readonly children?: ConstructNode | ConstructNode[]
}

/**
 * Generic catalog: escape hatch for any Flink SQL catalog type
 * not covered by the specialized catalog components.
 *
 * The `type` identifier and all `options` are passed through
 * to the CREATE CATALOG DDL during code generation.
 */
export function GenericCatalog(props: GenericCatalogProps): CatalogResult {
  const { children, ...rest } = props
  const childArray =
    children == null ? [] : Array.isArray(children) ? children : [children]

  const node = createElement("GenericCatalog", { ...rest }, ...childArray)
  const handle = createCatalogHandle(props.name, node.id)
  return { node, handle }
}
