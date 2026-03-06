import { createElement } from "@/core/jsx-runtime.js"
import type { BaseComponentProps, ConstructNode } from "@/core/types.js"
import type { CatalogHandle } from "./catalogs.js"

// ── MaterializedTable ──────────────────────────────────────────────

export interface MaterializedTableProps extends BaseComponentProps {
  /** Table name for the materialized table */
  readonly name: string
  /** Catalog handle — materialized tables must target a managed catalog */
  readonly catalog: CatalogHandle
  /** Database within the catalog (uses catalog default if omitted) */
  readonly database?: string
  /** Freshness interval, e.g. "INTERVAL '30' SECOND". Required for Flink < 2.2. */
  readonly freshness?: string
  /** Refresh mode: continuous streaming, periodic full refresh, or automatic (Flink decides) */
  readonly refreshMode?: "continuous" | "full" | "automatic"
  /** Human-readable comment for the table */
  readonly comment?: string
  /** Partition columns */
  readonly partitionedBy?: readonly string[]
  /** Table options (WITH clause) */
  readonly with?: Record<string, string>
  /** Bucketing configuration (Flink 2.2+ only) */
  readonly bucketing?: {
    readonly columns: readonly string[]
    readonly count: number
  }
  /** Upstream query that defines the materialized table */
  readonly children?: ConstructNode | ConstructNode[]
}

/**
 * MaterializedTable: declarative auto-refreshing derived table.
 *
 * Synthesizes to `CREATE MATERIALIZED TABLE ... AS <query>` DDL.
 * Requires Flink >= 2.0 and a managed catalog (Paimon, Iceberg, etc.).
 *
 * Usage:
 * ```tsx
 * <MaterializedTable
 *   name="user_summary"
 *   catalog={paimonCatalog}
 *   freshness="INTERVAL '30' SECOND"
 *   refreshMode="continuous"
 * >
 *   <Aggregate select={{ user_id: 'user_id', cnt: 'COUNT(*)' }} groupBy={['user_id']}>
 *     <KafkaSource topic="events" schema={EventSchema} />
 *   </Aggregate>
 * </MaterializedTable>
 * ```
 */
export function MaterializedTable(
  props: MaterializedTableProps,
): ConstructNode {
  if (!props.name) {
    throw new Error("MaterializedTable requires a name")
  }
  if (!props.catalog) {
    throw new Error("MaterializedTable requires a managed catalog")
  }

  const { children, catalog, ...rest } = props
  const childArray =
    children == null ? [] : Array.isArray(children) ? children : [children]

  return createElement(
    "MaterializedTable",
    {
      ...rest,
      catalogName: catalog.catalogName,
      catalogNodeId: catalog.nodeId,
    },
    ...childArray,
  )
}
