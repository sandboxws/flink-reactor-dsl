import { createElement } from "../core/jsx-runtime.js"
import type { BaseComponentProps, ConstructNode } from "../core/types.js"
import type { CatalogHandle } from "./catalogs.js"

// ── CatalogSource ───────────────────────────────────────────────────

export interface CatalogSourceProps extends BaseComponentProps {
  readonly catalog: CatalogHandle
  readonly database: string
  readonly table: string
  readonly children?: ConstructNode | ConstructNode[]
}

/**
 * Catalog source: reads from a pre-existing table in a registered catalog.
 *
 * Schema is inferred from the catalog at runtime — no `schema` prop is
 * required. The construct node stores the catalog-qualified reference
 * (`catalog.database.table`) for SQL generation.
 */
export function CatalogSource(props: CatalogSourceProps): ConstructNode {
  const { children, catalog, ...rest } = props
  const childArray =
    children == null ? [] : Array.isArray(children) ? children : [children]

  return createElement(
    "CatalogSource",
    {
      ...rest,
      catalogName: catalog.catalogName,
      catalogNodeId: catalog.nodeId,
    },
    ...childArray,
  )
}
