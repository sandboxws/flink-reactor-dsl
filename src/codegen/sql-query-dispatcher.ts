import type { ConstructNode } from "@/core/types.js"
import { quoteIdentifier as q } from "./sql/sql-identifiers.js"
import type { BuildContext, BuildQueryFn } from "./sql-build-context.js"
import {
  buildAggregateQuery,
  buildWindowQuery,
} from "./sql-query-aggregate-window.js"
import {
  buildMatchRecognizeQuery,
  buildQueryComponentQuery,
  buildRawSqlQuery,
} from "./sql-query-escape.js"
import {
  buildAddFieldQuery,
  buildCastQuery,
  buildCoalesceQuery,
  buildDropQuery,
  buildRenameQuery,
} from "./sql-query-field-ops.js"
import {
  buildIntervalJoinQuery,
  buildJoinQuery,
  buildLateralJoinQuery,
  buildLookupJoinQuery,
  buildTemporalJoinQuery,
} from "./sql-query-joins.js"
import {
  buildSideOutputQuery,
  buildValidateQuery,
} from "./sql-query-side-paths.js"
import {
  buildDeduplicateQuery,
  buildFilterQuery,
  buildFlatMapQuery,
  buildMapQuery,
  buildQualifyQuery,
  buildTopNQuery,
  buildUnionQuery,
} from "./sql-query-transforms.js"

/**
 * The query-building dispatcher.
 *
 * `buildQuery` was historically an 89-case switch — every new component
 * type added another case, and the file accumulated unrelated builders
 * by virtue of co-location. With every builder now in its own focused
 * module, the dispatcher reduces to:
 *
 *   1. Plugin override — first to allow user code to take over a
 *      built-in component (e.g. a custom KafkaSource emitter).
 *   2. Inline special cases — components whose SQL doesn't fit a
 *      "build me a SELECT clause" shape: sources with embedded child
 *      transforms (KafkaSource wrapping a Map), VirtualRef (synthesis
 *      sentinel), CatalogSource (qualified table reference), and View
 *      (named alias). These can't be moved to the registry because
 *      their bodies are inline string-construction, not delegations.
 *   3. Registry lookup — `QUERY_BUILDERS` maps a component name to a
 *      builder function. This is the bulk of the dispatcher and is the
 *      part that grows when a new transform/window component lands —
 *      add one map entry and you're done.
 *   4. Default fallback — recurse into the first child for unknown
 *      components. Preserves backward compatibility with one-off
 *      construct shapes that don't have a registered builder.
 */

type QueryBuilder = (ctx: BuildContext, node: ConstructNode) => string

const QUERY_BUILDERS = new Map<string, QueryBuilder>([
  // Transforms
  ["Filter", buildFilterQuery],
  ["Map", buildMapQuery],
  ["FlatMap", buildFlatMapQuery],
  ["Aggregate", buildAggregateQuery],
  ["Union", buildUnionQuery],
  ["Deduplicate", buildDeduplicateQuery],
  ["TopN", buildTopNQuery],

  // Field transforms
  ["Rename", buildRenameQuery],
  ["Drop", buildDropQuery],
  ["Cast", buildCastQuery],
  ["Coalesce", buildCoalesceQuery],
  ["AddField", buildAddFieldQuery],

  // Joins
  ["Join", buildJoinQuery],
  ["TemporalJoin", buildTemporalJoinQuery],
  ["LookupJoin", buildLookupJoinQuery],
  ["IntervalJoin", buildIntervalJoinQuery],
  ["LateralJoin", buildLateralJoinQuery],

  // Windows — three variants share buildWindowQuery
  ["TumbleWindow", buildWindowQuery],
  ["SlideWindow", buildWindowQuery],
  ["SessionWindow", buildWindowQuery],

  // Escape hatches
  ["Query", buildQueryComponentQuery],
  ["Qualify", buildQualifyQuery],
  ["MatchRecognize", buildMatchRecognizeQuery],

  // Side paths — main-path SELECT only; the side-path INSERTs are
  // emitted by `collectSideDml` in `sql-query-side-paths.ts`.
  ["Validate", buildValidateQuery],
  ["SideOutput", buildSideOutputQuery],
])

const SOURCE_COMPONENTS = new Set([
  "KafkaSource",
  "JdbcSource",
  "GenericSource",
  "DataGenSource",
  "FlussSource",
])

export const buildQuery: BuildQueryFn = (ctx, node) => {
  // 1. Plugin overrides win over built-ins so users can replace any
  //    component's SQL emission without forking the DSL.
  const pluginGen = ctx.pluginSqlGenerators?.get(node.component)
  if (pluginGen) {
    return pluginGen(node, ctx.nodeIndex)
  }

  // 2. Inline special cases.

  if (node.component === "VirtualRef") {
    if (node.props._sql) return node.props._sql as string
    return `SELECT * FROM ${q(node.id)}`
  }

  if (SOURCE_COMPONENTS.has(node.component)) {
    if (node.children.length > 0) {
      // Source wraps child transforms (e.g. <KafkaSource><Map/></KafkaSource>)
      // Build query by chaining children on top of the source reference.
      let result = `SELECT * FROM ${q(node.id)}`
      for (const child of node.children) {
        const virtualSource: ConstructNode = {
          id: node.id,
          kind: "Source",
          component: "VirtualRef",
          props: {},
          children: [],
        }
        const saved = child.children
        ;(child as { children: ConstructNode[] }).children = [virtualSource]
        result = buildQuery(ctx, child)
        ;(child as { children: ConstructNode[] }).children = saved
      }
      return result
    }
    return `SELECT * FROM ${q(node.id)}`
  }

  if (node.component === "CatalogSource") {
    return `SELECT * FROM ${q(String(node.props.catalogName))}.${q(String(node.props.database))}.${q(String(node.props.table))}`
  }

  if (node.component === "RawSQL") {
    return buildRawSqlQuery(node)
  }

  if (node.component === "View") {
    return `SELECT * FROM ${q(node.props.name as string)}`
  }

  // 3. Registry lookup — the bulk of the dispatch.
  const builder = QUERY_BUILDERS.get(node.component)
  if (builder) return builder(ctx, node)

  // 4. Default fallback — recurse into the first child for unknown
  //    components. Preserves the historical behavior.
  if (node.children.length > 0) {
    return buildQuery(ctx, node.children[0])
  }
  return `SELECT * FROM ${q(node.id)}`
}
