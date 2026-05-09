import type { ConstructNode } from "@/core/types.js"
import {
  collectTransformChain,
  findDeepestSource,
  type ResolvedColumn,
  resolveNodeSchema,
  resolveTransformSchema,
} from "../schema-introspect.js"
import type { BuildContext } from "../sql-build-context.js"
import { sameNameJoinKeys } from "./sql-join-helpers.js"

/**
 * Sink metadata resolution — derives the schema, changelog mode, and
 * primary key for each Sink in a pipeline.
 *
 * `resolveSinkMetadata` is the single public entry point. It walks the
 * construct tree once and returns a `Map<sinkId, SinkMetadata>` consumed
 * by the sink-side DDL emitter (CREATE TABLE) and the DML insert-target
 * builder. Pulling this out of the SQL generator keeps the changelog-mode
 * and PK-propagation rules in one focused file — both are subtle,
 * cross-cutting decisions that affect downstream connector emission
 * (upsert-kafka vs. kafka, primary key declaration, retract semantics).
 */

export type ChangelogMode = "append-only" | "retract"

export interface SinkMetadata {
  readonly schema: ResolvedColumn[]
  readonly changelogMode: ChangelogMode
  readonly primaryKey?: readonly string[]
}

/**
 * Walk the pipeline tree to resolve metadata (schema, changelog mode, primary key) for all sinks.
 *
 * Handles both patterns:
 * - Reverse-nesting (Sink → Transform → Source): sink's children are its upstream
 * - Forward-reading with Route (Pipeline → Source, Route → Branch → Transform, Sink):
 *   sink's upstream comes from the Route's source via branch transforms
 */
export function resolveSinkMetadata(
  ctx: BuildContext,
  pipelineNode: ConstructNode,
): Map<string, SinkMetadata> {
  const result = new Map<string, SinkMetadata>()

  function resolveSourceChangelogMode(source: ConstructNode): ChangelogMode {
    const mode = source.props.changelogMode as string | undefined
    if (mode === "retract") return "retract"
    // A KafkaSource with a schema primary key is emitted as upsert-kafka,
    // which carries upsert (retract-equivalent) changelog semantics even
    // though its format (json/avro) isn't listed as CDC by inferChangelogMode.
    if (source.component === "KafkaSource") {
      const schema = source.props.schema as
        | { primaryKey?: { columns?: readonly string[] } }
        | undefined
      if ((schema?.primaryKey?.columns?.length ?? 0) > 0) return "retract"
    }
    return "append-only"
  }

  function resolveSourcePrimaryKey(
    source: ConstructNode,
  ): readonly string[] | undefined {
    const schema = source.props.schema as
      | { primaryKey?: { columns: readonly string[] } }
      | undefined
    if (schema?.primaryKey?.columns) return schema.primaryKey.columns
    // Sources whose schema doesn't carry a PK but whose component-level prop
    // does (e.g. FlussSource on a Fluss PrimaryKey table) — surface the prop
    // PK so downstream sinks can see the upstream key.
    const propPk = source.props.primaryKey as readonly string[] | undefined
    if (Array.isArray(propPk) && propPk.length > 0) return propPk
    return undefined
  }

  /**
   * Propagate changelog mode through a transform chain.
   *
   * - Unbounded Aggregate (no window) → retract (emits updates as values change)
   * - Windowed Aggregate (inside TumbleWindow/SlideWindow/SessionWindow) → append-only
   *   (each window fires a single final result)
   * - Other transforms preserve the upstream changelog mode.
   */
  function propagateChangelogMode(
    transforms: readonly ConstructNode[],
    upstreamMode: ChangelogMode,
  ): ChangelogMode {
    let mode = upstreamMode
    const hasWindow = transforms.some((t) => t.kind === "Window")
    for (const t of transforms) {
      // Only unbounded aggregation produces retract; windowed aggregation is append-only
      if (t.component === "Aggregate" && !hasWindow) {
        mode = "retract"
      }
      // Session windows produce retract (sessions can merge on late events)
      if (t.component === "SessionWindow") {
        mode = "retract"
      }
    }
    return mode
  }

  /**
   * Propagate primary key through a transform chain.
   * - Filter/Map/Deduplicate/TopN: PK preserved from upstream
   * - Aggregate: PK becomes the groupBy columns
   * - Window with Aggregate child: PK becomes the groupBy columns
   */
  function propagatePrimaryKey(
    transforms: readonly ConstructNode[],
    upstreamPk: readonly string[] | undefined,
  ): readonly string[] | undefined {
    let pk = upstreamPk
    for (const t of transforms) {
      if (t.component === "Aggregate") {
        pk = t.props.groupBy as readonly string[]
      } else if (t.kind === "Window") {
        // Extract PK from Aggregate child inside the window
        const aggChild = t.children?.find((c) => c.component === "Aggregate")
        if (aggChild) {
          pk = aggChild.props.groupBy as readonly string[]
        }
      } else if (t.component === "Rename" && pk) {
        const columns = t.props.columns as Record<string, string>
        pk = pk.map((col) => columns[col] ?? col)
      } else if (t.component === "Drop" && pk) {
        const dropCols = new Set(t.props.columns as readonly string[])
        if (pk.some((col) => dropCols.has(col))) {
          pk = undefined
        }
      }
    }
    return pk
  }

  function walk(node: ConstructNode, parent?: ConstructNode): void {
    if (node.kind === "Sink") {
      // Pattern 1: sink has children (reverse-nesting)
      if (node.children.length > 0) {
        const upstream = node.children[0]
        const schema = resolveNodeSchema(upstream, ctx.nodeIndex)
        // Walk to source to get changelog mode
        const sourceNode = findDeepestSource(upstream)
        const changelogMode = sourceNode
          ? propagateChangelogMode(
              collectTransformChain(node.children[0]),
              resolveSourceChangelogMode(sourceNode),
            )
          : "append-only"
        const primaryKey = sourceNode
          ? propagatePrimaryKey(
              collectTransformChain(node.children[0]),
              resolveSourcePrimaryKey(sourceNode),
            )
          : undefined
        if (schema) result.set(node.id, { schema, changelogMode, primaryKey })
        return
      }
      // Pattern 2: forward-reading JSX — resolve from preceding siblings
      if (parent) {
        const sinkIndex = parent.children.indexOf(node)
        let schema: ResolvedColumn[] | null = null
        let changelogMode: ChangelogMode = "append-only"
        let primaryKey: readonly string[] | undefined
        for (let i = sinkIndex - 1; i >= 0; i--) {
          const sibling = parent.children[i]
          // Start from Sources, or from self-contained nodes that embed their
          // own source data (e.g. Union with source children, LookupJoin).
          // Window nodes with only Aggregate children are NOT self-contained —
          // they need upstream data and are applied as intermediate transforms.
          if (sibling.kind === "Source") {
            // Sources always resolve
          } else if (sibling.kind === "RawSQL") {
            // RawSQL is source-like — declares its own outputSchema
          } else if (
            (sibling.kind === "Transform" ||
              sibling.kind === "Window" ||
              sibling.kind === "Join" ||
              sibling.kind === "CEP") &&
            sibling.children.length > 0 &&
            findDeepestSource(sibling) !== null
          ) {
            // Self-contained node (has its own source data)
          } else {
            continue
          }
          const startSchema = resolveNodeSchema(sibling, ctx.nodeIndex)
          if (!startSchema) continue
          schema = startSchema
          if (sibling.kind === "Source") {
            changelogMode = resolveSourceChangelogMode(sibling)
            primaryKey = resolveSourcePrimaryKey(sibling)
          } else if (sibling.kind === "Join") {
            // Anti/semi joins produce retract (result changes when right side changes)
            const joinType = sibling.props.type as string | undefined
            if (joinType === "anti" || joinType === "semi") {
              changelogMode = "retract"
            }
            // Inner/left/right/full joins are retract when either input is retract
            // (e.g. CDC / upsert-kafka source on the broadcast side of a join).
            const left = sibling.children[0]
            const right = sibling.children[1]
            const leftDeepest = left ? findDeepestSource(left) : null
            const rightDeepest = right ? findDeepestSource(right) : null
            if (
              (leftDeepest &&
                resolveSourceChangelogMode(leftDeepest) === "retract") ||
              (rightDeepest &&
                resolveSourceChangelogMode(rightDeepest) === "retract")
            ) {
              changelogMode = "retract"
            }
            // Prefer a same-name join key as the sink's PK (natural key of
            // the joined result). Fall back to the left source's PK.
            const onClause = sibling.props.on as string | undefined
            const sharedKeys = onClause ? sameNameJoinKeys(onClause) : null
            if (sharedKeys) {
              primaryKey = sharedKeys
            } else if (leftDeepest) {
              primaryKey = resolveSourcePrimaryKey(leftDeepest)
            }
          }
          // Propagate through intermediate transforms
          const transforms: ConstructNode[] = []
          for (let j = i + 1; j < sinkIndex; j++) {
            const transform = parent.children[j]
            if (
              transform.kind === "Transform" ||
              transform.kind === "Window" ||
              transform.kind === "RawSQL"
            ) {
              transforms.push(transform)
              if (schema) schema = resolveTransformSchema(transform, schema)
            }
          }
          changelogMode = propagateChangelogMode(transforms, changelogMode)
          primaryKey = propagatePrimaryKey(transforms, primaryKey)
          break
        }
        if (schema) result.set(node.id, { schema, changelogMode, primaryKey })
      }
      return
    }

    if (node.component === "Route") {
      // Resolve upstream schema for the Route using the same backward walk
      // as sink resolution — find the Source and propagate through transforms.
      let routeSchema: ResolvedColumn[] | null = null
      let baseChangelogMode: ChangelogMode = "append-only"
      let basePk: readonly string[] | undefined
      if (parent) {
        const routeIndex = parent.children.indexOf(node)
        for (let i = routeIndex - 1; i >= 0; i--) {
          const sibling = parent.children[i]
          if (sibling.kind === "Source") {
            // Source — resolve directly
          } else if (sibling.kind === "RawSQL") {
            // RawSQL is source-like — declares its own outputSchema
          } else if (
            (sibling.kind === "Transform" ||
              sibling.kind === "Window" ||
              sibling.kind === "Join" ||
              sibling.kind === "CEP") &&
            sibling.children.length > 0 &&
            findDeepestSource(sibling) !== null
          ) {
            // Self-contained node
          } else {
            continue
          }
          const startSchema = resolveNodeSchema(sibling, ctx.nodeIndex)
          if (!startSchema) continue
          routeSchema = startSchema
          if (sibling.kind === "Source") {
            baseChangelogMode = resolveSourceChangelogMode(sibling)
            basePk = resolveSourcePrimaryKey(sibling)
          } else if (sibling.kind === "Join") {
            const joinType = sibling.props.type as string | undefined
            if (joinType === "anti" || joinType === "semi") {
              baseChangelogMode = "retract"
            }
            const leftSource = findDeepestSource(sibling)
            if (leftSource) basePk = resolveSourcePrimaryKey(leftSource)
          }
          // Propagate through intermediate transforms between start and Route
          const transforms: ConstructNode[] = []
          for (let j = i + 1; j < routeIndex; j++) {
            const transform = parent.children[j]
            if (
              transform.kind === "Transform" ||
              transform.kind === "Window" ||
              transform.kind === "RawSQL"
            ) {
              transforms.push(transform)
              if (routeSchema)
                routeSchema = resolveTransformSchema(transform, routeSchema)
            }
          }
          baseChangelogMode = propagateChangelogMode(
            transforms,
            baseChangelogMode,
          )
          basePk = propagatePrimaryKey(transforms, basePk)
          break
        }
      }

      // Resolve metadata for each sink in each branch
      const branches = node.children.filter(
        (c) =>
          c.component === "Route.Branch" || c.component === "Route.Default",
      )
      for (const branch of branches) {
        const transforms = branch.children.filter((c) => c.kind !== "Sink")
        const sinks = branch.children.filter((c) => c.kind === "Sink")

        // Propagate schema through transforms
        let schema = routeSchema
        for (const transform of transforms) {
          if (!schema) break
          schema = resolveTransformSchema(transform, schema)
        }

        const changelogMode = propagateChangelogMode(
          transforms,
          baseChangelogMode,
        )
        const primaryKey = propagatePrimaryKey(transforms, basePk)

        if (schema) {
          for (const sink of sinks) {
            result.set(sink.id, { schema, changelogMode, primaryKey })
          }
        }
      }
      return
    }

    // Recurse
    for (const child of node.children) {
      walk(child, node)
    }
  }

  walk(pipelineNode)
  return result
}
