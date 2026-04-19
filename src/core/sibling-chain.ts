import type { ConstructNode } from "./types.js"

/**
 * Resolves sink↔source bindings that arise from **sibling-chain** topology —
 * the linear-JSX form where a Source, optional Transforms, and a Sink sit
 * adjacent under a common parent (typically `<Pipeline>` or `<StatementSet>`):
 *
 *   <Pipeline>
 *     <KafkaSource … />
 *     <Filter …/>
 *     <KafkaSink … />        ← bound to KafkaSource via preceding siblings
 *   </Pipeline>
 *
 * Sinks declared with explicit `children` nesting (`IcebergSink({ children:[src] })`)
 * are handled by the graph model in SynthContext and are NOT returned here.
 *
 * This mirrors the rules used by the SQL generator's `buildSiblingChainQuery`
 * and the Pattern 2 walk in `resolveSinkSchema`, so the validator and codegen
 * agree on what "connected" means.
 */
export interface SiblingChainResolution {
  /** sinkId → id of the source feeding it via sibling chain */
  readonly sinkToSource: ReadonlyMap<string, string>
  /** sourceId → set of sink ids it feeds via sibling chain */
  readonly sourceToSinks: ReadonlyMap<string, ReadonlySet<string>>
}

export function resolveSiblingChains(
  root: ConstructNode,
): SiblingChainResolution {
  const sinkToSource = new Map<string, string>()
  const sourceToSinks = new Map<string, Set<string>>()

  const record = (sourceId: string, sinkId: string): void => {
    sinkToSource.set(sinkId, sourceId)
    let set = sourceToSinks.get(sourceId)
    if (!set) {
      set = new Set()
      sourceToSinks.set(sourceId, set)
    }
    set.add(sinkId)
  }

  const walk = (node: ConstructNode): void => {
    const kids = node.children
    for (let i = 0; i < kids.length; i++) {
      const child = kids[i]

      // Sink with no children → needs a preceding sibling to be "connected"
      if (child.kind === "Sink" && child.children.length === 0) {
        const sourceId = findPrecedingSourceId(kids, i)
        if (sourceId) record(sourceId, child.id)
      }

      // Route: the source before the Route feeds every Sink in each branch
      if (child.component === "Route") {
        const sourceId = findPrecedingSourceId(kids, i)
        if (sourceId) {
          for (const branch of child.children) {
            for (const grand of branch.children) {
              if (grand.kind === "Sink") record(sourceId, grand.id)
            }
          }
        }
      }
    }

    for (const child of kids) walk(child)
  }

  walk(root)
  return { sinkToSource, sourceToSinks }
}

/**
 * Walk backward through a sibling list starting at `from - 1` and return the
 * id of the nearest Source-like node. A Source-like node is:
 *   - a Source, or
 *   - a self-contained Transform/Window/Join/CEP — one that has its own
 *     children and contains a Source descendant (e.g. `Union` wrapping
 *     sources, `LookupJoin` with its right-hand table attached).
 */
function findPrecedingSourceId(
  siblings: readonly ConstructNode[],
  from: number,
): string | null {
  for (let i = from - 1; i >= 0; i--) {
    const sibling = siblings[i]
    if (sibling.kind === "Source") return sibling.id
    if (
      (sibling.kind === "Transform" ||
        sibling.kind === "Window" ||
        sibling.kind === "Join" ||
        sibling.kind === "CEP") &&
      sibling.children.length > 0
    ) {
      const deep = findDeepestSourceId(sibling)
      if (deep) return deep
    }
  }
  return null
}

function findDeepestSourceId(node: ConstructNode): string | null {
  if (node.kind === "Source") return node.id
  for (const child of node.children) {
    const found = findDeepestSourceId(child)
    if (found) return found
  }
  return null
}
