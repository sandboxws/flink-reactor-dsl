/**
 * Canonical component inventory for flink-reactor.
 *
 * This is the single source of truth for all DSL components known
 * to the plugin. When a new component is added to the DSL, it must
 * be added here — parity tests will fail otherwise.
 *
 * The inventory mirrors the BUILTIN_KINDS map from
 * `src/core/jsx-runtime.ts` in the main DSL package.
 */

/** Component kind categories matching the DSL's NodeKind type */
export type ComponentKind =
  | "Pipeline"
  | "Source"
  | "Sink"
  | "Transform"
  | "Join"
  | "Window"
  | "Catalog"
  | "RawSQL"
  | "UDF"
  | "CEP"
  | "View"
  | "MaterializedTable"
  | "Qualify"

/** Canonical mapping of every DSL component to its kind */
export const DSL_COMPONENTS: ReadonlyMap<string, ComponentKind> = new Map([
  ["Pipeline", "Pipeline"],
  // Sources
  ["KafkaSource", "Source"],
  ["JdbcSource", "Source"],
  ["GenericSource", "Source"],
  ["CatalogSource", "Source"],
  // Sinks
  ["KafkaSink", "Sink"],
  ["JdbcSink", "Sink"],
  ["FileSystemSink", "Sink"],
  ["PaimonSink", "Sink"],
  ["IcebergSink", "Sink"],
  ["GenericSink", "Sink"],
  // Transforms
  ["Filter", "Transform"],
  ["Map", "Transform"],
  ["FlatMap", "Transform"],
  ["Aggregate", "Transform"],
  ["Union", "Transform"],
  ["Deduplicate", "Transform"],
  ["TopN", "Transform"],
  ["Route", "Transform"],
  ["Rename", "Transform"],
  ["Drop", "Transform"],
  ["Cast", "Transform"],
  ["Coalesce", "Transform"],
  ["AddField", "Transform"],
  ["Query", "Transform"],
  ["SideOutput", "Transform"],
  ["SideOutput.Sink", "Transform"],
  ["Validate", "Transform"],
  ["Validate.Reject", "Transform"],
  // Joins
  ["Join", "Join"],
  ["TemporalJoin", "Join"],
  ["LookupJoin", "Join"],
  ["IntervalJoin", "Join"],
  ["LateralJoin", "Join"],
  // Windows
  ["TumbleWindow", "Window"],
  ["SlideWindow", "Window"],
  ["SessionWindow", "Window"],
  // Catalogs
  ["PaimonCatalog", "Catalog"],
  ["IcebergCatalog", "Catalog"],
  ["HiveCatalog", "Catalog"],
  ["JdbcCatalog", "Catalog"],
  ["GenericCatalog", "Catalog"],
  // Specialized
  ["RawSQL", "RawSQL"],
  ["UDF", "UDF"],
  ["MatchRecognize", "CEP"],
  ["View", "View"],
  ["MaterializedTable", "MaterializedTable"],
  ["Qualify", "Qualify"],
])

/** Get all component names of a given kind */
export function getComponentsByKind(kind: ComponentKind): string[] {
  const result: string[] = []
  for (const [name, k] of DSL_COMPONENTS) {
    if (k === kind) result.push(name)
  }
  return result
}

/** Get all top-level component names (not sub-components like Route.Branch) */
export function getTopLevelComponents(): string[] {
  return [...DSL_COMPONENTS.keys()].filter((name) => !name.includes("."))
}

/** Get all sub-components (e.g., Route.Branch, Query.Select) */
export function getSubComponents(): string[] {
  return [...DSL_COMPONENTS.keys()].filter((name) => name.includes("."))
}

/**
 * Sub-component names known to the hierarchy but NOT in BUILTIN_KINDS.
 *
 * These are valid in the component hierarchy (e.g., Query.Select is a
 * valid child of Query) but are implemented as part of their parent
 * component rather than as standalone DSL nodes.
 */
export const HIERARCHY_ONLY_COMPONENTS: readonly string[] = [
  "Route.Branch",
  "Route.Default",
  "Query.Select",
  "Query.Where",
  "Query.GroupBy",
  "Query.Having",
  "Query.OrderBy",
]
