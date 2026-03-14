/**
 * Rule parity verification.
 *
 * Compares the plugin's component rules against the canonical
 * component inventory and produces actionable mismatch reports.
 * Can be used in tests (CI gate) or at plugin init time (log warnings).
 */
import {
  DSL_COMPONENTS,
  HIERARCHY_ONLY_COMPONENTS,
  getComponentsByKind,
} from "./component-inventory"
import type { ComponentRulesRegistry } from "./types"

export interface ParityMismatch {
  /** What kind of mismatch was detected */
  type: "missing_from_rules" | "unknown_in_rules" | "missing_from_inventory"
  /** Component name(s) involved */
  components: string[]
  /** Human-readable description of the issue */
  message: string
  /** Suggested fix */
  fix: string
}

/**
 * Run a full parity check between the rules registry and the
 * canonical component inventory. Returns an array of mismatches;
 * empty means everything is in sync.
 */
export function checkRuleParity(
  registry: ComponentRulesRegistry,
): ParityMismatch[] {
  const mismatches: ParityMismatch[] = []
  const allKnown = new Set([
    ...DSL_COMPONENTS.keys(),
    ...HIERARCHY_ONLY_COMPONENTS,
  ])

  // Check: every top-level DSL component appears in Pipeline rules
  const pipelineChildren = registry.getAllowedChildren("Pipeline")
  if (Array.isArray(pipelineChildren)) {
    const pipelineSet = new Set(pipelineChildren)
    const kinds = [
      "Source",
      "Sink",
      "Transform",
      "Join",
      "Window",
      "RawSQL",
      "UDF",
      "CEP",
      "View",
      "MaterializedTable",
      "Qualify",
    ] as const
    for (const kind of kinds) {
      const components = getComponentsByKind(kind).filter(
        (c) => !c.includes("."),
      )
      const missing = components.filter((c) => !pipelineSet.has(c))
      if (missing.length > 0) {
        mismatches.push({
          type: "missing_from_rules",
          components: missing,
          message: `${kind} component(s) not in Pipeline rules: ${missing.join(", ")}`,
          fix: `Add ${missing.map((c) => `"${c}"`).join(", ")} to Pipeline's allowed children in component-rules.ts`,
        })
      }
    }
  }

  // Check: every hierarchy-only component is referenced in rules
  const allReferenced = new Set(registry.getAllReferencedComponents())
  const missingHierarchy = HIERARCHY_ONLY_COMPONENTS.filter(
    (c) => !allReferenced.has(c),
  )
  if (missingHierarchy.length > 0) {
    mismatches.push({
      type: "missing_from_rules",
      components: [...missingHierarchy],
      message: `Hierarchy sub-components not referenced in any rule: ${missingHierarchy.join(", ")}`,
      fix: "Add these as allowed children of their parent component in component-rules.ts",
    })
  }

  // Check: every rule references only known components
  for (const parent of registry.getRegisteredParents()) {
    if (!allKnown.has(parent)) {
      mismatches.push({
        type: "unknown_in_rules",
        components: [parent],
        message: `Parent "${parent}" in rules is not in the component inventory`,
        fix: `Add "${parent}" to component-inventory.ts or remove it from component-rules.ts`,
      })
    }

    const children = registry.getAllowedChildren(parent)
    if (Array.isArray(children)) {
      const unknown = children.filter((c) => !allKnown.has(c))
      if (unknown.length > 0) {
        mismatches.push({
          type: "unknown_in_rules",
          components: unknown,
          message: `Children of "${parent}" not in inventory: ${unknown.join(", ")}`,
          fix: `Add ${unknown.map((c) => `"${c}"`).join(", ")} to component-inventory.ts or remove from "${parent}" rules`,
        })
      }
    }
  }

  return mismatches
}

/**
 * Format parity mismatches into a human-readable report.
 */
export function formatParityReport(mismatches: ParityMismatch[]): string {
  if (mismatches.length === 0) {
    return "Rule parity check passed: all components are in sync."
  }

  const lines = [`Rule parity check failed with ${mismatches.length} issue(s):\n`]
  for (const m of mismatches) {
    lines.push(`  [${m.type}] ${m.message}`)
    lines.push(`    Fix: ${m.fix}`)
    lines.push("")
  }
  return lines.join("\n")
}
