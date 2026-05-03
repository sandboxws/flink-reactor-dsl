// ── Synth-time validator: pipelines must agree with `services` ──────
//
// Walks each pipeline's connector usage and cross-checks it against the
// project's resolved `services` block. Errors at synth time so problems
// surface in `fr deploy`, `fr cluster up`, CI, and pre-commit hooks —
// before any cluster contact.

import {
  CONNECTOR_TO_SERVICE,
  type ServiceKind,
} from "@/codegen/connector-registry.js"
import { collectConnectorIds } from "@/codegen/connector-resolver.js"
import type { ConstructNode } from "./types.js"

export interface ValidatedPipeline {
  readonly name: string
  readonly node: ConstructNode
}

/**
 * Run cross-checks between a project's pipelines and its `services` block.
 *
 * Throws on the first violation with a message naming the pipeline and
 * the missing service. The thrown error includes a `code` property of
 * `"missing_service"` for callers that want structured handling.
 */
export function validateServicesAgainstPipelines(
  pipelines: readonly ValidatedPipeline[],
  services: Readonly<Record<string, unknown>> | undefined,
): void {
  const declared = collectDeclaredServices(services)

  for (const { name, node } of pipelines) {
    const connectorIds = collectConnectorIds(node)
    for (const id of connectorIds) {
      const required = CONNECTOR_TO_SERVICE[id]
      if (required === undefined) continue // connector has no infra dep
      if (!declared.has(required)) {
        const err = new Error(
          `pipeline '${name}' uses connector '${id}' but ` +
            `\`services.${required}\` is not declared in flink-reactor.config.ts. ` +
            `Add \`services: { ${required}: {} }\` (or set it for this environment) ` +
            `to enable ${required} for this project.`,
        ) as Error & { code?: string }
        err.code = "missing_service"
        throw err
      }
    }
  }
}

/**
 * Reduce a resolved `services` block to the set of service kinds that are
 * actually enabled — i.e. present and not explicitly set to `false`.
 */
function collectDeclaredServices(
  services: Readonly<Record<string, unknown>> | undefined,
): Set<ServiceKind> {
  const out = new Set<ServiceKind>()
  if (!services) return out
  for (const key of ["kafka", "postgres", "fluss", "iceberg"] as const) {
    const value = services[key]
    if (value === undefined) continue
    if (value === false) continue
    out.add(key)
  }
  return out
}
