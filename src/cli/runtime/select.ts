import type { ProjectContext } from "@/cli/discovery.js"
import type { Runtime } from "@/core/config.js"
import { SUPPORTED_RUNTIMES } from "@/core/config.js"
import { DockerAdapter } from "./docker.js"
import { HomebrewAdapter } from "./homebrew.js"
import { KubernetesAdapter } from "./kubernetes.js"
import { MinikubeAdapter } from "./minikube.js"
import type { RuntimeAdapter } from "./types.js"

const ADAPTERS: Record<Runtime, RuntimeAdapter> = {
  docker: DockerAdapter,
  minikube: MinikubeAdapter,
  homebrew: HomebrewAdapter,
  kubernetes: KubernetesAdapter,
}

/**
 * Pick the runtime adapter for a project context.
 *
 * Precedence:
 *   1. Explicit `override` (from `--runtime=<name>` flag), validated against
 *      the env's `supportedRuntimes` list.
 *   2. The resolved env's `runtime`.
 *   3. `docker` (matches the platform docs' "easiest" default) when there is
 *      no resolved config — covers projects without flink-reactor.config.ts.
 */
export function selectAdapter(
  ctx: ProjectContext,
  override?: Runtime,
): RuntimeAdapter {
  if (override && !SUPPORTED_RUNTIMES.includes(override)) {
    throw new Error(
      `Unknown runtime '${override}'. Supported: ${SUPPORTED_RUNTIMES.join(", ")}`,
    )
  }

  const resolved = ctx.resolvedConfig
  if (override) {
    if (resolved && !resolved.supportedRuntimes.includes(override)) {
      const envName = resolved.environmentName ?? "<default>"
      throw new Error(
        `Runtime '${override}' is not supported by environment '${envName}'. ` +
          `Supported: [${resolved.supportedRuntimes.join(", ")}]. ` +
          `Add '${override}' to supportedRuntimes in flink-reactor.config.ts to enable it.`,
      )
    }
    return ADAPTERS[override]
  }

  if (resolved) return ADAPTERS[resolved.runtime]
  return DockerAdapter
}

export { ADAPTERS }
