import type { ProjectContext } from "@/cli/discovery.js"
import type { Runtime } from "@/core/config.js"

export interface UpOptions {
  readonly port?: string
  readonly seed?: boolean
}

export interface DownOptions {
  readonly volumes?: boolean
  /** Stop the underlying VM (minikube) / full teardown. */
  readonly all?: boolean
}

export interface DeployOptions {
  readonly pipeline?: string
  readonly dryRun?: boolean
  readonly consoleUrl?: string
  readonly outdir?: string
}

export interface HealthReport {
  readonly healthy: boolean
  readonly details: string
}

/**
 * One lane of the `fr` CLI. Implementations wrap the per-runtime logic
 * (docker-compose, kubectl apply, `flink run`, …) behind a uniform
 * `up`/`down`/`deploy` surface so the top-level commands can dispatch by
 * `resolvedConfig.runtime` without caring which runtime is in use.
 */
export interface RuntimeAdapter {
  readonly name: Runtime
  up(ctx: ProjectContext, opts: UpOptions): Promise<void>
  down(ctx: ProjectContext, opts: DownOptions): Promise<void>
  deploy(ctx: ProjectContext, opts: DeployOptions): Promise<void>
  health(ctx: ProjectContext): Promise<HealthReport>
}
