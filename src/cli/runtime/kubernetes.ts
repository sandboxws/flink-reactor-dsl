import { execSync } from "node:child_process"
import { existsSync, readFileSync } from "node:fs"
import { join } from "node:path"
import pc from "picocolors"
import { runSynth } from "@/cli/commands/synth.js"
import type { ProjectContext } from "@/cli/discovery.js"
import { discoverPipelines } from "@/cli/discovery.js"
import type {
  DeployOptions,
  DownOptions,
  HealthReport,
  RuntimeAdapter,
  UpOptions,
} from "./types.js"

function requireContext(ctx: ProjectContext): string {
  const kctx = ctx.resolvedConfig?.kubectl.context
  if (!kctx) {
    throw new Error(
      "kubectl.context is required for runtime=kubernetes but is not set.\n" +
        "  Set environments.<name>.kubectl.context in flink-reactor.config.ts\n" +
        "  or pass --context to the command.",
    )
  }
  return kctx
}

export const KubernetesAdapter: RuntimeAdapter = {
  name: "kubernetes",

  async up(_ctx: ProjectContext, _opts: UpOptions): Promise<void> {
    // Remote K8s targets are assumed to be already running. Nothing to
    // bootstrap from this side — deploy is the meaningful action.
    console.log(
      pc.dim(
        "runtime=kubernetes targets an already-running cluster. Nothing to start.",
      ),
    )
  },

  async down(_ctx: ProjectContext, _opts: DownOptions): Promise<void> {
    console.log(
      pc.dim(
        "runtime=kubernetes does not manage cluster lifecycle. Use `fr deploy` / kubectl delete for resources.",
      ),
    )
  },

  async deploy(ctx: ProjectContext, opts: DeployOptions): Promise<void> {
    const outdir = opts.outdir ?? "dist"
    const projectDir = ctx.projectDir

    console.log(pc.dim("Running synthesis...\n"))
    const artifacts = await runSynth({
      pipeline: opts.pipeline,
      env: ctx.resolvedConfig?.environmentName,
      outdir,
      projectDir,
      consoleUrl: opts.consoleUrl,
    })

    if (artifacts.length === 0) {
      console.log(pc.yellow("No pipelines to deploy."))
      return
    }

    const pipelines = discoverPipelines(projectDir, opts.pipeline)

    if (opts.dryRun) {
      console.log(pc.dim("\n--- Dry run: showing generated CRDs ---\n"))
      for (const p of pipelines) {
        const yamlPath = join(projectDir, outdir, p.name, "deployment.yaml")
        if (!existsSync(yamlPath)) continue
        console.log(pc.bold(`# ${p.name}`))
        console.log(readFileSync(yamlPath, "utf-8"))
        console.log("---")
      }
      return
    }

    const kctx = requireContext(ctx)
    let failed = 0

    for (const p of pipelines) {
      const yamlPath = join(projectDir, outdir, p.name, "deployment.yaml")
      if (!existsSync(yamlPath)) {
        console.log(pc.yellow(`  ${p.name}: no deployment.yaml — skipping`))
        continue
      }

      console.log(pc.dim(`\nApplying ${p.name} to context ${kctx}...`))
      try {
        execSync(`kubectl --context ${kctx} apply -f "${yamlPath}"`, {
          cwd: projectDir,
          stdio: "inherit",
        })
        console.log(pc.green(`  ${p.name}: applied`))
      } catch {
        console.log(pc.red(`  ${p.name}: kubectl apply failed`))
        failed++
      }
    }

    if (failed > 0) {
      throw new Error(`Deployment finished with ${failed} failure(s).`)
    }
  },

  async health(ctx: ProjectContext): Promise<HealthReport> {
    try {
      const kctx = requireContext(ctx)
      execSync(`kubectl --context ${kctx} get ns`, { stdio: "pipe" })
      return { healthy: true, details: `context ${kctx} reachable` }
    } catch (err) {
      return {
        healthy: false,
        details: err instanceof Error ? err.message : String(err),
      }
    }
  },
}
