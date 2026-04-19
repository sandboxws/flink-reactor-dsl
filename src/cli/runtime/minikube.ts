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

const DEFAULT_NAMESPACE = "flink-demo"

function namespaceFor(ctx: ProjectContext): string {
  return ctx.resolvedConfig?.kubernetes.namespace ?? DEFAULT_NAMESPACE
}

function contextArgsFor(ctx: ProjectContext): string[] {
  const ktx = ctx.resolvedConfig?.kubectl.context
  return ktx ? ["--context", ktx] : []
}

export const MinikubeAdapter: RuntimeAdapter = {
  name: "minikube",

  async up(ctx: ProjectContext, _opts: UpOptions): Promise<void> {
    // Delegate to existing sim up implementation via commander dynamic import
    // to avoid a hard cycle and preserve all of its preflight/startup logic.
    const { runSimUp } = await import("@/cli/commands/sim.js")
    await runSimUp({
      cpus: "12",
      memory: "65536",
      disk: "100g",
      k8sVersion: "v1.31.0",
      namespace: namespaceFor(ctx),
      operator: true,
      env: ctx.resolvedConfig?.environmentName,
      init: true,
    })
  },

  async down(ctx: ProjectContext, opts: DownOptions): Promise<void> {
    const { runSimDown } = await import("@/cli/commands/sim.js")
    await runSimDown({
      volumes: opts.volumes ?? false,
      all: opts.all ?? false,
      namespace: namespaceFor(ctx),
    })
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

    const ctxArgs = contextArgsFor(ctx)
    let failed = 0

    for (const p of pipelines) {
      const yamlPath = join(projectDir, outdir, p.name, "deployment.yaml")
      if (!existsSync(yamlPath)) {
        console.log(pc.yellow(`  ${p.name}: no deployment.yaml — skipping`))
        continue
      }

      console.log(pc.dim(`\nApplying ${p.name}...`))
      try {
        execSync(["kubectl", "apply", "-f", yamlPath, ...ctxArgs].join(" "), {
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
      const ctxArgs = contextArgsFor(ctx)
      execSync(
        ["kubectl", ...ctxArgs, "get", "ns", namespaceFor(ctx)].join(" "),
        { stdio: "pipe" },
      )
      return { healthy: true, details: `namespace ${namespaceFor(ctx)} exists` }
    } catch {
      return {
        healthy: false,
        details: `namespace ${namespaceFor(ctx)} not reachable — run \`fr up\``,
      }
    }
  },
}
