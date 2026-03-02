import { execSync } from "node:child_process"
import { existsSync, readFileSync } from "node:fs"
import { join } from "node:path"
import type { Command } from "commander"
import pc from "picocolors"
import { discoverPipelines } from "../discovery.js"
import { runSynth } from "./synth.js"

// ── Command registration ────────────────────────────────────────────

export function registerDeployCommand(program: Command): void {
  program
    .command("deploy")
    .description("Deploy pipelines to a Kubernetes cluster")
    .option("-p, --pipeline <name>", "Deploy a specific pipeline")
    .option("-e, --env <name>", "Environment name")
    .option("--dry-run", "Synth only, print YAML without applying")
    .option("--context <context>", "kubectl context")
    .action(
      async (opts: {
        pipeline?: string
        env?: string
        dryRun?: boolean
        context?: string
      }) => {
        await runDeploy(opts)
      },
    )
}

// ── Deploy logic ────────────────────────────────────────────────────

export async function runDeploy(opts: {
  pipeline?: string
  env?: string
  dryRun?: boolean
  context?: string
  projectDir?: string
}): Promise<void> {
  const projectDir = opts.projectDir ?? process.cwd()

  // Step 1: Synthesize
  console.log(pc.dim("Running synthesis...\n"))
  const artifacts = await runSynth({
    pipeline: opts.pipeline,
    env: opts.env,
    outdir: "dist",
    projectDir,
  })

  if (artifacts.length === 0) {
    console.log(pc.yellow("No pipelines to deploy."))
    return
  }

  const pipelines = discoverPipelines(projectDir, opts.pipeline)

  if (opts.dryRun) {
    // Dry-run: just print the YAML
    console.log(pc.dim("\n--- Dry run: showing generated CRDs ---\n"))

    for (const p of pipelines) {
      const yamlPath = join(projectDir, "dist", p.name, "deployment.yaml")
      if (!existsSync(yamlPath)) continue

      const yaml = readFileSync(yamlPath, "utf-8")
      console.log(pc.bold(`# ${p.name}`))
      console.log(yaml)
      console.log("---")
    }
    return
  }

  // Step 2: Apply CRDs via kubectl
  const contextFlag = opts.context ? `--context ${opts.context}` : ""

  for (const p of pipelines) {
    const yamlPath = join(projectDir, "dist", p.name, "deployment.yaml")
    if (!existsSync(yamlPath)) {
      console.log(pc.yellow(`No deployment.yaml for ${p.name}. Skipping.`))
      continue
    }

    console.log(pc.dim(`\nApplying ${p.name}...`))

    try {
      const output = execSync(
        `kubectl apply -f "${yamlPath}" ${contextFlag}`.trim(),
        { cwd: projectDir, encoding: "utf-8", stdio: "pipe" },
      )
      console.log(pc.green(`  ${p.name}: ${output.trim()}`))
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err)
      console.error(pc.red(`  ${p.name}: Failed to apply — ${message}`))
      process.exitCode = 1
    }
  }

  // Step 3: Print status
  console.log("")
  if (!process.exitCode) {
    console.log(pc.green("Deployment complete."))

    for (const p of pipelines) {
      console.log(pc.dim(`  ${p.name}: deployed`))
    }

    console.log("")
    console.log(pc.dim("Check status with:"))
    console.log(pc.dim(`  kubectl get flinkdeployments ${contextFlag}`.trim()))
  } else {
    console.log(pc.red("Deployment finished with errors."))
  }
}
