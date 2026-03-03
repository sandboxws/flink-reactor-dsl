import type { Command } from "commander"
import pc from "picocolors"
import {
  buildResolvedDashboardJson,
  resolveConfig,
} from "../../core/config-resolver.js"
import { writeResolvedDashboardConfig } from "../config-writer.js"
import { loadConfig } from "../discovery.js"

// ── Command registration ────────────────────────────────────────────

export function registerDashboardCommand(program: Command): void {
  const dashboard = program
    .command("dashboard")
    .description("Dashboard configuration utilities")

  dashboard
    .command("export")
    .description("Export resolved dashboard config as JSON")
    .requiredOption("-e, --env <name>", "Environment name")
    .option("-o, --output <path>", "Output file path")
    .action(async (opts: { env: string; output?: string }) => {
      await runDashboardExport(opts)
    })
}

// ── Export logic ─────────────────────────────────────────────────────

async function runDashboardExport(opts: {
  env: string
  output?: string
  projectDir?: string
}): Promise<void> {
  const projectDir = opts.projectDir ?? process.cwd()

  const config = await loadConfig(projectDir)
  if (!config) {
    console.error(pc.red("No flink-reactor.config.ts found."))
    process.exitCode = 1
    return
  }

  if (!config.environments || Object.keys(config.environments).length === 0) {
    console.error(pc.red("No environments block found in config."))
    console.error(
      pc.dim("Add an environments section to flink-reactor.config.ts."),
    )
    process.exitCode = 1
    return
  }

  const resolved = resolveConfig(config, opts.env)
  const json = buildResolvedDashboardJson(resolved)
  const outputPath = writeResolvedDashboardConfig(projectDir, json, opts.output)

  console.log(pc.green(`\nDashboard config exported: ${outputPath}\n`))
  console.log(pc.dim("Production usage:"))
  console.log(pc.dim("  # Copy into Docker image"))
  console.log(
    pc.dim(
      `  COPY ${outputPath.replace(`${projectDir}/`, "")} /app/dashboard-config.json`,
    ),
  )
  console.log(pc.dim("  ENV FLINK_REACTOR_CONFIG=/app/dashboard-config.json"))
  console.log("")
}
