import { existsSync } from "node:fs"
import { join, resolve } from "node:path"
import type { Command } from "commander"
import pc from "picocolors"
import {
  type IntrospectedColumn,
  type IntrospectedSchema,
  introspectPipelineSchemas,
} from "../../codegen/schema-introspect.js"
import type { ConstructNode } from "../../core/types.js"
import { loadPipeline } from "../discovery.js"

export function registerSchemaCommand(program: Command): void {
  program
    .command("schema")
    .description("Display input and output schemas of a pipeline")
    .argument("<path>", "Path to a .tsx pipeline file, or a pipeline name")
    .option("--json", "Output as JSON")
    .action(async (pathArg: string, opts: { json?: boolean }) => {
      await runSchema(pathArg, opts)
    })
}

export async function runSchema(
  pathArg: string,
  opts: { json?: boolean; projectDir?: string },
): Promise<IntrospectedSchema[]> {
  const projectDir = opts.projectDir ?? process.cwd()
  const filePath = resolveFilePath(pathArg, projectDir)

  if (!filePath) {
    console.error(pc.red(`Error: Cannot find pipeline at '${pathArg}'`))
    process.exitCode = 1
    return []
  }

  let pipelineNode: ConstructNode
  try {
    pipelineNode = await loadPipeline(filePath)
  } catch (err) {
    console.error(pc.red(`Error loading pipeline: ${(err as Error).message}`))
    process.exitCode = 1
    return []
  }

  const schemas = introspectPipelineSchemas(pipelineNode)

  if (schemas.length === 0) {
    console.error(pc.yellow("No schemas found in this pipeline."))
    process.exitCode = 1
    return []
  }

  if (opts.json) {
    console.log(JSON.stringify(schemas, null, 2))
  } else {
    printSchemas(schemas, pipelineNode.props.name as string | undefined)
  }

  return schemas
}

// ── Path resolution ─────────────────────────────────────────────────

function resolveFilePath(pathArg: string, projectDir: string): string | null {
  // If it contains a path separator or ends in .tsx, treat as file path
  if (
    pathArg.includes("/") ||
    pathArg.includes("\\") ||
    pathArg.endsWith(".tsx")
  ) {
    const abs = resolve(projectDir, pathArg)
    return existsSync(abs) ? abs : null
  }

  // Otherwise, treat as pipeline name → pipelines/<name>/index.tsx
  const conventionPath = join(projectDir, "pipelines", pathArg, "index.tsx")
  if (existsSync(conventionPath)) {
    return conventionPath
  }

  // Fallback: try as direct path
  const abs = resolve(projectDir, pathArg)
  return existsSync(abs) ? abs : null
}

// ── TUI table rendering ─────────────────────────────────────────────

function printSchemas(
  schemas: IntrospectedSchema[],
  pipelineName?: string,
): void {
  const sources = schemas.filter((s) => s.kind === "source")
  const sinks = schemas.filter((s) => s.kind === "sink")

  // Pipeline header
  const name = pipelineName ?? "unknown"
  console.log("")
  console.log(`  ${pc.bold(pc.cyan("Pipeline:"))} ${pc.bold(name)}`)
  console.log("")

  // Sources
  if (sources.length > 0) {
    console.log(
      `  ${pc.bold(pc.green("■"))} ${pc.bold("Sources (Input Schemas)")}`,
    )
    console.log("")
    for (const src of sources) {
      printSchemaTable(src)
    }
  }

  // Sinks
  if (sinks.length > 0) {
    console.log(
      `  ${pc.bold(pc.magenta("■"))} ${pc.bold("Sinks (Output Schemas)")}`,
    )
    console.log("")
    for (const sink of sinks) {
      if (sink.columns.length === 0) {
        console.log(
          `  ${pc.dim("─")} ${pc.bold(sink.nameHint)} ${pc.dim(`(${sink.component})`)}`,
        )
        console.log(`    ${pc.dim("Schema could not be resolved")}`)
        console.log("")
        continue
      }
      printSchemaTable(sink)
    }
  }
}

function printSchemaTable(schema: IntrospectedSchema): void {
  const { nameHint, component, columns } = schema

  // Compute column widths
  const fieldWidth = Math.max(5, ...columns.map((c) => c.name.length))
  const typeWidth = Math.max(4, ...columns.map((c) => c.type.length))
  const constraintWidth = Math.max(
    11, // "Constraints"
    ...columns.map((c) => c.constraints.join(", ").length),
  )

  const fw = fieldWidth + 2
  const tw = typeWidth + 2
  const cw = constraintWidth + 2

  const hasConstraints = columns.some((c) => c.constraints.length > 0)

  // Header
  console.log(
    `  ${pc.dim("─")} ${pc.bold(nameHint)} ${pc.dim(`(${component})`)}`,
  )

  if (hasConstraints) {
    printTableWithConstraints(columns, fw, tw, cw)
  } else {
    printTableWithoutConstraints(columns, fw, tw)
  }

  console.log("")
}

function printTableWithConstraints(
  columns: readonly IntrospectedColumn[],
  fw: number,
  tw: number,
  cw: number,
): void {
  // Top border
  console.log(
    `  ${pc.dim(`┌${"─".repeat(fw)}┬${"─".repeat(tw)}┬${"─".repeat(cw)}┐`)}`,
  )

  // Column headers
  console.log(
    `  ${pc.dim("│")} ${pc.bold("Field".padEnd(fw - 2))} ${pc.dim("│")} ${pc.bold("Type".padEnd(tw - 2))} ${pc.dim("│")} ${pc.bold("Constraints".padEnd(cw - 2))} ${pc.dim("│")}`,
  )

  // Header separator
  console.log(
    `  ${pc.dim(`├${"─".repeat(fw)}┼${"─".repeat(tw)}┼${"─".repeat(cw)}┤`)}`,
  )

  // Data rows
  for (const col of columns) {
    const constraints = col.constraints.join(", ")
    const styledConstraints = constraints
      ? pc.yellow(constraints.padEnd(cw - 2))
      : " ".repeat(cw - 2)
    console.log(
      `  ${pc.dim("│")} ${col.name.padEnd(fw - 2)} ${pc.dim("│")} ${pc.cyan(col.type.padEnd(tw - 2))} ${pc.dim("│")} ${styledConstraints} ${pc.dim("│")}`,
    )
  }

  // Bottom border
  console.log(
    `  ${pc.dim(`└${"─".repeat(fw)}┴${"─".repeat(tw)}┴${"─".repeat(cw)}┘`)}`,
  )
}

function printTableWithoutConstraints(
  columns: readonly IntrospectedColumn[],
  fw: number,
  tw: number,
): void {
  // Top border
  console.log(`  ${pc.dim(`┌${"─".repeat(fw)}┬${"─".repeat(tw)}┐`)}`)

  // Column headers
  console.log(
    `  ${pc.dim("│")} ${pc.bold("Field".padEnd(fw - 2))} ${pc.dim("│")} ${pc.bold("Type".padEnd(tw - 2))} ${pc.dim("│")}`,
  )

  // Header separator
  console.log(`  ${pc.dim(`├${"─".repeat(fw)}┼${"─".repeat(tw)}┤`)}`)

  // Data rows
  for (const col of columns) {
    console.log(
      `  ${pc.dim("│")} ${col.name.padEnd(fw - 2)} ${pc.dim("│")} ${pc.cyan(col.type.padEnd(tw - 2))} ${pc.dim("│")}`,
    )
  }

  // Bottom border
  console.log(`  ${pc.dim(`└${"─".repeat(fw)}┴${"─".repeat(tw)}┘`)}`)
}
