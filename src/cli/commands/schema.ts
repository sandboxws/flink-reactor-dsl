import { existsSync } from "node:fs"
import { join, resolve } from "node:path"
import type { Command } from "commander"
import { Effect } from "effect"
import pc from "picocolors"
import { loadPipeline } from "@/cli/discovery.js"
import { runCommand } from "@/cli/effect-runner.js"
import {
  type IntrospectedColumn,
  type IntrospectedSchema,
  introspectPipelineSchemas,
} from "@/codegen/schema-introspect.js"
import { CliError } from "@/core/errors.js"
import type { ConstructNode } from "@/core/types.js"

export function registerSchemaCommand(program: Command): void {
  program
    .command("schema")
    .description("Display input and output schemas of a pipeline")
    .argument("<path>", "Path to a .tsx pipeline file, or a pipeline name")
    .option("--json", "Output as JSON")
    .option(
      "--live",
      "For Pipeline Connector sources (e.g. PostgresCdcPipelineSource), " +
        "connect to the live database and introspect column metadata",
    )
    .option(
      "--pg-connection-string <url>",
      "Postgres connection string (overrides FR_PG_CONNECTION_STRING)",
    )
    .action(
      async (
        pathArg: string,
        opts: {
          json?: boolean
          live?: boolean
          pgConnectionString?: string
        },
      ) => {
        await runCommand(
          Effect.tryPromise({
            try: () => runSchema(pathArg, opts),
            catch: (err) =>
              new CliError({
                reason: "invalid_args",
                message: (err as Error).message,
              }),
          }),
        )
      },
    )
}

export async function runSchema(
  pathArg: string,
  opts: {
    json?: boolean
    projectDir?: string
    live?: boolean
    pgConnectionString?: string
  },
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
    pipelineNode = await loadPipeline(filePath, projectDir)
  } catch (err) {
    console.error(pc.red(`Error loading pipeline: ${(err as Error).message}`))
    process.exitCode = 1
    return []
  }

  let schemas = introspectPipelineSchemas(pipelineNode)

  // Live Postgres introspection — opt-in via --live. Merges live-discovered
  // columns into the displayed schema for any PostgresCdcPipelineSource.
  if (opts.live) {
    schemas = await enrichWithLivePostgres(pipelineNode, schemas, {
      connectionString:
        opts.pgConnectionString ?? process.env.FR_PG_CONNECTION_STRING,
    })
  }

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

// ── Live Postgres enrichment ────────────────────────────────────────

async function enrichWithLivePostgres(
  pipelineNode: ConstructNode,
  schemas: IntrospectedSchema[],
  opts: { connectionString?: string },
): Promise<IntrospectedSchema[]> {
  // Collect all PostgresCdcPipelineSource nodes.
  const cdcSources: ConstructNode[] = []
  walkTree(pipelineNode, (n) => {
    if (n.component === "PostgresCdcPipelineSource") cdcSources.push(n)
  })
  if (cdcSources.length === 0) return schemas

  if (!opts.connectionString) {
    console.error(
      pc.red(
        "--live requires a Postgres connection string. Pass --pg-connection-string or set FR_PG_CONNECTION_STRING.",
      ),
    )
    process.exitCode = 1
    return schemas
  }

  const { introspectPostgresTables, splitQualifiedTable } = await import(
    "@/cli/connectors/postgres-introspect.js"
  )

  const allSchemas = new Set<string>()
  const allTables = new Set<string>()
  for (const src of cdcSources) {
    const schemaList = (src.props.schemaList as readonly string[]) ?? []
    const tableList = (src.props.tableList as readonly string[]) ?? []
    for (const s of schemaList) allSchemas.add(s)
    for (const t of tableList) {
      const { table } = splitQualifiedTable(t)
      allTables.add(table)
    }
  }

  try {
    const live = await introspectPostgresTables({
      connectionString: opts.connectionString,
      schemaList: [...allSchemas],
      tableList: [...allTables],
    })

    // For each CDC source, replace/augment its IntrospectedSchema columns.
    return schemas.map((s) => {
      const src = cdcSources.find((n) => n.id === s.nodeId)
      if (!src) return s
      const tableList = (src.props.tableList as readonly string[]) ?? []
      if (tableList.length === 0) return s
      // Use the first table as the canonical schema (CDC sources fan out,
      // but the DSL displays the union; for v1 we show columns of the first
      // table — richer mapping can follow in a later change).
      const first = tableList[0]
      const cols = live.get(first)
      if (!cols || cols.length === 0) return s
      return { ...s, columns: cols }
    })
  } catch (err) {
    console.error(
      pc.red(`Live Postgres introspection failed: ${(err as Error).message}`),
    )
    process.exitCode = 1
    return schemas
  }
}

function walkTree(
  root: ConstructNode,
  visit: (n: ConstructNode) => void,
): void {
  visit(root)
  for (const c of root.children) walkTree(c, visit)
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
