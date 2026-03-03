import { execSync } from "node:child_process"
import { writeFileSync } from "node:fs"
import { join } from "node:path"
import type { Command } from "commander"
import pc from "picocolors"
import { loadPipeline, resolveProjectContext } from "@/cli/discovery.js"
import { type GraphEdge, SynthContext } from "@/core/synth-context.js"
import type { ConstructNode } from "@/core/types.js"

type GraphFormat = "ascii" | "dot" | "svg"

// ── Command registration ────────────────────────────────────────────

export function registerGraphCommand(program: Command): void {
  program
    .command("graph")
    .description("Visualize pipeline DAG")
    .option("-p, --pipeline <name>", "Graph a specific pipeline")
    .option("-f, --format <format>", "Output format (ascii, dot, svg)", "ascii")
    .option("--open", "Open SVG in browser (with --format svg)")
    .action(
      async (opts: { pipeline?: string; format: string; open?: boolean }) => {
        await runGraph(opts)
      },
    )
}

// ── Graph logic ─────────────────────────────────────────────────────

export async function runGraph(opts: {
  pipeline?: string
  format: string
  open?: boolean
  projectDir?: string
}): Promise<string> {
  const projectDir = opts.projectDir ?? process.cwd()
  const projectCtx = await resolveProjectContext(projectDir, {
    pipeline: opts.pipeline,
  })

  if (projectCtx.pipelines.length === 0) {
    console.log(pc.yellow("No pipelines found in pipelines/ directory."))
    return ""
  }

  const format = (opts.format ?? "ascii") as GraphFormat
  const outputs: string[] = []

  for (const discovered of projectCtx.pipelines) {
    const pipelineNode = await loadPipeline(discovered.entryPoint, projectDir)
    const ctx = new SynthContext()
    ctx.buildFromTree(pipelineNode)

    const nodes = ctx.getAllNodes()
    const edges = ctx.getAllEdges()

    let output: string

    switch (format) {
      case "dot":
        output = generateDot(discovered.name, nodes, edges)
        break
      case "svg":
        output = generateSvg(
          discovered.name,
          nodes,
          edges,
          projectDir,
          opts.open,
        )
        break
      default:
        output = generateAscii(discovered.name, nodes, edges)
        break
    }

    outputs.push(output)
    console.log(output)
  }

  return outputs.join("\n")
}

// ── ASCII DAG rendering ─────────────────────────────────────────────

function nodeLabel(node: ConstructNode): string {
  const name = (node.props.name as string) ?? node.id
  return `[${node.kind}] ${node.component}: ${name}`
}

export function generateAscii(
  pipelineName: string,
  nodes: readonly ConstructNode[],
  edges: readonly GraphEdge[],
): string {
  const lines: string[] = []
  lines.push(`Pipeline: ${pipelineName}`)
  lines.push("=".repeat(40))
  lines.push("")

  if (nodes.length === 0) {
    lines.push("  (empty pipeline)")
    return lines.join("\n")
  }

  // Build adjacency for topological ordering
  const outgoing = new Map<string, string[]>()
  const incoming = new Map<string, Set<string>>()
  const nodeMap = new Map<string, ConstructNode>()

  for (const node of nodes) {
    nodeMap.set(node.id, node)
    outgoing.set(node.id, [])
    incoming.set(node.id, new Set())
  }

  for (const edge of edges) {
    outgoing.get(edge.from)?.push(edge.to)
    incoming.get(edge.to)?.add(edge.from)
  }

  // Find roots (no incoming edges)
  const roots = nodes.filter((n) => (incoming.get(n.id)?.size ?? 0) === 0)

  // BFS to render layers
  const visited = new Set<string>()

  function renderNode(nodeId: string, indent: number): void {
    if (visited.has(nodeId)) return
    visited.add(nodeId)

    const node = nodeMap.get(nodeId)
    if (!node) return

    const prefix = indent === 0 ? "" : `${"  ".repeat(indent - 1)}\u2514\u2500 `
    lines.push(`${prefix}${nodeLabel(node)}`)

    const children = outgoing.get(nodeId) ?? []
    for (const childId of children) {
      if (!visited.has(childId)) {
        renderNode(childId, indent + 1)
      } else {
        const childNode = nodeMap.get(childId)
        const childLabel = childNode ? nodeLabel(childNode) : childId
        lines.push(
          `${"  ".repeat(indent)}\u2514\u2500 ${pc.dim(`(ref) ${childLabel}`)}`,
        )
      }
    }
  }

  for (const root of roots) {
    renderNode(root.id, 0)
    lines.push("")
  }

  // Render any unvisited nodes (disconnected)
  for (const node of nodes) {
    if (!visited.has(node.id)) {
      renderNode(node.id, 0)
      lines.push("")
    }
  }

  return lines.join("\n")
}

// ── DOT format ──────────────────────────────────────────────────────

export function generateDot(
  pipelineName: string,
  nodes: readonly ConstructNode[],
  edges: readonly GraphEdge[],
): string {
  const lines: string[] = []
  lines.push(`digraph "${pipelineName}" {`)
  lines.push("  rankdir=TB;")
  lines.push('  node [shape=box, style=rounded, fontname="monospace"];')
  lines.push("")

  // Kind → color mapping
  const kindColors: Record<string, string> = {
    Source: "#4CAF50",
    Transform: "#2196F3",
    Join: "#FF9800",
    Window: "#9C27B0",
    Sink: "#F44336",
    Catalog: "#795548",
    RawSQL: "#607D8B",
    UDF: "#009688",
    CEP: "#E91E63",
    Pipeline: "#9E9E9E",
  }

  for (const node of nodes) {
    const label = `${node.kind}\\n${node.component}`
    const color = kindColors[node.kind] ?? "#9E9E9E"
    lines.push(
      `  "${node.id}" [label="${label}", fillcolor="${color}", style="rounded,filled", fontcolor="white"];`,
    )
  }

  lines.push("")

  for (const edge of edges) {
    lines.push(`  "${edge.from}" -> "${edge.to}";`)
  }

  lines.push("}")
  return lines.join("\n")
}

// ── SVG generation ──────────────────────────────────────────────────

function generateSvg(
  pipelineName: string,
  nodes: readonly ConstructNode[],
  edges: readonly GraphEdge[],
  projectDir: string,
  openInBrowser?: boolean,
): string {
  const dot = generateDot(pipelineName, nodes, edges)
  const dotPath = join(projectDir, "dist", `${pipelineName}.dot`)
  const svgPath = join(projectDir, "dist", `${pipelineName}.svg`)

  try {
    const { mkdirSync } = require("node:fs")
    mkdirSync(join(projectDir, "dist"), { recursive: true })
    writeFileSync(dotPath, dot, "utf-8")

    execSync(`dot -Tsvg "${dotPath}" -o "${svgPath}"`, { stdio: "pipe" })

    if (openInBrowser) {
      const platform = process.platform
      const openCmd =
        platform === "darwin"
          ? "open"
          : platform === "win32"
            ? "start"
            : "xdg-open"
      execSync(`${openCmd} "${svgPath}"`, { stdio: "ignore" })
    }

    console.log(pc.dim(`SVG written to ${svgPath}`))
    return dot
  } catch {
    console.error(
      pc.red(
        "SVG generation requires graphviz. Install with: brew install graphviz",
      ),
    )
    console.log(pc.dim("Falling back to DOT output:"))
    return dot
  }
}
