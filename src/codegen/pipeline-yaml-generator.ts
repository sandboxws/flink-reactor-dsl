// ── Flink CDC 3.6 Pipeline YAML generator ───────────────────────────
// For pipelines whose source is a Pipeline Connector (e.g.
// PostgresCdcPipelineSource), the runtime is flink-cdc-cli.jar reading
// a pipeline.yaml document — NOT Flink SQL. This module emits that YAML.
//
// Shape (3 top-level stanzas):
//   source:   postgres connector props
//   sink:     iceberg / paimon connector props forwarded from the sink node
//   pipeline: job metadata (name, parallelism, schema.change.behavior)

import type { SecretRef } from "@/core/secret-ref.js"
import { isSecretRef, renderSecretPlaceholder } from "@/core/secret-ref.js"
import type { ConstructNode } from "@/core/types.js"
import { toYaml } from "./crd-generator.js"
import { hasPipelineConnectorSource } from "./sql-generator.js"

// ── Types ───────────────────────────────────────────────────────────

export interface GeneratePipelineYamlOptions {
  /** Overrides the default `schema.change.behavior: evolve` value. */
  readonly schemaChangeBehavior?: "evolve" | "lenient" | "try_evolve" | "ignore"
}

// ── Helpers ─────────────────────────────────────────────────────────

function findFirstByComponent(
  node: ConstructNode,
  component: string,
): ConstructNode | undefined {
  if (node.component === component) return node
  for (const c of node.children) {
    const found = findFirstByComponent(c, component)
    if (found) return found
  }
  return undefined
}

function findSinkNode(node: ConstructNode): ConstructNode | undefined {
  if (node.kind === "Sink") return node
  for (const c of node.children) {
    const s = findSinkNode(c)
    if (s) return s
  }
  return undefined
}

function findCatalogNode(
  root: ConstructNode,
  catalogName: string,
): ConstructNode | undefined {
  if (
    root.kind === "Catalog" &&
    (root.props.name as string | undefined) === catalogName
  ) {
    return root
  }
  for (const c of root.children) {
    const found = findCatalogNode(c, catalogName)
    if (found) return found
  }
  return undefined
}

function renderValue(value: unknown): unknown {
  if (isSecretRef(value)) return renderSecretPlaceholder(value as SecretRef)
  return value
}

// ── Source stanza builders ──────────────────────────────────────────

function deriveSlotName(pipelineName: string): string {
  return `fr_${pipelineName.replace(/[^a-z0-9_]/gi, "_").toLowerCase()}_slot`
}

function derivePublicationName(pipelineName: string): string {
  return `fr_${pipelineName.replace(/[^a-z0-9_]/gi, "_").toLowerCase()}_pub`
}

function buildPostgresSourceStanza(
  node: ConstructNode,
  pipelineName: string,
): Record<string, unknown> {
  const p = node.props
  const tableList = p.tableList as readonly string[]
  const schemaList = p.schemaList as readonly string[]

  const stanza: Record<string, unknown> = {
    type: "postgres",
    hostname: p.hostname,
    port: p.port ?? 5432,
    username: p.username,
    password: renderValue(p.password),
    "database-name": p.database,
    "schema-name": schemaList.join(","),
    "table-name": tableList.join(","),
    "decoding.plugin.name": p.decodingPluginName ?? "pgoutput",
    "slot.name": p.replicationSlotName ?? deriveSlotName(pipelineName),
    "publication.name":
      p.publicationName ?? derivePublicationName(pipelineName),
    "scan.startup.mode": p.startupMode ?? "initial",
  }

  // snapshotMode → Flink CDC scan.* keys
  const snapshotMode = (p.snapshotMode as string | undefined) ?? "initial"
  if (snapshotMode === "never") {
    stanza["scan.snapshot.enabled"] = false
  } else if (snapshotMode === "initial_only") {
    stanza["scan.snapshot.enabled"] = true
    stanza["scan.startup.mode"] = "snapshot"
  } else {
    stanza["scan.snapshot.enabled"] = true
  }

  if (p.chunkSize !== undefined) {
    stanza["scan.incremental.snapshot.chunk.size"] = p.chunkSize
  }
  if (p.parallelism !== undefined) {
    stanza["scan.incremental.snapshot.chunk.key-column.parallelism"] =
      p.parallelism
  }
  if (p.heartbeatIntervalMs !== undefined) {
    stanza["heartbeat.interval.ms"] = p.heartbeatIntervalMs
  }
  if (p.slotDropOnStop === true) {
    stanza["slot.drop.on.stop"] = true
  }

  return stanza
}

// ── Sink stanza builders ────────────────────────────────────────────

function buildIcebergSinkStanza(
  sinkNode: ConstructNode,
  root: ConstructNode,
): Record<string, unknown> {
  const p = sinkNode.props
  const catalogName = p.catalogName as string
  const catalogNode = findCatalogNode(root, catalogName)
  const stanza: Record<string, unknown> = {
    type: "iceberg",
    "catalog.name": catalogName,
    database: p.database,
    table: p.table,
  }
  if (catalogNode) {
    const cp = catalogNode.props
    stanza["catalog.properties.type"] =
      (cp.catalogType as string | undefined) ?? "rest"
    if (cp.uri) stanza["catalog.properties.uri"] = cp.uri
    if (cp.warehouse) stanza["catalog.properties.warehouse"] = cp.warehouse
  }
  if (p.formatVersion !== undefined) {
    stanza["table.properties.format-version"] = String(p.formatVersion)
  }
  if (p.upsertEnabled === true) {
    stanza["table.properties.upsert.enabled"] = "true"
  }
  if (Array.isArray(p.primaryKey) && p.primaryKey.length > 0) {
    stanza["table.properties.primary-key"] = (
      p.primaryKey as readonly string[]
    ).join(",")
  }
  return stanza
}

function buildPaimonSinkStanza(
  sinkNode: ConstructNode,
  root: ConstructNode,
): Record<string, unknown> {
  const p = sinkNode.props
  const catalogName = p.catalogName as string
  const catalogNode = findCatalogNode(root, catalogName)
  const stanza: Record<string, unknown> = {
    type: "paimon",
    "catalog.name": catalogName,
    database: p.database,
    table: p.table,
  }
  if (catalogNode?.props.warehouse) {
    stanza["catalog.properties.warehouse"] = catalogNode.props.warehouse
  }
  if (catalogNode?.props.metastore) {
    stanza["catalog.properties.metastore"] = catalogNode.props.metastore
  }
  if (Array.isArray(p.primaryKey) && p.primaryKey.length > 0) {
    stanza["table.properties.primary-key"] = (
      p.primaryKey as readonly string[]
    ).join(",")
  }
  if (p.mergeEngine) stanza["table.properties.merge-engine"] = p.mergeEngine
  if (p.changelogProducer) {
    stanza["table.properties.changelog-producer"] = p.changelogProducer
  }
  return stanza
}

function buildSinkStanza(
  sinkNode: ConstructNode,
  root: ConstructNode,
): Record<string, unknown> {
  switch (sinkNode.component) {
    case "IcebergSink":
      return buildIcebergSinkStanza(sinkNode, root)
    case "PaimonSink":
      return buildPaimonSinkStanza(sinkNode, root)
    default:
      throw new Error(
        `Pipeline Connector cannot emit a sink stanza for component '${sinkNode.component}'. ` +
          `Supported sinks: IcebergSink, PaimonSink. Connect to a supported sink or fall back to the Kafka-hop topology.`,
      )
  }
}

// ── Public API ──────────────────────────────────────────────────────

/**
 * Generate a Flink CDC 3.6 pipeline.yaml document for a Pipeline-Connector
 * pipeline. Returns `null` when the tree has no Pipeline Connector source.
 *
 * The output is deterministic: stanza key order is stable, and any
 * identical input produces byte-identical output.
 */
export function generatePipelineYaml(
  pipelineNode: ConstructNode,
  options: GeneratePipelineYamlOptions = {},
): string | null {
  if (!hasPipelineConnectorSource(pipelineNode)) return null

  const pipelineName =
    (pipelineNode.props.name as string | undefined) ?? "pipeline"

  // v1 only supports Postgres — when we add more Pipeline Connector sources
  // we can dispatch on component here.
  const source = findFirstByComponent(pipelineNode, "PostgresCdcPipelineSource")
  if (!source) return null

  const sink = findSinkNode(pipelineNode)
  if (!sink) {
    throw new Error(
      `Pipeline '${pipelineName}' has a PostgresCdcPipelineSource but no downstream sink`,
    )
  }

  const doc = {
    source: buildPostgresSourceStanza(source, pipelineName),
    sink: buildSinkStanza(sink, pipelineNode),
    pipeline: {
      name: pipelineName,
      ...(pipelineNode.props.parallelism !== undefined
        ? { parallelism: pipelineNode.props.parallelism }
        : {}),
      "schema.change.behavior": options.schemaChangeBehavior ?? "evolve",
    },
  }

  return toYaml(doc)
}
