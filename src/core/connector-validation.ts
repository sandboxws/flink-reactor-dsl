import { findDeepestSource } from "@/codegen/schema-introspect.js"
import type { ValidationDiagnostic } from "./synth-context.js"
import type { ConstructNode } from "./types.js"

// ── Property rule types ──────────────────────────────────────────────

interface RequiredRule {
  readonly kind: "required"
  readonly prop: string
}

interface ConditionalRule {
  readonly kind: "conditional"
  readonly prop: string
  readonly when: (props: Record<string, unknown>) => boolean
  readonly description: string
}

interface InfraProvidedRule {
  readonly kind: "infra-provided"
  readonly prop: string
}

type PropertyRule = RequiredRule | ConditionalRule | InfraProvidedRule

// ── Connector property registry ──────────────────────────────────────

const REGISTRY_BACKED_CDC_FORMATS = new Set([
  "debezium-avro",
  "debezium-protobuf",
])

const schemaRegistryRule = (prop: "format"): ConditionalRule => ({
  kind: "conditional",
  prop: "schemaRegistryUrl",
  when: (props) =>
    typeof props[prop] === "string" &&
    REGISTRY_BACKED_CDC_FORMATS.has(props[prop] as string),
  description:
    "required when `format` is `debezium-avro` or `debezium-protobuf` — the Flink SQL key is `<format>.schema-registry.url`",
})

const CONNECTOR_REGISTRY: ReadonlyMap<string, readonly PropertyRule[]> =
  new Map<string, PropertyRule[]>([
    [
      "KafkaSource",
      [
        { kind: "required", prop: "topic" },
        { kind: "required", prop: "schema" },
        { kind: "infra-provided", prop: "bootstrapServers" },
        schemaRegistryRule("format"),
      ],
    ],
    [
      "KafkaSink",
      [
        { kind: "required", prop: "topic" },
        { kind: "infra-provided", prop: "bootstrapServers" },
        schemaRegistryRule("format"),
      ],
    ],
    [
      "JdbcSource",
      [
        { kind: "required", prop: "url" },
        { kind: "required", prop: "table" },
        { kind: "required", prop: "schema" },
      ],
    ],
    [
      "JdbcSink",
      [
        { kind: "required", prop: "url" },
        { kind: "required", prop: "table" },
        {
          kind: "conditional",
          prop: "keyFields",
          when: (props) => props.upsertMode === true,
          description: "required when `upsertMode` is true",
        },
      ],
    ],
    ["FileSystemSink", [{ kind: "required", prop: "path" }]],
    [
      "PostgresCdcPipelineSource",
      [
        { kind: "required", prop: "hostname" },
        { kind: "required", prop: "database" },
        { kind: "required", prop: "username" },
        { kind: "required", prop: "password" },
        { kind: "required", prop: "schemaList" },
        { kind: "required", prop: "tableList" },
      ],
    ],
    [
      "FlussSource",
      [
        // The CatalogHandle prop is destructured into `catalogName`+
        // `catalogNodeId` by the factory, so validate against the post-
        // construction shape rather than the input prop name.
        { kind: "required", prop: "catalogName" },
        { kind: "required", prop: "database" },
        { kind: "required", prop: "table" },
        { kind: "required", prop: "schema" },
        {
          kind: "conditional",
          prop: "scanStartupTimestampMs",
          when: (props) => props.scanStartupMode === "timestamp",
          description:
            "required when `scanStartupMode === 'timestamp'`; supplies the millisecond offset Fluss seeks to before streaming",
        },
      ],
    ],
    [
      "FlussSink",
      [
        { kind: "required", prop: "catalogName" },
        { kind: "required", prop: "database" },
        { kind: "required", prop: "table" },
      ],
    ],
  ])

// ── Pipeline Connector sink compatibility ───────────────────────────

const FLINK_CDC_DOCS_URL =
  "https://nightlies.apache.org/flink/flink-cdc-docs-release-3.6/"

/**
 * Flink CDC 3.6 Pipeline Connector sink compatibility. Any sink named here
 * accepts a retract stream from a Pipeline-Connector source; anything else
 * is rejected at synth time.
 *
 * Today this is a simple component-name allowlist. As sinks grow (or if
 * upsert-kafka lands in sinks.ts), extend with per-sink predicates.
 */
function isPipelineConnectorCompatibleSink(sink: ConstructNode): {
  readonly ok: boolean
  readonly reason?: string
} {
  switch (sink.component) {
    case "IcebergSink": {
      // MoR requires formatVersion 2 + upsertEnabled (or the MoR props from change 41).
      const formatVersion = sink.props.formatVersion as number | undefined
      const upsertEnabled = sink.props.upsertEnabled === true
      if (formatVersion !== 2 || !upsertEnabled) {
        return {
          ok: false,
          reason:
            "IcebergSink requires `formatVersion: 2` and `upsertEnabled: true` (or the MoR props from change 41) to accept a CDC retract stream",
        }
      }
      return { ok: true }
    }
    case "PaimonSink": {
      const pk = sink.props.primaryKey as readonly string[] | undefined
      if (!Array.isArray(pk) || pk.length === 0) {
        return {
          ok: false,
          reason:
            "PaimonSink requires a non-empty `primaryKey` to accept a CDC retract stream",
        }
      }
      return { ok: true }
    }
    case "FlussSink": {
      const pk = sink.props.primaryKey as readonly string[] | undefined
      if (!Array.isArray(pk) || pk.length === 0) {
        return {
          ok: false,
          reason:
            "Fluss Log table (no primaryKey) cannot sink CDC retracts; set primaryKey to declare a PrimaryKey table",
        }
      }
      return { ok: true }
    }
    default:
      return {
        ok: false,
        reason: `${sink.component} is not a Flink CDC 3.6 Pipeline Connector sink. Supported sinks: IcebergSink (formatVersion 2 + upsertEnabled), PaimonSink (primaryKey), FlussSink (primaryKey). See ${FLINK_CDC_DOCS_URL}`,
      }
  }
}

function walkNodes(
  root: ConstructNode,
  visit: (n: ConstructNode) => void,
): void {
  visit(root)
  for (const c of root.children) walkNodes(c, visit)
}

// ── Validation options ───────────────────────────────────────────────

export interface ConnectorValidationOptions {
  /** When true, infra-provided props emit warnings instead of errors */
  readonly standalone?: boolean
}

// ── Validation function ──────────────────────────────────────────────

/**
 * Validate connector properties across a pipeline tree.
 *
 * Walks all Source/Sink nodes and checks required, conditional, and
 * infra-provided properties against the registry. Must be called
 * AFTER `propagateInfraToChildren()` so inherited props are visible.
 */
export function validateConnectorProperties(
  root: ConstructNode,
  options?: ConnectorValidationOptions,
): ValidationDiagnostic[] {
  const diagnostics: ValidationDiagnostic[] = []

  function walk(node: ConstructNode): void {
    const rules = CONNECTOR_REGISTRY.get(node.component)

    if (rules) {
      const missingProps: string[] = []

      for (const rule of rules) {
        const value = node.props[rule.prop]
        const isMissing = value === undefined || value === null

        if (!isMissing) continue

        switch (rule.kind) {
          case "required":
            missingProps.push(rule.prop)
            diagnostics.push({
              severity: "error",
              message: `${node.component} '${node.id}' is missing required property \`${rule.prop}\``,
              nodeId: node.id,
              component: node.component,
              category: "connector",
              details: { missingProps: [rule.prop] },
            })
            break

          case "conditional":
            if (rule.when(node.props)) {
              missingProps.push(rule.prop)
              diagnostics.push({
                severity: "error",
                message: `${node.component} '${node.id}' is missing property \`${rule.prop}\` (${rule.description})`,
                nodeId: node.id,
                component: node.component,
                category: "connector",
                details: { missingProps: [rule.prop] },
              })
            }
            break

          case "infra-provided":
            diagnostics.push({
              severity: options?.standalone ? "warning" : "error",
              message: options?.standalone
                ? `${node.component} '${node.id}' is missing \`${rule.prop}\` — may be provided by InfraConfig at synthesis time`
                : `${node.component} '${node.id}' is missing required property \`${rule.prop}\``,
              nodeId: node.id,
              component: node.component,
              category: "connector",
              details: { missingProps: [rule.prop] },
            })
            break
        }
      }
    }

    for (const child of node.children) {
      walk(child)
    }
  }

  walk(root)

  // IcebergSink MoR-config validation.
  //
  // `upsertEnabled: true` without either `equalityFieldColumns` or
  // `primaryKey` is a misconfiguration: Iceberg cannot do upserts without a
  // key, and silently falling back to position-only deletes destroys MoR
  // read performance.
  //
  // `writeDistributionMode: 'none'` combined with pipeline parallelism > 1
  // is the classic small-file-explosion footgun — emit a warning that names
  // the sink, but do not fail synthesis (users sometimes want this on
  // purpose for single-writer sinks).
  const pipelineParallelism =
    typeof root.props.parallelism === "number"
      ? (root.props.parallelism as number)
      : undefined
  walkNodes(root, (n) => {
    if (n.component !== "IcebergSink") return
    const upsertEnabled = n.props.upsertEnabled === true
    const equalityFields = n.props.equalityFieldColumns as
      | readonly string[]
      | undefined
    const primaryKey = n.props.primaryKey as readonly string[] | undefined
    const hasEqualityFields =
      Array.isArray(equalityFields) && equalityFields.length > 0
    const hasPrimaryKey = Array.isArray(primaryKey) && primaryKey.length > 0

    if (upsertEnabled && !hasEqualityFields && !hasPrimaryKey) {
      diagnostics.push({
        severity: "error",
        message: `IcebergSink '${n.id}': Iceberg MoR requires either \`equalityFieldColumns\` or \`primaryKey\` when \`upsertEnabled\` is true. See https://iceberg.apache.org/docs/latest/flink-writes/#writing-with-sql for details.`,
        nodeId: n.id,
        component: n.component,
        category: "connector",
      })
    }

    const writeDist = n.props.writeDistributionMode as string | undefined
    if (
      writeDist === "none" &&
      pipelineParallelism !== undefined &&
      pipelineParallelism > 1
    ) {
      diagnostics.push({
        severity: "warning",
        message: `IcebergSink '${n.id}': \`writeDistributionMode: 'none'\` with pipeline \`parallelism: ${pipelineParallelism}\` will produce one small file per (writer × partition) and starve compaction. Consider \`writeDistributionMode: 'hash'\`.`,
        nodeId: n.id,
        component: n.component,
        category: "connector",
      })
    }
  })

  // PaimonSink merge-engine validation.
  //
  // A retract upstream connected to a PaimonSink without `mergeEngine` would
  // compile to a Paimon append-only table — silently wrong for CDC. Mirror
  // change 41's Iceberg MoR guard, but trigger on upstream changelog mode
  // rather than per-sink config.
  walkNodes(root, (n) => {
    if (n.component !== "PaimonSink") return
    if (n.props.mergeEngine !== undefined) return
    if (inferUpstreamChangelogMode(n) !== "retract") return
    diagnostics.push({
      severity: "error",
      message: `PaimonSink '${n.id}' with retract upstream requires \`mergeEngine\` (one of: 'deduplicate', 'partial-update', 'aggregation', 'first-row').`,
      nodeId: n.id,
      component: n.component,
      category: "connector",
    })
  })

  // Cross-node: Pipeline Connector source → compatible sink validation.
  const cdcSources: ConstructNode[] = []
  const sinks: ConstructNode[] = []
  const tappedCdc: ConstructNode[] = []
  walkNodes(root, (n) => {
    if (n.component === "PostgresCdcPipelineSource") {
      cdcSources.push(n)
      if (n.props.tap !== undefined && n.props.tap !== false) {
        tappedCdc.push(n)
      }
    }
    if (n.kind === "Sink") sinks.push(n)
  })

  if (cdcSources.length > 0) {
    // Tap on a Pipeline Connector source is not supported yet — reject
    // explicitly so the user sees why it's ignored.
    for (const src of tappedCdc) {
      diagnostics.push({
        severity: "error",
        message: `${src.component} '${src.id}' has \`tap\` set, but operator tapping is not supported for Flink CDC Pipeline Connector sources yet. Remove the tap prop or switch to a Kafka-hop topology.`,
        nodeId: src.id,
        component: src.component,
        category: "connector",
      })
    }

    for (const src of cdcSources) {
      for (const sink of sinks) {
        const compat = isPipelineConnectorCompatibleSink(sink)
        if (!compat.ok) {
          diagnostics.push({
            severity: "error",
            message: `${src.component} '${src.id}' → ${sink.component} '${sink.id}': ${compat.reason}`,
            nodeId: sink.id,
            component: sink.component,
            category: "connector",
            details: { sourceNodeId: src.id, sinkNodeId: sink.id },
          })
        }
      }
    }
  }

  // Cross-node: FlussSource → retract-requiring sink validation.
  //
  // A FlussSource reading a Log table (no `primaryKey`) emits an append-only
  // changelog. Sinks that materialize an upsert/retract changelog (PaimonSink
  // with `mergeEngine`, IcebergSink with `upsertEnabled`) cannot accept it —
  // surface this at synth time rather than at planner time inside Flink.
  //
  // FlussSource with `tap: true` is rejected unconditionally — PrimaryKey
  // table reads are stateful so tapping requires materialization the runtime
  // does not yet support.
  const flussSources: ConstructNode[] = []
  walkNodes(root, (n) => {
    if (n.component === "FlussSource") flussSources.push(n)
  })

  for (const src of flussSources) {
    if (src.props.tap !== undefined && src.props.tap !== false) {
      diagnostics.push({
        severity: "error",
        message: `FlussSource '${src.id}' has \`tap\` set, but tapping is not supported on FlussSource — PrimaryKey table reads are stateful and tapping requires materialization. Remove the tap prop.`,
        nodeId: src.id,
        component: src.component,
        category: "connector",
      })
    }

    if (
      src.props.scanStartupTimestampMs !== undefined &&
      src.props.scanStartupMode !== "timestamp"
    ) {
      diagnostics.push({
        severity: "error",
        message: `FlussSource '${src.id}': \`scanStartupTimestampMs\` is only valid when \`scanStartupMode === 'timestamp'\` (got \`scanStartupMode = ${JSON.stringify(src.props.scanStartupMode ?? "initial")}\`).`,
        nodeId: src.id,
        component: src.component,
        category: "connector",
      })
    }

    const pk = src.props.primaryKey as readonly string[] | undefined
    const hasPk = Array.isArray(pk) && pk.length > 0
    if (hasPk) continue

    for (const sink of sinks) {
      const reason = retractRequiredSinkReason(sink)
      if (!reason) continue
      diagnostics.push({
        severity: "error",
        message: `FlussSource '${src.id}' → ${sink.component} '${sink.id}': ${reason} — set \`primaryKey\` on FlussSource to read the upstream Fluss PrimaryKey table as a retract changelog.`,
        nodeId: sink.id,
        component: sink.component,
        category: "connector",
        details: { sourceNodeId: src.id, sinkNodeId: sink.id },
      })
    }
  }

  return diagnostics
}

/**
 * Sinks that materialize an upsert/retract changelog and therefore reject
 * append-only upstream streams. Returns the human-readable reason when the
 * sink requires retract, or undefined when it accepts append-only.
 */
function retractRequiredSinkReason(sink: ConstructNode): string | undefined {
  if (sink.component === "PaimonSink" && sink.props.mergeEngine) {
    return `PaimonSink with \`mergeEngine: '${String(sink.props.mergeEngine)}'\` requires a retract upstream`
  }
  if (sink.component === "IcebergSink" && sink.props.upsertEnabled === true) {
    return "IcebergSink with `upsertEnabled: true` requires a retract upstream"
  }
  return undefined
}

/**
 * Static classifier for the upstream changelog mode of a sink. Mirrors the
 * heuristics in `sql-generator.ts:resolveSourceChangelogMode` but works on
 * the construct tree alone (no SynthContext), because connector-validation
 * runs before synthesis.
 */
function inferUpstreamChangelogMode(
  sink: ConstructNode,
): "retract" | "append-only" {
  const upstream = sink.children[0]
  if (!upstream) return "append-only"
  const source = findDeepestSource(upstream)
  if (!source) return "append-only"
  return classifySourceChangelog(source)
}

function classifySourceChangelog(
  source: ConstructNode,
): "retract" | "append-only" {
  const declared = source.props.changelogMode as string | undefined
  if (declared === "retract") return "retract"
  if (source.component === "PostgresCdcPipelineSource") return "retract"
  if (source.component === "KafkaSource") {
    const schema = source.props.schema as
      | { primaryKey?: { columns?: readonly string[] } }
      | undefined
    if ((schema?.primaryKey?.columns?.length ?? 0) > 0) return "retract"
  }
  if (source.component === "FlussSource") {
    const pk = source.props.primaryKey as readonly string[] | undefined
    if (Array.isArray(pk) && pk.length > 0) return "retract"
  }
  return "append-only"
}
