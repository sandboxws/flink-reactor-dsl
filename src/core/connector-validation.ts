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
    default:
      return {
        ok: false,
        reason: `${sink.component} is not a Flink CDC 3.6 Pipeline Connector sink. Supported sinks: IcebergSink (formatVersion 2 + upsertEnabled), PaimonSink (primaryKey). See ${FLINK_CDC_DOCS_URL}`,
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

  return diagnostics
}
