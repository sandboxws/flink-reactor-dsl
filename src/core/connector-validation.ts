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
  ])

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
  return diagnostics
}
