import type { PipelineProps } from "../components/pipeline.js"
import { FlinkVersionCompat } from "../core/flink-compat.js"
import type { ConstructNode, FlinkMajorVersion } from "../core/types.js"

// ── Types ───────────────────────────────────────────────────────────

/** Kubernetes resource quantity (CPU / memory) */
export interface ResourceSpec {
  readonly cpu?: string
  readonly memory?: string
}

export interface PodSpec {
  readonly resource?: ResourceSpec
  readonly replicas?: number
}

/** Options passed to the CRD generator */
export interface CrdGeneratorOptions {
  readonly flinkVersion: FlinkMajorVersion
  /** Flink Docker image (default: derived from flinkVersion) */
  readonly flinkImage?: string
  /** SQL runner JAR URI */
  readonly jarUri?: string
  /** SQL runner entry args */
  readonly jarArgs?: readonly string[]
  /** Override default jobManager spec */
  readonly jobManager?: PodSpec
  /** Override default taskManager spec */
  readonly taskManager?: PodSpec
  /** Extra labels on the CRD metadata */
  readonly labels?: Record<string, string>
  /** Extra annotations on the CRD metadata */
  readonly annotations?: Record<string, string>
}

/** The generated FlinkDeployment CRD as a plain object */
export interface FlinkDeploymentCrd {
  readonly apiVersion: "flink.apache.org/v1beta1"
  readonly kind: "FlinkDeployment"
  readonly metadata: {
    readonly name: string
    readonly labels?: Record<string, string>
    readonly annotations?: Record<string, string>
  }
  readonly spec: {
    readonly image: string
    readonly flinkVersion: string
    readonly flinkConfiguration: Record<string, string>
    readonly jobManager: {
      readonly resource: ResourceSpec
      readonly replicas: number
    }
    readonly taskManager: {
      readonly resource: ResourceSpec
      readonly replicas?: number
    }
    readonly job: {
      readonly jarURI: string
      readonly parallelism: number
      readonly args?: readonly string[]
    }
    readonly [key: string]: unknown
  }
}

// ── Default image mapping ───────────────────────────────────────────

const FLINK_IMAGE_MAP: Record<FlinkMajorVersion, string> = {
  "1.20": "flink:1.20",
  "2.0": "flink:2.0",
  "2.1": "flink:2.1",
  "2.2": "flink:2.2",
}

// flinkVersion → operator-expected version string
const FLINK_VERSION_MAP: Record<FlinkMajorVersion, string> = {
  "1.20": "v1_20",
  "2.0": "v2_0",
  "2.1": "v2_1",
  "2.2": "v2_2",
}

// ── Duration parsing ────────────────────────────────────────────────

const DURATION_REGEX =
  /^(\d+)\s*(ms|s|sec|second|seconds|m|min|minute|minutes|h|hour|hours|d|day|days)$/i

/**
 * Parse a human-readable duration string to milliseconds.
 * Supports: ms, s/sec/second/seconds, m/min/minute/minutes, h/hour/hours, d/day/days
 */
export function toMilliseconds(duration: string): number {
  const match = DURATION_REGEX.exec(duration.trim())
  if (!match) {
    throw new Error(`Invalid duration: '${duration}'`)
  }

  const value = parseInt(match[1], 10)
  const unit = match[2].toLowerCase()

  switch (unit) {
    case "ms":
      return value
    case "s":
    case "sec":
    case "second":
    case "seconds":
      return value * 1000
    case "m":
    case "min":
    case "minute":
    case "minutes":
      return value * 60 * 1000
    case "h":
    case "hour":
    case "hours":
      return value * 60 * 60 * 1000
    case "d":
    case "day":
    case "days":
      return value * 24 * 60 * 60 * 1000
    default:
      throw new Error(`Unknown duration unit: '${unit}'`)
  }
}

// ── CRD generation ──────────────────────────────────────────────────

/**
 * Generate a FlinkDeployment CRD object from a Pipeline construct node.
 */
export function generateCrd(
  pipelineNode: ConstructNode,
  options: CrdGeneratorOptions,
): FlinkDeploymentCrd {
  const props = pipelineNode.props as unknown as PipelineProps
  const { flinkVersion } = options

  // Build flinkConfiguration
  const config: Record<string, string> = {}

  // 1.2: execution mode
  if (props.mode === "batch") {
    config["execution.runtime-mode"] = "BATCH"
  } else {
    config["execution.runtime-mode"] = "STREAMING"
  }

  // 1.4: checkpoint config
  if (props.checkpoint) {
    config["execution.checkpointing.interval"] = String(
      toMilliseconds(props.checkpoint.interval),
    )
    if (props.checkpoint.mode) {
      config["execution.checkpointing.mode"] =
        props.checkpoint.mode === "exactly-once"
          ? "EXACTLY_ONCE"
          : "AT_LEAST_ONCE"
    }
  }

  // 1.5: state backend
  if (props.stateBackend) {
    config["state.backend.type"] = props.stateBackend
  }

  // 1.6: state TTL
  if (props.stateTtl) {
    config["table.exec.state.ttl"] = String(toMilliseconds(props.stateTtl))
  }

  // 1.7: restart strategy
  if (props.restartStrategy) {
    config["restart-strategy.type"] = props.restartStrategy.type
    if (props.restartStrategy.type === "fixed-delay") {
      if (props.restartStrategy.attempts !== undefined) {
        config["restart-strategy.fixed-delay.attempts"] = String(
          props.restartStrategy.attempts,
        )
      }
      if (props.restartStrategy.delay) {
        config["restart-strategy.fixed-delay.delay"] =
          props.restartStrategy.delay
      }
    }
  }

  // 1.8: passthrough flinkConfig
  if (props.flinkConfig) {
    for (const [key, value] of Object.entries(props.flinkConfig)) {
      config[key] = value
    }
  }

  // Normalize config keys for the target Flink version
  const normalizedConfig = FlinkVersionCompat.normalizeConfig(
    config,
    flinkVersion,
  )

  // Build the CRD
  const image = options.flinkImage ?? FLINK_IMAGE_MAP[flinkVersion]
  const flinkVersionStr = FLINK_VERSION_MAP[flinkVersion]

  const metadata: FlinkDeploymentCrd["metadata"] = {
    name: props.name,
    ...(options.labels ? { labels: options.labels } : {}),
    ...(options.annotations ? { annotations: options.annotations } : {}),
  }

  const jobManagerSpec = {
    resource: options.jobManager?.resource ?? { cpu: "1", memory: "1024m" },
    replicas: options.jobManager?.replicas ?? 1,
  }

  const taskManagerSpec = {
    resource: options.taskManager?.resource ?? { cpu: "1", memory: "1024m" },
    ...(options.taskManager?.replicas !== undefined
      ? { replicas: options.taskManager.replicas }
      : {}),
  }

  const jobSpec: FlinkDeploymentCrd["spec"]["job"] = {
    jarURI: options.jarUri ?? "local:///opt/flink/usrlib/sql-runner.jar",
    parallelism: props.parallelism ?? 1,
    ...(options.jarArgs ? { args: options.jarArgs } : {}),
  }

  return {
    apiVersion: "flink.apache.org/v1beta1",
    kind: "FlinkDeployment",
    metadata,
    spec: {
      image,
      flinkVersion: flinkVersionStr,
      flinkConfiguration: normalizedConfig,
      jobManager: jobManagerSpec,
      taskManager: taskManagerSpec,
      job: jobSpec,
    },
  }
}

// ── YAML serialization ──────────────────────────────────────────────

/**
 * Simple YAML serializer for FlinkDeployment CRDs.
 * Handles objects, arrays, strings, numbers, and booleans.
 * Does not handle complex YAML features (anchors, multiline, etc.).
 */
export function toYaml(obj: unknown, indent: number = 0): string {
  const pad = "  ".repeat(indent)

  if (obj === null || obj === undefined) {
    return "null"
  }

  if (typeof obj === "string") {
    // Quote strings that look like numbers, booleans, or contain special chars
    if (
      /^[\d.]+$/.test(obj) ||
      /^(true|false|null|yes|no)$/i.test(obj) ||
      /[:{}[\],&*?|>!%@`#]/.test(obj) ||
      obj === ""
    ) {
      return `'${obj.replace(/'/g, "''")}'`
    }
    return obj
  }

  if (typeof obj === "number" || typeof obj === "boolean") {
    return String(obj)
  }

  if (Array.isArray(obj)) {
    if (obj.length === 0) return "[]"
    const lines: string[] = []
    for (const item of obj) {
      if (typeof item === "object" && item !== null && !Array.isArray(item)) {
        const entries = Object.entries(item)
        if (entries.length > 0) {
          const [firstKey, firstVal] = entries[0]
          lines.push(`${pad}- ${firstKey}: ${toYaml(firstVal, indent + 2)}`)
          for (const [key, val] of entries.slice(1)) {
            lines.push(`${pad}  ${key}: ${toYaml(val, indent + 2)}`)
          }
        }
      } else {
        lines.push(`${pad}- ${toYaml(item, indent + 1)}`)
      }
    }
    return `\n${lines.join("\n")}`
  }

  if (typeof obj === "object") {
    const entries = Object.entries(obj as Record<string, unknown>)
    if (entries.length === 0) return "{}"

    const lines: string[] = []
    for (const [key, value] of entries) {
      if (value === undefined) continue
      if (typeof value === "object" && value !== null) {
        const nested = toYaml(value, indent + 1)
        if (nested.startsWith("\n")) {
          lines.push(`${pad}${key}:${nested}`)
        } else {
          lines.push(`${pad}${key}: ${nested}`)
        }
      } else {
        lines.push(`${pad}${key}: ${toYaml(value, indent + 1)}`)
      }
    }

    if (indent === 0) {
      return `${lines.join("\n")}\n`
    }
    return `\n${lines.join("\n")}`
  }

  return String(obj)
}

/**
 * Generate a FlinkDeployment CRD and serialize to YAML.
 */
export function generateCrdYaml(
  pipelineNode: ConstructNode,
  options: CrdGeneratorOptions,
): string {
  const crd = generateCrd(pipelineNode, options)
  return toYaml(crd)
}
