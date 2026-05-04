import { Either } from "effect"
import type { PipelineProps } from "@/components/pipeline.js"
import { CrdGenerationError } from "@/core/errors.js"
import { FlinkVersionCompat } from "@/core/flink-compat.js"
import { isSecretRef, type SecretRef } from "@/core/secret-ref.js"
import type { ConstructNode, FlinkMajorVersion } from "@/core/types.js"
import { pipelineYamlConfigMapName } from "./secondary-resources.js"
import { hasPipelineConnectorSource } from "./sql-generator.js"

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
  /**
   * Synthesized SQL for this pipeline. When set, written into
   * `flinkConfiguration["pipeline.sql"]` so the resulting Flink job preserves
   * the source SQL on its user-config map (visible via /jobs/:id/config and
   * the dashboard's SQL tab).
   */
  readonly sourceSql?: string
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

/** The generated FlinkBlueGreenDeployment CRD as a plain object */
export interface FlinkBlueGreenDeploymentCrd {
  readonly apiVersion: "flink.apache.org/v1beta1"
  readonly kind: "FlinkBlueGreenDeployment"
  readonly metadata: {
    readonly name: string
    readonly labels?: Record<string, string>
    readonly annotations?: Record<string, string>
  }
  readonly spec: {
    readonly configuration?: Record<string, string>
    readonly ingress?: {
      readonly template?: string
      readonly className?: string
      readonly annotations?: Record<string, string>
    }
    readonly template: {
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
          readonly upgradeMode: string
          readonly args?: readonly string[]
        }
        readonly [key: string]: unknown
      }
    }
  }
}

/** Union of all Flink CRD types. Discriminate on `kind`. */
export type AnyFlinkCrd = FlinkDeploymentCrd | FlinkBlueGreenDeploymentCrd

// ── CRD utilities ───────────────────────────────────────────────────

/** Extract flinkConfiguration from any CRD type */
export function getFlinkConfiguration(
  crd: AnyFlinkCrd,
): Record<string, string> {
  if (crd.kind === "FlinkDeployment") {
    return crd.spec.flinkConfiguration
  }
  return crd.spec.template.spec.flinkConfiguration
}

/** Return a new CRD with updated flinkConfiguration */
export function withFlinkConfiguration(
  crd: AnyFlinkCrd,
  config: Record<string, string>,
): AnyFlinkCrd {
  if (crd.kind === "FlinkDeployment") {
    return { ...crd, spec: { ...crd.spec, flinkConfiguration: config } }
  }
  return {
    ...crd,
    spec: {
      ...crd.spec,
      template: {
        ...crd.spec.template,
        spec: { ...crd.spec.template.spec, flinkConfiguration: config },
      },
    },
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

// ── pipeline.global-job-parameters merge ────────────────────────────

/**
 * Merge a single key/value into the encoded form of Flink's
 * `pipeline.global-job-parameters` map.
 *
 * Flink parses this config option as `key1:value1,key2:value2`. We use it as
 * the transport for round-tripping FR metadata (e.g. the synthesized SQL)
 * onto a job's user-config. The `value` must contain neither `:` nor `,`;
 * callers are expected to base64-encode anything that might.
 *
 * If the user already supplied `pipeline.global-job-parameters` via
 * `flinkConfig`, we preserve their entries and only add ours when the key
 * is absent (user-set values win, so they can opt out by setting their own
 * `pipeline.sql.b64` to an empty string or any other value).
 */
function mergeGlobalJobParameters(
  existing: string | undefined,
  key: string,
  value: string,
): string {
  if (!existing || existing.trim().length === 0) {
    return `${key}:${value}`
  }
  // Parse existing entries — `key1:value1,key2:value2`.
  const entries = existing
    .split(",")
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0)
  const hasKey = entries.some((entry) => {
    const colonIdx = entry.indexOf(":")
    if (colonIdx === -1) return entry === key
    return entry.slice(0, colonIdx) === key
  })
  if (hasKey) return existing
  return `${existing},${key}:${value}`
}

// ── CRD generation ──────────────────────────────────────────────────

// ── Shared CRD building helpers ─────────────────────────────────────

interface InnerSpec {
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
    readonly upgradeMode?: string
    readonly args?: readonly string[]
  }
}

/**
 * Build the inner Flink deployment spec (shared between FlinkDeployment
 * and FlinkBlueGreenDeployment CRDs).
 */
function buildInnerSpec(
  props: PipelineProps,
  options: CrdGeneratorOptions,
  upgradeMode?: string,
): InnerSpec {
  const { flinkVersion } = options

  // Build flinkConfiguration
  const config: Record<string, string> = {}

  if (props.mode === "batch") {
    config["execution.runtime-mode"] = "BATCH"
  } else {
    config["execution.runtime-mode"] = "STREAMING"
  }

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

  if (props.stateBackend) {
    config["state.backend.type"] = props.stateBackend
  }

  if (props.stateTtl) {
    config["table.exec.state.ttl"] = String(toMilliseconds(props.stateTtl))
  }

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

  if (props.flinkConfig) {
    // Sort by key so output bytes don't depend on the user's
    // `{ ...defaults, ...overrides }` insertion order. Flink reads
    // flinkConfiguration as a map — order has no runtime meaning.
    const sortedKeys = Object.keys(props.flinkConfig).sort()
    for (const key of sortedKeys) {
      config[key] = props.flinkConfig[key]
    }
  }

  // Preserve the synthesized SQL on user-config so /jobs/:id/config (and the
  // dashboard's SQL tab) can surface it for the resulting job. We carry the
  // SQL inside `pipeline.global-job-parameters` (Flink's recognized passthrough
  // map → ExecutionConfig.globalJobParameters → user-config), base64-encoded
  // under the key `pipeline.sql.b64` so colons/commas/newlines in the SQL
  // don't collide with Flink's map-type parser.
  if (options.sourceSql && options.sourceSql.trim().length > 0) {
    config["pipeline.global-job-parameters"] = mergeGlobalJobParameters(
      config["pipeline.global-job-parameters"],
      "pipeline.sql.b64",
      Buffer.from(options.sourceSql, "utf-8").toString("base64"),
    )
  }

  const normalizedConfig = FlinkVersionCompat.normalizeConfig(
    config,
    flinkVersion,
  )

  const image = options.flinkImage ?? FLINK_IMAGE_MAP[flinkVersion]
  const flinkVersionStr = FLINK_VERSION_MAP[flinkVersion]

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

  const jobSpec = {
    jarURI: options.jarUri ?? "local:///opt/flink/usrlib/sql-runner.jar",
    parallelism: props.parallelism ?? 1,
    ...(upgradeMode ? { upgradeMode } : {}),
    ...(options.jarArgs ? { args: options.jarArgs } : {}),
  }

  return {
    image,
    flinkVersion: flinkVersionStr,
    flinkConfiguration: normalizedConfig,
    jobManager: jobManagerSpec,
    taskManager: taskManagerSpec,
    job: jobSpec,
  }
}

// ── Pipeline Connector podTemplate builders ─────────────────────────

const PIPELINE_YAML_MOUNT_PATH = "/etc/flink-cdc"
const PIPELINE_YAML_VOLUME_NAME = "pipeline-yaml"
const FLINK_CDC_CLI_JAR = "local:///opt/flink-cdc/lib/flink-cdc-cli.jar"
const FLINK_MAIN_CONTAINER = "flink-main-container"

/** Recursively collect unique SecretRefs, de-duplicated by envName. */
export function collectSecretRefs(node: ConstructNode): readonly SecretRef[] {
  const seen = new Map<string, SecretRef>()

  function walk(n: ConstructNode): void {
    for (const value of Object.values(n.props)) {
      if (isSecretRef(value) && !seen.has(value.envName)) {
        seen.set(value.envName, value)
      }
    }
    for (const c of n.children) walk(c)
  }

  walk(node)
  return [...seen.values()]
}

interface PodTemplateContainer {
  readonly name: string
  readonly env?: ReadonlyArray<{
    readonly name: string
    readonly valueFrom: {
      readonly secretKeyRef: { readonly name: string; readonly key: string }
    }
  }>
  readonly volumeMounts?: ReadonlyArray<{
    readonly name: string
    readonly mountPath: string
    readonly readOnly?: boolean
  }>
}

interface PodTemplate {
  readonly spec: {
    readonly containers: readonly PodTemplateContainer[]
    readonly volumes?: ReadonlyArray<{
      readonly name: string
      readonly configMap: { readonly name: string }
    }>
  }
}

function buildPipelineConnectorPodTemplate(
  pipelineName: string,
  secretRefs: readonly SecretRef[],
): PodTemplate {
  const env = secretRefs.map((ref) => ({
    name: ref.envName,
    valueFrom: {
      secretKeyRef: { name: ref.name, key: ref.key },
    },
  }))

  const container: PodTemplateContainer = {
    name: FLINK_MAIN_CONTAINER,
    ...(env.length > 0 ? { env } : {}),
    volumeMounts: [
      {
        name: PIPELINE_YAML_VOLUME_NAME,
        mountPath: PIPELINE_YAML_MOUNT_PATH,
        readOnly: true,
      },
    ],
  }

  return {
    spec: {
      containers: [container],
      volumes: [
        {
          name: PIPELINE_YAML_VOLUME_NAME,
          configMap: { name: pipelineYamlConfigMapName(pipelineName) },
        },
      ],
    },
  }
}

// ── CRD generation ──────────────────────────────────────────────────

/**
 * Generate a Flink CRD object from a Pipeline construct node.
 * Returns FlinkDeploymentCrd by default, or FlinkBlueGreenDeploymentCrd
 * when upgradeStrategy.mode === "blue-green".
 */
export function generateCrd(
  pipelineNode: ConstructNode,
  options: CrdGeneratorOptions,
): AnyFlinkCrd {
  const props = pipelineNode.props as unknown as PipelineProps

  // Pipeline Connector pipelines need a different jarURI, job args, and
  // a podTemplate that mounts the pipeline.yaml ConfigMap + wires SecretRefs
  // as env vars. Blue-green is incompatible with pipeline connectors today.
  const isPipelineConnector = hasPipelineConnectorSource(pipelineNode)

  if (props.upgradeStrategy?.mode === "blue-green") {
    if (isPipelineConnector) {
      throw new Error(
        "Blue-green upgrade strategy is not supported for Flink CDC Pipeline Connector pipelines",
      )
    }
    return generateBlueGreenCrd(props, options)
  }

  if (isPipelineConnector) {
    return generatePipelineConnectorCrd(pipelineNode, props, options)
  }

  return generateFlinkDeploymentCrd(props, options)
}

function generatePipelineConnectorCrd(
  pipelineNode: ConstructNode,
  props: PipelineProps,
  options: CrdGeneratorOptions,
): FlinkDeploymentCrd {
  const cdcOptions: CrdGeneratorOptions = {
    ...options,
    jarUri: options.jarUri ?? FLINK_CDC_CLI_JAR,
    jarArgs: options.jarArgs ?? [
      "--pipeline",
      `${PIPELINE_YAML_MOUNT_PATH}/pipeline.yaml`,
    ],
  }
  const innerSpec = buildInnerSpec(props, cdcOptions)
  const secretRefs = collectSecretRefs(pipelineNode)
  const podTemplate = buildPipelineConnectorPodTemplate(props.name, secretRefs)

  const metadata: FlinkDeploymentCrd["metadata"] = {
    name: props.name,
    ...(options.labels ? { labels: options.labels } : {}),
    ...(options.annotations ? { annotations: options.annotations } : {}),
  }

  return {
    apiVersion: "flink.apache.org/v1beta1",
    kind: "FlinkDeployment",
    metadata,
    spec: {
      image: innerSpec.image,
      flinkVersion: innerSpec.flinkVersion,
      flinkConfiguration: innerSpec.flinkConfiguration,
      jobManager: innerSpec.jobManager,
      taskManager: innerSpec.taskManager,
      podTemplate,
      job: {
        jarURI: innerSpec.job.jarURI,
        parallelism: innerSpec.job.parallelism,
        ...(innerSpec.job.args ? { args: innerSpec.job.args } : {}),
      },
    },
  }
}

function generateFlinkDeploymentCrd(
  props: PipelineProps,
  options: CrdGeneratorOptions,
): FlinkDeploymentCrd {
  const innerSpec = buildInnerSpec(props, options)

  const metadata: FlinkDeploymentCrd["metadata"] = {
    name: props.name,
    ...(options.labels ? { labels: options.labels } : {}),
    ...(options.annotations ? { annotations: options.annotations } : {}),
  }

  return {
    apiVersion: "flink.apache.org/v1beta1",
    kind: "FlinkDeployment",
    metadata,
    spec: {
      image: innerSpec.image,
      flinkVersion: innerSpec.flinkVersion,
      flinkConfiguration: innerSpec.flinkConfiguration,
      jobManager: innerSpec.jobManager,
      taskManager: innerSpec.taskManager,
      job: {
        jarURI: innerSpec.job.jarURI,
        parallelism: innerSpec.job.parallelism,
        ...(innerSpec.job.args ? { args: innerSpec.job.args } : {}),
      },
    },
  }
}

function generateBlueGreenCrd(
  props: PipelineProps,
  options: CrdGeneratorOptions,
): FlinkBlueGreenDeploymentCrd {
  const strategy = props.upgradeStrategy!
  const upgradeMode = strategy.upgradeMode ?? "savepoint"
  const innerSpec = buildInnerSpec(props, options, upgradeMode)

  const metadata: FlinkBlueGreenDeploymentCrd["metadata"] = {
    name: props.name,
    ...(options.labels ? { labels: options.labels } : {}),
    ...(options.annotations ? { annotations: options.annotations } : {}),
  }

  // Build BG operator configuration (duration strings written as-is)
  const bgConfig: Record<string, string> = {}
  if (strategy.blueGreen?.abortGracePeriod) {
    bgConfig["blue-green.abort.grace-period"] =
      strategy.blueGreen.abortGracePeriod
  }
  if (strategy.blueGreen?.deploymentDeletionDelay) {
    bgConfig["blue-green.deployment.deletion.delay"] =
      strategy.blueGreen.deploymentDeletionDelay
  }
  if (strategy.blueGreen?.rescheduleInterval) {
    bgConfig["blue-green.reschedule.interval"] =
      strategy.blueGreen.rescheduleInterval
  }

  // Build ingress config
  const ingress = strategy.ingress
    ? {
        ...(strategy.ingress.template
          ? { template: strategy.ingress.template }
          : {}),
        ...(strategy.ingress.className
          ? { className: strategy.ingress.className }
          : {}),
        ...(strategy.ingress.annotations
          ? { annotations: strategy.ingress.annotations }
          : {}),
      }
    : undefined

  return {
    apiVersion: "flink.apache.org/v1beta1",
    kind: "FlinkBlueGreenDeployment",
    metadata,
    spec: {
      ...(Object.keys(bgConfig).length > 0 ? { configuration: bgConfig } : {}),
      ...(ingress ? { ingress } : {}),
      template: {
        spec: innerSpec as FlinkBlueGreenDeploymentCrd["spec"]["template"]["spec"],
      },
    },
  }
}

// ── YAML serialization ──────────────────────────────────────────────

import { stringify as yamlStringify } from "yaml"

/**
 * Serialize an object to YAML for FlinkDeployment CRDs, ConfigMaps, and
 * Flink CDC pipeline.yaml documents.
 *
 * Backed by eemeli/yaml. The previous hand-rolled emitter punted on
 * multi-line strings, values with leading whitespace, and a few other
 * special-character cases — yaml handles these correctly. Options below
 * are pinned so the output stays stable across yaml minor updates.
 *
 * The legacy `indent` parameter is no longer used (yaml's recursive
 * formatting computes nested indentation itself); it's kept on the
 * signature for backward compatibility with any external caller.
 */
export function toYaml(obj: unknown, _indent: number = 0): string {
  // Strip undefined values so YAML doesn't emit `key: null` for them.
  // The hand-rolled emitter skipped undefined entries; eemeli emits them
  // as null. To preserve historical behavior, recursively prune them.
  const pruned = pruneUndefined(obj)
  return yamlStringify(pruned, {
    indent: 2,
    lineWidth: 0,
    defaultStringType: "PLAIN",
    defaultKeyType: "PLAIN",
    singleQuote: true,
  })
}

function pruneUndefined(value: unknown): unknown {
  if (value === null || value === undefined) return value
  if (Array.isArray(value)) {
    return value.map(pruneUndefined)
  }
  if (typeof value === "object") {
    const out: Record<string, unknown> = {}
    for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
      if (v === undefined) continue
      out[k] = pruneUndefined(v)
    }
    return out
  }
  return value
}

// ── Effect-typed variant ─────────────────────────────────────────────

/**
 * Generate CRD returning Either with typed error.
 * Synchronous, no I/O — uses Either for pure error signaling.
 */
export function generateCrdEither(
  pipelineNode: ConstructNode,
  options: CrdGeneratorOptions,
): Either.Either<AnyFlinkCrd, CrdGenerationError> {
  try {
    return Either.right(generateCrd(pipelineNode, options))
  } catch (err) {
    const pipelineName = (pipelineNode.props.name as string) ?? pipelineNode.id
    return Either.left(
      new CrdGenerationError({
        message: err instanceof Error ? err.message : String(err),
        pipelineName,
      }),
    )
  }
}

/**
 * Generate a Flink CRD and serialize to YAML.
 */
export function generateCrdYaml(
  pipelineNode: ConstructNode,
  options: CrdGeneratorOptions,
): string {
  const crd = generateCrd(pipelineNode, options)
  return toYaml(crd)
}
