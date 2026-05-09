import { FlinkVersionCompat } from "@/core/flink-compat.js"
import type { ConstructNode, FlinkMajorVersion } from "@/core/types.js"
import { toMilliseconds } from "./sql-duration.js"

/**
 * Generate the leading `SET 'key' = 'value';` block from a Pipeline node's
 * configuration props (parallelism, checkpoint, state backend, etc.).
 *
 * The pipeline name is always emitted so Flink's job name matches the DSL
 * pipeline — dashboards key on this when matching live jobs to tap
 * manifests. User-supplied overrides via `flinkConfig` win over derived
 * defaults; the final dictionary runs through
 * `FlinkVersionCompat.normalizeConfig` to translate version-aware option
 * keys (e.g. checkpointing renames between Flink 1.x and 2.x).
 */
export function generateSetStatements(
  pipeline: ConstructNode,
  version: FlinkMajorVersion,
): string[] {
  const config: Record<string, string> = {}
  const props = pipeline.props

  // Always set pipeline.name so Flink job name matches the pipeline name.
  // This enables the dashboard to match running jobs to tap manifests by name.
  const pipelineName = props.name as string | undefined
  if (pipelineName) {
    config["pipeline.name"] = pipelineName
  }

  if (props.mode) {
    config["execution.runtime-mode"] = props.mode as string
  }

  if (props.parallelism !== undefined) {
    config["parallelism.default"] = String(props.parallelism)
  }

  const checkpoint = props.checkpoint as
    | { interval: string; mode?: string }
    | undefined
  if (checkpoint) {
    config["execution.checkpointing.interval"] = String(
      toMilliseconds(checkpoint.interval),
    )
    if (checkpoint.mode) {
      // Flink SQL expects the enum literals EXACTLY_ONCE / AT_LEAST_ONCE,
      // not the DSL-ergonomic kebab-case values.
      config["execution.checkpointing.mode"] =
        checkpoint.mode === "exactly-once" ? "EXACTLY_ONCE" : "AT_LEAST_ONCE"
    }
  }

  if (props.stateBackend) {
    config["state.backend.type"] = props.stateBackend as string
  }

  if (props.stateTtl) {
    config["table.exec.state.ttl"] = String(
      toMilliseconds(props.stateTtl as string),
    )
  }

  if (props.restartStrategy) {
    const rs = props.restartStrategy as {
      type: string
      attempts?: number
      delay?: string
    }
    config["restart-strategy.type"] = rs.type
    if (rs.attempts !== undefined) {
      config["restart-strategy.fixed-delay.attempts"] = String(rs.attempts)
    }
    if (rs.delay) {
      config["restart-strategy.fixed-delay.delay"] = rs.delay
    }
  }

  const userConfig = props.flinkConfig as Record<string, string> | undefined
  if (userConfig) {
    Object.assign(config, userConfig)
  }

  const normalized = FlinkVersionCompat.normalizeConfig(config, version)

  return Object.entries(normalized).map(
    ([key, value]) => `SET '${key}' = '${value}';`,
  )
}
