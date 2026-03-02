import { createElement } from "../core/jsx-runtime.js"
import type { ConstructNode } from "../core/types.js"

// ── Pipeline types ──────────────────────────────────────────────────

export type PipelineMode = "streaming" | "batch"

export type StateBackend = "hashmap" | "rocksdb"

export interface CheckpointConfig {
  readonly interval: string
  readonly mode?: "exactly-once" | "at-least-once"
}

export interface RestartStrategy {
  readonly type: "fixed-delay" | "failure-rate" | "no-restart"
  readonly attempts?: number
  readonly delay?: string
}

export interface PipelineProps {
  readonly name: string
  readonly mode?: PipelineMode
  readonly parallelism?: number
  readonly checkpoint?: CheckpointConfig
  readonly stateBackend?: StateBackend
  readonly stateTtl?: string
  readonly restartStrategy?: RestartStrategy
  readonly flinkConfig?: Record<string, string>
  readonly children?: ConstructNode | ConstructNode[]
}

// ── Validation ──────────────────────────────────────────────────────

const VALID_MODES: ReadonlySet<string> = new Set<PipelineMode>([
  "streaming",
  "batch",
])

const VALID_CHECKPOINT_MODES: ReadonlySet<string> = new Set([
  "exactly-once",
  "at-least-once",
])

function validatePipelineProps(props: PipelineProps): void {
  if (props.mode !== undefined && !VALID_MODES.has(props.mode)) {
    throw new Error(
      `Invalid pipeline mode '${props.mode}'. Must be 'streaming' or 'batch'`,
    )
  }

  if (props.checkpoint) {
    if (!props.checkpoint.interval) {
      throw new Error("Checkpoint config requires an interval")
    }
    if (
      props.checkpoint.mode !== undefined &&
      !VALID_CHECKPOINT_MODES.has(props.checkpoint.mode)
    ) {
      throw new Error(
        `Invalid checkpoint mode '${props.checkpoint.mode}'. Must be 'exactly-once' or 'at-least-once'`,
      )
    }
  }
}

// ── Pipeline factory ────────────────────────────────────────────────

/**
 * Pipeline component: wraps source, transform, and sink components.
 *
 * Establishes the synthesis scope and records runtime configuration
 * (parallelism, checkpointing, state backend, etc.) on the construct node.
 */
export function Pipeline(props: PipelineProps): ConstructNode {
  validatePipelineProps(props)

  const { children, ...rest } = props
  const childArray =
    children == null ? [] : Array.isArray(children) ? children : [children]

  return createElement("Pipeline", { ...rest }, ...childArray)
}
