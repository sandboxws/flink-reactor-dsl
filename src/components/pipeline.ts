import { createElement } from "@/core/jsx-runtime.js"
import type { ConstructNode } from "@/core/types.js"

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

// ── Blue-green upgrade strategy types ────────────────────────────────

export type UpgradeMode = "stateless" | "savepoint" | "last-state"

export interface BlueGreenConfig {
  readonly abortGracePeriod?: string
  readonly deploymentDeletionDelay?: string
  readonly rescheduleInterval?: string
}

export interface IngressConfig {
  readonly template?: string
  readonly className?: string
  readonly annotations?: Record<string, string>
}

export interface UpgradeStrategy {
  readonly mode: "blue-green"
  readonly upgradeMode?: UpgradeMode
  readonly blueGreen?: BlueGreenConfig
  readonly ingress?: IngressConfig
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
  readonly upgradeStrategy?: UpgradeStrategy
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

/** Warnings emitted during validation (non-fatal) */
export type ValidationWarning = {
  readonly level: "warning"
  readonly message: string
}

/** Collected warnings from the most recent Pipeline validation */
let lastValidationWarnings: ValidationWarning[] = []

/** Retrieve warnings from the most recent Pipeline() call, then clear. */
export function consumeValidationWarnings(): ValidationWarning[] {
  const warnings = lastValidationWarnings
  lastValidationWarnings = []
  return warnings
}

function validatePipelineProps(props: PipelineProps): void {
  lastValidationWarnings = []

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

  // Blue-green validation
  if (props.upgradeStrategy?.mode === "blue-green") {
    if (props.mode === "batch") {
      throw new Error(
        "Blue-green upgrade strategy is not supported for batch pipelines. Blue-green requires a long-running streaming job.",
      )
    }

    if (!props.checkpoint) {
      lastValidationWarnings.push({
        level: "warning",
        message:
          "Blue-green upgrade strategy without checkpoint configuration is risky. Stateful blue-green transitions rely on savepoints, which require checkpointing to be enabled.",
      })
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
