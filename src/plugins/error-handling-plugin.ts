import {
  getFlinkConfiguration,
  withFlinkConfiguration,
} from "@/codegen/crd-generator.js"
import type { FlinkReactorPlugin } from "@/core/plugin.js"
import type { ValidationDiagnostic } from "@/core/synth-context.js"
import { findNodes, walkTree } from "@/core/tree-utils.js"
import type { ConstructNode } from "@/core/types.js"

// ── Restart strategy types ──────────────────────────────────────────

export interface FixedDelayStrategy {
  readonly type: "fixed-delay"
  /** Maximum number of restart attempts. Defaults to `3`. */
  readonly attempts?: number
  /** Delay between restarts (e.g., '10s', '1m'). Defaults to `'10s'`. */
  readonly delay?: string
}

export interface FailureRateStrategy {
  readonly type: "failure-rate"
  /** Max failures per interval. Defaults to `3`. */
  readonly maxFailuresPerInterval?: number
  /** Failure rate interval (e.g., '5m', '10m'). Defaults to `'5m'`. */
  readonly failureRateInterval?: string
  /** Delay between restarts (e.g., '10s'). Defaults to `'10s'`. */
  readonly delay?: string
}

export interface ExponentialDelayStrategy {
  readonly type: "exponential-delay"
  /** Initial backoff delay (e.g., '1s'). Defaults to `'1s'`. */
  readonly initialBackoff?: string
  /** Maximum backoff delay (e.g., '5m'). Defaults to `'5m'`. */
  readonly maxBackoff?: string
  /** Backoff multiplier. Defaults to `2.0`. */
  readonly backoffMultiplier?: number
  /** Reset backoff threshold (e.g., '10m'). Defaults to `'10m'`. */
  readonly resetBackoffThreshold?: string
  /** Jitter factor (0.0 - 1.0). Defaults to `0.1`. */
  readonly jitter?: number
}

export type RestartStrategyConfig =
  | FixedDelayStrategy
  | FailureRateStrategy
  | ExponentialDelayStrategy

// ── Plugin options ──────────────────────────────────────────────────

export interface ErrorHandlingPluginOptions {
  /**
   * Restart strategy to configure in the CRD.
   * Defaults to `{ type: 'fixed-delay', attempts: 3, delay: '10s' }`.
   */
  readonly restartStrategy?: RestartStrategyConfig
  /**
   * When true, emit warnings for sinks that have no upstream
   * error-handling component (Validate or SideOutput).
   * Defaults to `true`.
   */
  readonly warnOnUnprotectedSinks?: boolean
  /**
   * Components considered as error-handling guards.
   * Defaults to `['Validate', 'SideOutput']`.
   */
  readonly guardComponents?: readonly string[]
}

// ── Defaults ────────────────────────────────────────────────────────

const DEFAULT_STRATEGY: RestartStrategyConfig = {
  type: "fixed-delay",
  attempts: 3,
  delay: "10s",
}

const DEFAULT_GUARDS = ["Validate", "SideOutput"]

// ── Helper: check if a sink has upstream error handling ─────────────

/**
 * Walk the upstream path from a sink toward sources,
 * looking for any guard component. The tree structure uses
 * parent.children = [upstream], so we walk children recursively.
 */
function hasUpstreamGuard(
  node: ConstructNode,
  guardSet: ReadonlySet<string>,
): boolean {
  let found = false
  walkTree(node, (n) => {
    if (n.id !== node.id && guardSet.has(n.component)) {
      found = true
      return false // stop walking this branch
    }
  })
  return found
}

// ── Plugin factory ──────────────────────────────────────────────────

/**
 * Built-in error handling plugin for FlinkReactor.
 *
 * Demonstrates:
 * - **Validation pattern**: custom rules that warn when sinks lack
 *   upstream error-handling components (Validate, SideOutput)
 * - **CRD transformer pattern**: configures Flink restart strategy
 *   (`fixed-delay`, `failure-rate`, `exponential-delay`)
 *
 * These two concerns are complementary:
 * - Validation catches design-time issues ("you forgot error handling")
 * - CRD transforms handle runtime resilience ("restart gracefully on failure")
 *
 * @example
 * ```ts
 * import { errorHandlingPlugin } from 'flink-reactor/plugins';
 *
 * export default defineConfig({
 *   plugins: [
 *     errorHandlingPlugin({
 *       restartStrategy: { type: 'failure-rate', maxFailuresPerInterval: 5 },
 *       warnOnUnprotectedSinks: true,
 *     }),
 *   ],
 * });
 * ```
 */
export function errorHandlingPlugin(
  options: ErrorHandlingPluginOptions = {},
): FlinkReactorPlugin {
  const strategy = options.restartStrategy ?? DEFAULT_STRATEGY
  const warnOnUnprotectedSinks = options.warnOnUnprotectedSinks ?? true
  const guardComponents = new Set(options.guardComponents ?? DEFAULT_GUARDS)

  return {
    name: "flink-reactor:error-handling",
    version: "0.1.0",
    ordering: { after: ["flink-reactor:logging", "flink-reactor:metrics"] },

    validate: warnOnUnprotectedSinks
      ? (tree: ConstructNode): ValidationDiagnostic[] => {
          const diagnostics: ValidationDiagnostic[] = []

          // Find all sink nodes in the tree
          const sinks = findNodes(tree, (n) => n.kind === "Sink")

          for (const sink of sinks) {
            // Check if any child path of the sink contains a guard component
            if (!hasUpstreamGuard(sink, guardComponents)) {
              diagnostics.push({
                severity: "warning",
                message: `Sink '${sink.component}' (${sink.id}) has no upstream error handling. Consider adding a Validate or SideOutput component.`,
                nodeId: sink.id,
                component: sink.component,
              })
            }
          }

          return diagnostics
        }
      : undefined,

    transformCrd(crd) {
      const config = { ...getFlinkConfiguration(crd) }

      config["restart-strategy.type"] = strategy.type

      switch (strategy.type) {
        case "fixed-delay": {
          config["restart-strategy.fixed-delay.attempts"] = String(
            strategy.attempts ?? 3,
          )
          config["restart-strategy.fixed-delay.delay"] = strategy.delay ?? "10s"
          break
        }
        case "failure-rate": {
          config["restart-strategy.failure-rate.max-failures-per-interval"] =
            String(strategy.maxFailuresPerInterval ?? 3)
          config["restart-strategy.failure-rate.failure-rate-interval"] =
            strategy.failureRateInterval ?? "5m"
          config["restart-strategy.failure-rate.delay"] =
            strategy.delay ?? "10s"
          break
        }
        case "exponential-delay": {
          config["restart-strategy.exponential-delay.initial-backoff"] =
            strategy.initialBackoff ?? "1s"
          config["restart-strategy.exponential-delay.max-backoff"] =
            strategy.maxBackoff ?? "5m"
          config["restart-strategy.exponential-delay.backoff-multiplier"] =
            String(strategy.backoffMultiplier ?? 2.0)
          config["restart-strategy.exponential-delay.reset-backoff-threshold"] =
            strategy.resetBackoffThreshold ?? "10m"
          config["restart-strategy.exponential-delay.jitter-factor"] = String(
            strategy.jitter ?? 0.1,
          )
          break
        }
      }

      // Add strategy metadata as annotation
      const annotations = {
        ...crd.metadata.annotations,
        "flink-reactor.io/restart-strategy": strategy.type,
      }

      const updated = withFlinkConfiguration(crd, config)
      return {
        ...updated,
        metadata: { ...updated.metadata, annotations },
      }
    },
  }
}
