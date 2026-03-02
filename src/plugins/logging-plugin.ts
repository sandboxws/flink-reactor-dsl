import type { FlinkReactorPlugin } from "../core/plugin.js"
import { findNodes, mapTree } from "../core/tree-utils.js"
import type { ConstructNode } from "../core/types.js"

// ── Configuration ───────────────────────────────────────────────────

export interface LoggingPluginOptions {
  /**
   * Which component kinds to add logging metadata to.
   * Defaults to `['Source', 'Sink', 'Transform']`.
   */
  readonly targets?: readonly ConstructNode["kind"][]
  /**
   * A string tag added to each targeted node's props for identification.
   * Defaults to `'pipeline-log'`.
   */
  readonly tag?: string
  /**
   * Log level to configure in Flink. One of: TRACE, DEBUG, INFO, WARN, ERROR.
   * Defaults to `'INFO'`.
   */
  readonly level?: "TRACE" | "DEBUG" | "INFO" | "WARN" | "ERROR"
  /**
   * Additional Flink logger categories to configure.
   * Each entry maps a logger name (e.g., `'org.apache.flink.streaming'`) to a level.
   */
  readonly loggers?: Readonly<
    Record<string, "TRACE" | "DEBUG" | "INFO" | "WARN" | "ERROR">
  >
  /**
   * When true, configures Flink to include task/operator name in log output.
   * Defaults to `true`.
   */
  readonly includeOperatorName?: boolean
}

// ── Defaults ────────────────────────────────────────────────────────

const DEFAULT_TARGETS: readonly ConstructNode["kind"][] = [
  "Source",
  "Sink",
  "Transform",
]
const DEFAULT_TAG = "pipeline-log"
const DEFAULT_LEVEL = "INFO"

// ── Plugin factory ──────────────────────────────────────────────────

/**
 * Built-in logging plugin for FlinkReactor.
 *
 * Demonstrates:
 * - **Tree transformer pattern**: uses `mapTree()` to annotate targeted
 *   nodes with logging metadata
 * - **CRD transformer pattern**: injects Flink logging configuration
 *   into `flinkConfiguration`
 * - **Configurable targeting**: choose which component kinds to observe
 *
 * The plugin works by:
 * 1. Annotating targeted nodes with a `_logging` prop (tree transformer)
 * 2. Adding Flink log configuration keys to the CRD (CRD transformer)
 *
 * @example
 * ```ts
 * import { loggingPlugin } from 'flink-reactor/plugins';
 *
 * export default defineConfig({
 *   plugins: [
 *     loggingPlugin({ level: 'DEBUG', loggers: { 'org.apache.flink.streaming': 'DEBUG' } }),
 *   ],
 * });
 * ```
 */
export function loggingPlugin(
  options: LoggingPluginOptions = {},
): FlinkReactorPlugin {
  const targets = new Set(options.targets ?? DEFAULT_TARGETS)
  const tag = options.tag ?? DEFAULT_TAG
  const level = options.level ?? DEFAULT_LEVEL
  const loggers = options.loggers ?? {}
  const includeOperatorName = options.includeOperatorName ?? true

  return {
    name: "flink-reactor:logging",
    version: "0.1.0",
    ordering: { before: ["flink-reactor:metrics"] },

    transformTree(tree: ConstructNode): ConstructNode {
      return mapTree(tree, (node) => {
        if (targets.has(node.kind) && node.kind !== "Pipeline") {
          return {
            ...node,
            props: {
              ...node.props,
              _logging: { tag, level },
            },
          }
        }
        return node
      })
    },

    transformCrd(crd, pipelineNode) {
      const config = { ...crd.spec.flinkConfiguration }

      // Set root logger level
      config["rootLogger.level"] = level

      // Add operator name to log pattern if requested
      if (includeOperatorName) {
        config["rootLogger.appenderRef.file.ref"] = "LogFile"
        config["rootLogger.appenderRef.console.ref"] = "LogConsole"
      }

      // Configure additional loggers
      for (const [loggerName, loggerLevel] of Object.entries(loggers)) {
        config[`logger.${loggerName.replace(/\./g, "_")}.name`] = loggerName
        config[`logger.${loggerName.replace(/\./g, "_")}.level`] = loggerLevel
      }

      // Count annotated nodes to add as metadata annotation
      const annotatedCount = findNodes(
        pipelineNode,
        (n) => n.props._logging != null,
      ).length

      const annotations = {
        ...crd.metadata.annotations,
        "flink-reactor.io/logging-tag": tag,
        "flink-reactor.io/logging-targets": String(annotatedCount),
      }

      return {
        ...crd,
        metadata: { ...crd.metadata, annotations },
        spec: { ...crd.spec, flinkConfiguration: config },
      }
    },
  }
}
