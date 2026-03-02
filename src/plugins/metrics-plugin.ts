import type { FlinkReactorPlugin } from "@/core/plugin.js"

// ── Reporter configuration types ────────────────────────────────────

export interface PrometheusReporterConfig {
  readonly type: "prometheus"
  /** Port for the Prometheus endpoint. Defaults to `9249`. */
  readonly port?: number
}

export interface Slf4jReporterConfig {
  readonly type: "slf4j"
  /** Reporting interval (e.g., '60s', '5m'). Defaults to `'60s'`. */
  readonly interval?: string
}

export interface JmxReporterConfig {
  readonly type: "jmx"
  /** JMX port range (e.g., '8789'). Defaults to `'8789'`. */
  readonly port?: string
}

export type MetricReporterConfig =
  | PrometheusReporterConfig
  | Slf4jReporterConfig
  | JmxReporterConfig

// ── Plugin options ──────────────────────────────────────────────────

export interface MetricsPluginOptions {
  /**
   * Metric reporters to configure. Can specify multiple reporters
   * which will all be active simultaneously.
   *
   * @example
   * ```ts
   * metricsPlugin({
   *   reporters: [
   *     { type: 'prometheus', port: 9249 },
   *     { type: 'slf4j', interval: '30s' },
   *   ],
   * })
   * ```
   */
  readonly reporters: readonly MetricReporterConfig[]
  /**
   * Flink metric scope delimiter. Defaults to `'.'`.
   */
  readonly scopeDelimiter?: string
  /**
   * Latency tracking interval in milliseconds. Set to 0 to disable.
   * Defaults to `undefined` (Flink default: disabled).
   */
  readonly latencyTrackingInterval?: number
}

// ── Reporter factory classes ────────────────────────────────────────

const REPORTER_FACTORIES: Record<string, string> = {
  prometheus: "org.apache.flink.metrics.prometheus.PrometheusReporterFactory",
  slf4j: "org.apache.flink.metrics.slf4j.Slf4jReporterFactory",
  jmx: "org.apache.flink.metrics.jmx.JMXReporterFactory",
}

// ── Plugin factory ──────────────────────────────────────────────────

/**
 * Built-in metrics plugin for FlinkReactor.
 *
 * Demonstrates:
 * - **CRD transformer pattern**: injects Flink metric reporter configuration
 *   into `flinkConfiguration` without modifying the construct tree or SQL
 * - **Multiple reporter support**: configure Prometheus, SLF4J, and JMX simultaneously
 *
 * The plugin configures Flink's built-in metrics system by adding the
 * appropriate configuration keys to the FlinkDeployment CRD. No tree
 * modifications or SQL changes are needed — metrics are a runtime concern
 * handled entirely by Flink's metrics subsystem.
 *
 * @example
 * ```ts
 * import { metricsPlugin } from 'flink-reactor/plugins';
 *
 * export default defineConfig({
 *   plugins: [
 *     metricsPlugin({
 *       reporters: [{ type: 'prometheus', port: 9249 }],
 *     }),
 *   ],
 * });
 * ```
 */
export function metricsPlugin(
  options: MetricsPluginOptions,
): FlinkReactorPlugin {
  const { reporters, scopeDelimiter, latencyTrackingInterval } = options

  return {
    name: "flink-reactor:metrics",
    version: "0.1.0",

    transformCrd(crd) {
      const config = { ...crd.spec.flinkConfiguration }

      // Configure each reporter
      for (const reporter of reporters) {
        const name = reporter.type
        const prefix = `metrics.reporter.${name}`

        const factory = REPORTER_FACTORIES[reporter.type]
        if (factory) {
          config[`${prefix}.factory.class`] = factory
        }

        switch (reporter.type) {
          case "prometheus": {
            config[`${prefix}.port`] = String(reporter.port ?? 9249)
            break
          }
          case "slf4j": {
            config[`${prefix}.interval`] = reporter.interval ?? "60s"
            break
          }
          case "jmx": {
            config[`${prefix}.port`] = reporter.port ?? "8789"
            break
          }
        }
      }

      // Scope delimiter
      if (scopeDelimiter !== undefined) {
        config["metrics.scope.delimiter"] = scopeDelimiter
      }

      // Latency tracking
      if (latencyTrackingInterval !== undefined) {
        config["metrics.latency.interval"] = String(latencyTrackingInterval)
      }

      // Add reporter metadata as annotation
      const reporterNames = reporters.map((r) => r.type).join(",")
      const annotations = {
        ...crd.metadata.annotations,
        "flink-reactor.io/metric-reporters": reporterNames,
      }

      return {
        ...crd,
        metadata: { ...crd.metadata, annotations },
        spec: { ...crd.spec, flinkConfiguration: config },
      }
    },
  }
}
