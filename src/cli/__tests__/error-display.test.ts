import { Cause } from "effect"
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import {
  CliError,
  ClusterError,
  ConfigError,
  DiscoveryError,
  FileSystemError,
  SqlGatewayConnectionError,
  SqlGatewayResponseError,
  SqlGatewayTimeoutError,
  SqlGenerationError,
  ValidationError,
} from "../../core/errors.js"
import { renderCause } from "../error-display.js"

describe("renderCause", () => {
  let errorOutput: string[]

  beforeEach(() => {
    errorOutput = []
    vi.spyOn(console, "error").mockImplementation((...args: unknown[]) => {
      errorOutput.push(args.map(String).join(" "))
    })
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it("renders ConfigError with env var name", () => {
    const cause = Cause.fail(
      new ConfigError({
        reason: "missing_env_var",
        message: "DATABASE_URL not set",
        varName: "DATABASE_URL",
      }),
    )

    const exitCode = renderCause(cause)
    expect(exitCode).toBe(1)

    const output = errorOutput.join("\n")
    expect(output).toContain("Configuration error")
    expect(output).toContain("DATABASE_URL not set")
    expect(output).toContain("env var: DATABASE_URL")
  })

  it("renders ConfigError with path", () => {
    const cause = Cause.fail(
      new ConfigError({
        reason: "missing_file",
        message: "config not found",
        path: "/app/flink-reactor.config.ts",
      }),
    )

    renderCause(cause)
    const output = errorOutput.join("\n")
    expect(output).toContain("path: /app/flink-reactor.config.ts")
  })

  it("renders ConfigError with environment name", () => {
    const cause = Cause.fail(
      new ConfigError({
        reason: "unknown_environment",
        message: "unknown env: staging",
        environmentName: "staging",
      }),
    )

    renderCause(cause)
    const output = errorOutput.join("\n")
    expect(output).toContain("environment: staging")
  })

  it("renders SqlGatewayConnectionError with endpoint", () => {
    const cause = Cause.fail(
      new SqlGatewayConnectionError({
        message: "connection refused",
        baseUrl: "http://localhost:8083",
      }),
    )

    const exitCode = renderCause(cause)
    expect(exitCode).toBe(1)

    const output = errorOutput.join("\n")
    expect(output).toContain("SQL Gateway connection error")
    expect(output).toContain("endpoint: http://localhost:8083")
  })

  it("renders SqlGatewayResponseError with HTTP status and endpoint", () => {
    const cause = Cause.fail(
      new SqlGatewayResponseError({
        message: "internal error",
        statusCode: 500,
        baseUrl: "http://gw:8083",
      }),
    )

    renderCause(cause)
    const output = errorOutput.join("\n")
    expect(output).toContain("SQL Gateway error")
    expect(output).toContain("HTTP status: 500")
    expect(output).toContain("endpoint: http://gw:8083")
  })

  it("renders SqlGatewayTimeoutError with elapsed time", () => {
    const cause = Cause.fail(
      new SqlGatewayTimeoutError({
        message: "operation timed out",
        operationHandle: "op-abc",
        elapsedMs: 30000,
      }),
    )

    renderCause(cause)
    const output = errorOutput.join("\n")
    expect(output).toContain("SQL Gateway timeout")
    expect(output).toContain("operation: op-abc")
    expect(output).toContain("elapsed: 30000ms")
  })

  it("renders ValidationError with diagnostics", () => {
    const cause = Cause.fail(
      new ValidationError({
        diagnostics: [
          {
            severity: "error",
            message: "orphan source",
            component: "KafkaSource",
            nodeId: "src_1",
          },
          {
            severity: "warning",
            message: "no watermark",
            nodeId: "src_2",
          },
        ],
      }),
    )

    renderCause(cause)
    const output = errorOutput.join("\n")
    expect(output).toContain("error:")
    expect(output).toContain("orphan source")
    expect(output).toContain("component: KafkaSource")
    expect(output).toContain("warning:")
    expect(output).toContain("no watermark")
  })

  it("renders CliError concisely without stack traces", () => {
    const cause = Cause.fail(
      new CliError({
        reason: "missing_tool",
        message: "kubectl not found",
        tool: "kubectl",
      }),
    )

    renderCause(cause)
    const output = errorOutput.join("\n")
    expect(output).toContain("kubectl not found")
    // Should NOT contain stack trace indicators
    expect(output).not.toContain("at ")
    expect(output).not.toContain("Error:")
  })

  it("renders DiscoveryError with path", () => {
    const cause = Cause.fail(
      new DiscoveryError({
        reason: "pipeline_not_found",
        message: "no pipelines/ directory",
        path: "/app/pipelines",
      }),
    )

    renderCause(cause)
    const output = errorOutput.join("\n")
    expect(output).toContain("Discovery error")
    expect(output).toContain("path: /app/pipelines")
  })

  it("renders FileSystemError with operation and path", () => {
    const cause = Cause.fail(
      new FileSystemError({
        message: "ENOENT",
        path: "/app/config.ts",
        operation: "read",
      }),
    )

    renderCause(cause)
    const output = errorOutput.join("\n")
    expect(output).toContain("File system error (read)")
    expect(output).toContain("path: /app/config.ts")
  })

  it("renders ClusterError with command", () => {
    const cause = Cause.fail(
      new ClusterError({
        reason: "docker_failure",
        message: "docker daemon not running",
        command: "docker compose up",
      }),
    )

    renderCause(cause)
    const output = errorOutput.join("\n")
    expect(output).toContain("Cluster error")
    expect(output).toContain("command: docker compose up")
  })

  it("renders SqlGenerationError with component", () => {
    const cause = Cause.fail(
      new SqlGenerationError({
        message: "unsupported join",
        component: "TemporalJoin",
      }),
    )

    renderCause(cause)
    const output = errorOutput.join("\n")
    expect(output).toContain("SQL generation error")
    expect(output).toContain("component: TemporalJoin")
  })

  it("renders defects with concise message by default", () => {
    const cause = Cause.die(new Error("unexpected null reference"))

    renderCause(cause)
    const output = errorOutput.join("\n")
    expect(output).toContain("Unexpected error: unexpected null reference")
    expect(output).toContain("--verbose")
  })

  it("renders defects with full trace when verbose", () => {
    const err = new Error("unexpected null reference")
    err.stack = "Error: unexpected null reference\n    at foo.ts:42"
    const cause = Cause.die(err)

    renderCause(cause, { verbose: true })
    const output = errorOutput.join("\n")
    expect(output).toContain("full trace")
    expect(output).toContain("at foo.ts:42")
  })

  it("renders interruption with exit code 130", () => {
    const cause = Cause.interrupt("fiber-1")

    const exitCode = renderCause(cause)
    expect(exitCode).toBe(130)

    const output = errorOutput.join("\n")
    expect(output).toContain("Operation cancelled")
  })

  it("renders multiple failures from parallel effects", () => {
    const cause = Cause.parallel(
      Cause.fail(
        new ConfigError({
          reason: "missing_env_var",
          message: "DB_URL not set",
          varName: "DB_URL",
        }),
      ),
      Cause.fail(
        new DiscoveryError({
          reason: "config_not_found",
          message: "no config",
          path: "/app",
        }),
      ),
    )

    renderCause(cause)
    const output = errorOutput.join("\n")
    expect(output).toContain("DB_URL not set")
    expect(output).toContain("no config")
  })
})
