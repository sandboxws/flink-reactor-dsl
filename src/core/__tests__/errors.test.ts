import { describe, expect, it } from "vitest"
import {
  CliError,
  ClusterError,
  ConfigError,
  CrdGenerationError,
  CycleDetectedError,
  DiscoveryError,
  FileSystemError,
  PluginError,
  SchemaError,
  SqlGatewayConnectionError,
  SqlGatewayResponseError,
  SqlGatewayTimeoutError,
  SqlGenerationError,
  ValidationError,
} from "../errors.js"

describe("Effect error hierarchy", () => {
  it("ConfigError has correct _tag and fields", () => {
    const err = new ConfigError({
      reason: "missing_env_var",
      message: "DATABASE_URL not set",
      varName: "DATABASE_URL",
    })
    expect(err._tag).toBe("ConfigError")
    expect(err.message).toBe("DATABASE_URL not set")
    expect(err.reason).toBe("missing_env_var")
    expect(err.varName).toBe("DATABASE_URL")
  })

  it("ConfigError supports all reason variants", () => {
    expect(
      new ConfigError({ reason: "invalid_config", message: "bad yaml" }).reason,
    ).toBe("invalid_config")
    expect(
      new ConfigError({
        reason: "unknown_environment",
        message: "no env",
        environmentName: "staging",
      }).environmentName,
    ).toBe("staging")
  })

  it("ValidationError has correct _tag and diagnostics", () => {
    const diagnostics = [
      {
        severity: "error" as const,
        message: "type mismatch",
        nodeId: "n1",
        ruleId: "r1",
      },
    ]
    const err = new ValidationError({ diagnostics })
    expect(err._tag).toBe("ValidationError")
    expect(err.diagnostics).toEqual(diagnostics)
  })

  it("CycleDetectedError has correct _tag and nodeIds", () => {
    const err = new CycleDetectedError({
      nodeIds: ["a", "b", "c"],
      message: "cycle: a → b → c → a",
    })
    expect(err._tag).toBe("CycleDetectedError")
    expect(err.nodeIds).toEqual(["a", "b", "c"])
    expect(err.message).toBe("cycle: a → b → c → a")
  })

  it("SchemaError has correct _tag and structured fields", () => {
    const err = new SchemaError({
      message: "type mismatch",
      component: "Filter",
      field: "amount",
      expected: "BIGINT",
      actual: "STRING",
    })
    expect(err._tag).toBe("SchemaError")
    expect(err.component).toBe("Filter")
    expect(err.field).toBe("amount")
    expect(err.expected).toBe("BIGINT")
    expect(err.actual).toBe("STRING")
  })

  it("SqlGenerationError has correct _tag and fields", () => {
    const err = new SqlGenerationError({
      message: "unsupported join type",
      component: "Join",
      nodeId: "join_1",
    })
    expect(err._tag).toBe("SqlGenerationError")
    expect(err.component).toBe("Join")
    expect(err.nodeId).toBe("join_1")
  })

  it("CrdGenerationError has correct _tag and fields", () => {
    const err = new CrdGenerationError({
      message: "invalid parallelism",
      pipelineName: "orders",
    })
    expect(err._tag).toBe("CrdGenerationError")
    expect(err.pipelineName).toBe("orders")
  })

  it("PluginError has correct _tag and reason", () => {
    const err = new PluginError({
      reason: "duplicate_name",
      message: "plugin 'metrics' already registered",
      pluginName: "metrics",
    })
    expect(err._tag).toBe("PluginError")
    expect(err.reason).toBe("duplicate_name")
    expect(err.pluginName).toBe("metrics")
  })

  it("SqlGatewayConnectionError has correct _tag", () => {
    const err = new SqlGatewayConnectionError({
      message: "connection refused",
      baseUrl: "http://localhost:8083",
    })
    expect(err._tag).toBe("SqlGatewayConnectionError")
    expect(err.baseUrl).toBe("http://localhost:8083")
  })

  it("SqlGatewayResponseError has correct _tag and statusCode", () => {
    const err = new SqlGatewayResponseError({
      message: "internal error",
      statusCode: 500,
      baseUrl: "http://localhost:8083",
    })
    expect(err._tag).toBe("SqlGatewayResponseError")
    expect(err.statusCode).toBe(500)
  })

  it("SqlGatewayTimeoutError has correct _tag and elapsedMs", () => {
    const err = new SqlGatewayTimeoutError({
      message: "operation timed out",
      operationHandle: "op-123",
      elapsedMs: 30000,
    })
    expect(err._tag).toBe("SqlGatewayTimeoutError")
    expect(err.elapsedMs).toBe(30000)
    expect(err.operationHandle).toBe("op-123")
  })

  it("DiscoveryError has correct _tag and reason", () => {
    const err = new DiscoveryError({
      reason: "config_not_found",
      message: "no flink-reactor.config.ts found",
      path: "/app",
    })
    expect(err._tag).toBe("DiscoveryError")
    expect(err.reason).toBe("config_not_found")
    expect(err.path).toBe("/app")
  })

  it("FileSystemError has correct _tag and operation", () => {
    const err = new FileSystemError({
      message: "ENOENT",
      path: "/missing/file.ts",
      operation: "read",
    })
    expect(err._tag).toBe("FileSystemError")
    expect(err.path).toBe("/missing/file.ts")
    expect(err.operation).toBe("read")
  })

  it("ClusterError has correct _tag and reason", () => {
    const err = new ClusterError({
      reason: "docker_failure",
      message: "docker daemon not running",
      command: "docker ps",
    })
    expect(err._tag).toBe("ClusterError")
    expect(err.reason).toBe("docker_failure")
    expect(err.command).toBe("docker ps")
  })

  it("CliError has correct _tag and reason", () => {
    const err = new CliError({
      reason: "missing_tool",
      message: "kubectl not found",
      tool: "kubectl",
    })
    expect(err._tag).toBe("CliError")
    expect(err.reason).toBe("missing_tool")
    expect(err.tool).toBe("kubectl")
  })

  it("all errors are instanceof Error", () => {
    const errors = [
      new ConfigError({ reason: "invalid_config", message: "bad" }),
      new ValidationError({ diagnostics: [] }),
      new CycleDetectedError({ nodeIds: [], message: "cycle" }),
      new SchemaError({ message: "bad schema" }),
      new SqlGenerationError({ message: "codegen fail" }),
      new CrdGenerationError({ message: "crd fail" }),
      new PluginError({ reason: "conflict", message: "conflict" }),
      new SqlGatewayConnectionError({
        message: "refused",
        baseUrl: "http://x",
      }),
      new SqlGatewayResponseError({ message: "500", statusCode: 500 }),
      new SqlGatewayTimeoutError({ message: "timeout", elapsedMs: 1000 }),
      new DiscoveryError({ reason: "config_not_found", message: "missing" }),
      new FileSystemError({ message: "ENOENT", path: "/x", operation: "read" }),
      new ClusterError({ reason: "timeout", message: "timed out" }),
      new CliError({ reason: "invalid_args", message: "bad args" }),
    ]
    for (const err of errors) {
      expect(err).toBeInstanceOf(Error)
    }
  })
})
