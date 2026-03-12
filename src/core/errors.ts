// ── Effect error hierarchy ──────────────────────────────────────────
// Tagged errors using Data.TaggedError for exhaustive matching.
// Each error carries structured context (not just a message string),
// enabling Effect's typed error channel (Effect<A, E, R>).

import { Data } from "effect"
import type { ValidationDiagnostic } from "./synth-context.js"

// ── Config & Environment ────────────────────────────────────────────

export class ConfigError extends Data.TaggedError("ConfigError")<{
  readonly reason: "missing_env_var" | "invalid_config" | "unknown_environment"
  readonly message: string
  readonly varName?: string
  readonly path?: string
  readonly environmentName?: string
}> {}

// ── Validation ──────────────────────────────────────────────────────

export class ValidationError extends Data.TaggedError("ValidationError")<{
  readonly diagnostics: readonly ValidationDiagnostic[]
}> {}

// ── Graph ───────────────────────────────────────────────────────────

export class CycleDetectedError extends Data.TaggedError("CycleDetectedError")<{
  readonly nodeIds: readonly string[]
  readonly message: string
}> {}

// ── Schema ──────────────────────────────────────────────────────────

export class SchemaError extends Data.TaggedError("SchemaError")<{
  readonly message: string
  readonly component?: string
  readonly field?: string
  readonly expected?: string
  readonly actual?: string
}> {}

// ── Code generation ─────────────────────────────────────────────────

export class SqlGenerationError extends Data.TaggedError("SqlGenerationError")<{
  readonly message: string
  readonly component?: string
  readonly nodeId?: string
}> {}

export class CrdGenerationError extends Data.TaggedError("CrdGenerationError")<{
  readonly message: string
  readonly pipelineName?: string
}> {}

// ── Plugins ─────────────────────────────────────────────────────────

export class PluginError extends Data.TaggedError("PluginError")<{
  readonly reason: "duplicate_name" | "circular_ordering" | "conflict"
  readonly message: string
  readonly pluginName?: string
}> {}

// ── SQL Gateway ─────────────────────────────────────────────────────

export class SqlGatewayConnectionError extends Data.TaggedError(
  "SqlGatewayConnectionError",
)<{
  readonly message: string
  readonly baseUrl: string
}> {}

export class SqlGatewayResponseError extends Data.TaggedError(
  "SqlGatewayResponseError",
)<{
  readonly message: string
  readonly statusCode: number
  readonly baseUrl?: string
}> {}

export class SqlGatewayTimeoutError extends Data.TaggedError(
  "SqlGatewayTimeoutError",
)<{
  readonly message: string
  readonly operationHandle?: string
  readonly elapsedMs: number
}> {}

/** Union of all SQL Gateway errors */
export type SqlGatewayError =
  | SqlGatewayConnectionError
  | SqlGatewayResponseError
  | SqlGatewayTimeoutError

// ── Discovery ───────────────────────────────────────────────────────

export class DiscoveryError extends Data.TaggedError("DiscoveryError")<{
  readonly reason:
    | "config_not_found"
    | "pipeline_not_found"
    | "import_failure"
    | "env_not_found"
  readonly message: string
  readonly path?: string
}> {}

// ── File system ─────────────────────────────────────────────────────

export class FileSystemError extends Data.TaggedError("FileSystemError")<{
  readonly message: string
  readonly path: string
  readonly operation: "read" | "write" | "exists" | "readdir"
}> {}

// ── Cluster ─────────────────────────────────────────────────────────

export class ClusterError extends Data.TaggedError("ClusterError")<{
  readonly reason: "docker_failure" | "kubectl_failure" | "timeout"
  readonly message: string
  readonly command?: string
}> {}

// ── CLI ─────────────────────────────────────────────────────────────

export class CliError extends Data.TaggedError("CliError")<{
  readonly reason: "missing_tool" | "user_input" | "invalid_args"
  readonly message: string
  readonly tool?: string
}> {}

// ── Union type ──────────────────────────────────────────────────────

/** Union of all FlinkReactor DSL errors */
export type SynthError =
  | ConfigError
  | ValidationError
  | CycleDetectedError
  | SchemaError
  | SqlGenerationError
  | CrdGenerationError
  | PluginError
  | SqlGatewayConnectionError
  | SqlGatewayResponseError
  | SqlGatewayTimeoutError
  | DiscoveryError
  | FileSystemError
  | ClusterError
  | CliError
