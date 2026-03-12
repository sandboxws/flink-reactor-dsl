// ── Effect service interfaces ───────────────────────────────────────
// Context.Tag services define the I/O boundaries of the system.
// Each service has a separate interface + Tag for dependency injection.
// Layers provide concrete implementations; tests can swap them.

import type { Effect } from "effect"
import { Context } from "effect"
import type { FlinkReactorConfig } from "./config.js"
import type { ResolvedConfig } from "./config-resolver.js"
import type { DiscoveryError, FileSystemError } from "./errors.js"
import type { ConstructNode } from "./types.js"

// ── FrFileSystem ────────────────────────────────────────────────────

/** Abstraction over filesystem operations for synthesis and CLI */
export interface FrFileSystemService {
  readonly readFile: (path: string) => Effect.Effect<string, FileSystemError>
  readonly writeFile: (
    path: string,
    content: string,
  ) => Effect.Effect<void, FileSystemError>
  readonly exists: (path: string) => Effect.Effect<boolean, FileSystemError>
  readonly readdir: (
    path: string,
  ) => Effect.Effect<readonly string[], FileSystemError>
  readonly stat: (
    path: string,
  ) => Effect.Effect<{ readonly isDirectory: boolean }, FileSystemError>
  readonly mkdir: (
    path: string,
    options?: { readonly recursive?: boolean },
  ) => Effect.Effect<void, FileSystemError>
}

export class FrFileSystem extends Context.Tag("FrFileSystem")<
  FrFileSystem,
  FrFileSystemService
>() {}

// ── ProcessRunner ───────────────────────────────────────────────────

export interface ProcessResult {
  readonly exitCode: number
  readonly stdout: string
  readonly stderr: string
}

/** Abstraction over child process execution (kubectl, docker, etc.) */
export interface ProcessRunnerService {
  readonly exec: (
    command: string,
    args: readonly string[],
    options?: {
      readonly cwd?: string
      readonly env?: Record<string, string>
      readonly timeout?: number
    },
  ) => Effect.Effect<ProcessResult>
}

export class ProcessRunner extends Context.Tag("ProcessRunner")<
  ProcessRunner,
  ProcessRunnerService
>() {}

// ── FrHttpClient ────────────────────────────────────────────────────

export interface HttpResponse {
  readonly status: number
  readonly statusText: string
  readonly json: () => Effect.Effect<unknown>
  readonly text: () => Effect.Effect<string>
}

/** Abstraction over HTTP client for SQL Gateway and other external APIs */
export interface FrHttpClientService {
  readonly request: (
    url: string,
    options?: {
      readonly method?: string
      readonly headers?: Record<string, string>
      readonly body?: string
    },
  ) => Effect.Effect<HttpResponse>
}

export class FrHttpClient extends Context.Tag("FrHttpClient")<
  FrHttpClient,
  FrHttpClientService
>() {}

// ── ConfigProvider ──────────────────────────────────────────────────

/** Service for loading project configuration and resolving environments */
export interface ConfigProviderService {
  readonly loadConfig: (
    projectDir: string,
  ) => Effect.Effect<FlinkReactorConfig | null, DiscoveryError>
  readonly resolveConfig: (
    config: FlinkReactorConfig,
    envName?: string,
  ) => Effect.Effect<ResolvedConfig>
}

export class ConfigProvider extends Context.Tag("ConfigProvider")<
  ConfigProvider,
  ConfigProviderService
>() {}

// ── PipelineLoader ──────────────────────────────────────────────────

/** Service for dynamically importing pipeline entry points */
export interface PipelineLoaderService {
  readonly loadPipeline: (
    entryPoint: string,
    projectDir: string,
  ) => Effect.Effect<ConstructNode, DiscoveryError>
}

export class PipelineLoader extends Context.Tag("PipelineLoader")<
  PipelineLoader,
  PipelineLoaderService
>() {}

// ── CliOutput ───────────────────────────────────────────────────────

/** Abstraction over styled console output for CLI commands */
export interface CliOutputService {
  readonly log: (message: string) => Effect.Effect<void>
  readonly warn: (message: string) => Effect.Effect<void>
  readonly error: (message: string) => Effect.Effect<void>
  readonly success: (message: string) => Effect.Effect<void>
}

export class CliOutput extends Context.Tag("CliOutput")<
  CliOutput,
  CliOutputService
>() {}
