// ── Layer graph ─────────────────────────────────────────────────────
// Composed Layer providing all service implementations.
// MainLive is the full production layer; test layers swap individual
// services for in-memory or mock implementations.

import { Effect, Layer } from "effect"
import type { FlinkReactorConfig } from "./config.js"
import type { ResolvedConfig } from "./config-resolver.js"
import { resolveConfig as resolveConfigFn } from "./config-resolver.js"
import { DiscoveryError, FileSystemError } from "./errors.js"
import {
  CliOutput,
  ConfigProvider,
  FrFileSystem,
  FrHttpClient,
  PipelineLoader,
  ProcessEnv,
  ProcessRunner,
} from "./services.js"

// ── ProcessEnv — process.env backed implementation ──────────────────

export const ProcessEnvLive = Layer.succeed(ProcessEnv, {
  get: (name: string) => Effect.sync(() => process.env[name]),
})

// ── FrFileSystem — Node.js implementation ───────────────────────────

export const NodeFileSystemLive = Layer.succeed(FrFileSystem, {
  readFile: (path: string) =>
    Effect.tryPromise({
      try: async () => {
        const { readFile } = await import("node:fs/promises")
        return readFile(path, "utf-8")
      },
      catch: (err) =>
        new FileSystemError({
          message: (err as Error).message,
          path,
          operation: "read",
        }),
    }),

  writeFile: (path: string, content: string) =>
    Effect.tryPromise({
      try: async () => {
        const { writeFile } = await import("node:fs/promises")
        await writeFile(path, content, "utf-8")
      },
      catch: (err) =>
        new FileSystemError({
          message: (err as Error).message,
          path,
          operation: "write",
        }),
    }),

  exists: (path: string) =>
    Effect.try({
      try: () => {
        const { existsSync } = require("node:fs") as typeof import("node:fs")
        return existsSync(path)
      },
      catch: (err) =>
        new FileSystemError({
          message: (err as Error).message,
          path,
          operation: "exists",
        }),
    }),

  readdir: (path: string) =>
    Effect.tryPromise({
      try: async () => {
        const { readdir } = await import("node:fs/promises")
        return readdir(path)
      },
      catch: (err) =>
        new FileSystemError({
          message: (err as Error).message,
          path,
          operation: "readdir",
        }),
    }),

  stat: (path: string) =>
    Effect.tryPromise({
      try: async () => {
        const { stat } = await import("node:fs/promises")
        const s = await stat(path)
        return { isDirectory: s.isDirectory() }
      },
      catch: (err) =>
        new FileSystemError({
          message: (err as Error).message,
          path,
          operation: "read",
        }),
    }),

  mkdir: (path: string, options?: { readonly recursive?: boolean }) =>
    Effect.tryPromise({
      try: async () => {
        const { mkdir } = await import("node:fs/promises")
        await mkdir(path, { recursive: options?.recursive })
      },
      catch: (err) =>
        new FileSystemError({
          message: (err as Error).message,
          path,
          operation: "write",
        }),
    }),
})

// ── ProcessRunner — Node.js implementation ──────────────────────────

export const ProcessRunnerLive = Layer.succeed(ProcessRunner, {
  exec: (
    command: string,
    args: readonly string[],
    options?: {
      readonly cwd?: string
      readonly env?: Record<string, string>
      readonly timeout?: number
    },
  ) =>
    Effect.tryPromise({
      try: async () => {
        const { execFile } = await import("node:child_process")
        const { promisify } = await import("node:util")
        const execFileAsync = promisify(execFile)
        const result = await execFileAsync(command, [...args], {
          cwd: options?.cwd,
          env: options?.env ? { ...process.env, ...options.env } : undefined,
          timeout: options?.timeout,
          maxBuffer: 10 * 1024 * 1024,
        })
        return {
          exitCode: 0 as number,
          stdout: result.stdout,
          stderr: result.stderr,
        }
      },
      catch: (err) => {
        const error = err as Error & {
          code?: number
          stdout?: string
          stderr?: string
        }
        return {
          exitCode: (error.code ?? 1) as number,
          stdout: error.stdout ?? "",
          stderr: error.stderr ?? error.message,
        }
      },
    }) as Effect.Effect<{
      exitCode: number
      stdout: string
      stderr: string
    }>,
})

// ── FrHttpClient — fetch-based implementation ───────────────────────

export const NodeHttpClientLive = Layer.succeed(FrHttpClient, {
  request: (
    url: string,
    options?: {
      readonly method?: string
      readonly headers?: Record<string, string>
      readonly body?: string
    },
  ) =>
    Effect.tryPromise({
      try: () =>
        fetch(url, {
          method: options?.method ?? "GET",
          headers: options?.headers,
          body: options?.body,
        }),
      catch: (err) => err as Error,
    }).pipe(
      Effect.map((res) => ({
        status: res.status,
        statusText: res.statusText,
        json: () =>
          Effect.tryPromise({
            try: () => res.json(),
            catch: (err) => err as Error,
          }) as Effect.Effect<unknown>,
        text: () =>
          Effect.tryPromise({
            try: () => res.text(),
            catch: (err) => err as Error,
          }) as Effect.Effect<string>,
      })),
    ) as Effect.Effect<{
      status: number
      statusText: string
      json: () => Effect.Effect<unknown>
      text: () => Effect.Effect<string>
    }>,
})

// ── ConfigProvider — uses discovery.ts functions ────────────────────

export const ConfigProviderLive = Layer.succeed(ConfigProvider, {
  loadConfig: (projectDir: string) =>
    Effect.tryPromise({
      try: async () => {
        const { loadConfig } = await import("../cli/discovery.js")
        return loadConfig(projectDir)
      },
      catch: (err) =>
        new DiscoveryError({
          reason: "config_not_found",
          message: (err as Error).message,
          path: projectDir,
        }),
    }),

  resolveConfig: (config: FlinkReactorConfig, envName?: string) =>
    Effect.try({
      try: () => resolveConfigFn(config, envName),
      catch: (err) => err as Error,
    }) as Effect.Effect<ResolvedConfig>,
})

// ── PipelineLoader — uses jiti ──────────────────────────────────────

export const PipelineLoaderLive = Layer.succeed(PipelineLoader, {
  loadPipeline: (entryPoint: string, projectDir: string) =>
    Effect.tryPromise({
      try: async () => {
        const { loadPipeline } = await import("../cli/discovery.js")
        return loadPipeline(entryPoint, projectDir)
      },
      catch: (err) =>
        new DiscoveryError({
          reason: "import_failure",
          message: (err as Error).message,
          path: entryPoint,
        }),
    }),
})

// ── CliOutput — console implementation ──────────────────────────────

export const CliOutputLive = Layer.succeed(CliOutput, {
  log: (_message: string) => Effect.sync(() => {}),
  warn: (_message: string) => Effect.sync(() => {}),
  error: (_message: string) => Effect.sync(() => {}),
  success: (_message: string) => Effect.sync(() => {}),
})

// ── Composed layers ─────────────────────────────────────────────────

/** Full production layer with all service implementations */
export const MainLive = Layer.mergeAll(
  NodeFileSystemLive,
  ProcessRunnerLive,
  NodeHttpClientLive,
  ConfigProviderLive,
  PipelineLoaderLive,
  CliOutputLive,
)

/**
 * Full application layer including ProcessEnv.
 * Use this for CLI entry points that need config resolution
 * and pipeline discovery via the Effect service graph.
 */
export const AppLayer = Layer.mergeAll(
  NodeFileSystemLive,
  ProcessEnvLive,
  ProcessRunnerLive,
  NodeHttpClientLive,
  ConfigProviderLive,
  PipelineLoaderLive,
  CliOutputLive,
)
