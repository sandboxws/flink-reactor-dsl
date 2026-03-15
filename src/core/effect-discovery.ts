// ── Effect-wrapped pipeline discovery ────────────────────────────────
// Pure Effect version of discoverPipelines that uses the FrFileSystem
// service instead of direct fs calls, enabling mock-based testing.

import { join } from "node:path"
import { Effect } from "effect"
import type { DiscoveredPipeline } from "../cli/discovery.js"
import { ConfigError, type FileSystemError } from "./errors.js"
import { FrFileSystem } from "./services.js"

/**
 * Discover pipelines by walking the pipelines/ directory using the
 * FrFileSystem service. Unlike the imperative `discoverPipelines()`,
 * this fails with `ConfigError` when the pipelines directory is missing.
 *
 * Each subdirectory containing an index.tsx is a pipeline.
 */
export function discoverPipelinesEffect(
  projectDir: string,
  targetPipeline?: string,
): Effect.Effect<
  DiscoveredPipeline[],
  ConfigError | FileSystemError,
  FrFileSystem
> {
  const pipelinesDir = join(projectDir, "pipelines")

  return Effect.gen(function* () {
    const fs = yield* FrFileSystem

    const dirExists = yield* fs.exists(pipelinesDir)
    if (!dirExists) {
      return yield* Effect.fail(
        new ConfigError({
          reason: "missing_directory",
          message: `Pipelines directory not found: ${pipelinesDir}`,
          path: pipelinesDir,
        }),
      )
    }

    const entries = yield* fs.readdir(pipelinesDir)
    const pipelines: DiscoveredPipeline[] = []

    for (const entry of entries) {
      const entryPath = join(pipelinesDir, entry)
      const stat = yield* fs.stat(entryPath)
      if (!stat.isDirectory) continue

      const indexPath = join(entryPath, "index.tsx")
      const indexExists = yield* fs.exists(indexPath)
      if (!indexExists) continue

      if (targetPipeline && entry !== targetPipeline) continue

      pipelines.push({ name: entry, entryPoint: indexPath })
    }

    return pipelines.sort((a, b) => a.name.localeCompare(b.name))
  })
}
