import { Effect, Exit, Layer } from "effect"
import { join } from "node:path"
import { describe, expect, it } from "vitest"
import { discoverPipelinesEffect } from "../effect-discovery.js"
import { ConfigError, FileSystemError } from "../errors.js"
import { FrFileSystem } from "../services.js"

// ── Mock FileSystem layer ───────────────────────────────────────────

interface MockFsEntry {
  readonly isDirectory: boolean
  readonly content?: string
}

function mockFileSystem(
  files: Record<string, MockFsEntry>,
): Layer.Layer<FrFileSystem> {
  return Layer.succeed(FrFileSystem, {
    readFile: (path: string) => {
      const entry = files[path]
      if (!entry || entry.content === undefined) {
        return Effect.fail(
          new FileSystemError({ message: `ENOENT: ${path}`, path, operation: "read" }),
        )
      }
      return Effect.succeed(entry.content)
    },
    writeFile: (_path: string, _content: string) => Effect.void,
    exists: (path: string) => Effect.succeed(path in files),
    readdir: (path: string) => {
      // Collect direct children of the given directory
      const prefix = path.endsWith("/") ? path : `${path}/`
      const children = new Set<string>()
      for (const key of Object.keys(files)) {
        if (key.startsWith(prefix)) {
          const rest = key.slice(prefix.length)
          const firstSegment = rest.split("/")[0]
          if (firstSegment) children.add(firstSegment)
        }
      }
      return Effect.succeed([...children].sort())
    },
    stat: (path: string) => {
      const entry = files[path]
      if (!entry) {
        return Effect.fail(
          new FileSystemError({ message: `ENOENT: ${path}`, path, operation: "read" }),
        )
      }
      return Effect.succeed({ isDirectory: entry.isDirectory })
    },
    mkdir: (_path: string) => Effect.void,
  })
}

// ── Tests ───────────────────────────────────────────────────────────

describe("discoverPipelinesEffect()", () => {
  const projectDir = "/project"
  const pipelinesDir = join(projectDir, "pipelines")

  it("discovers pipelines with index.tsx entry points", async () => {
    const layer = mockFileSystem({
      [pipelinesDir]: { isDirectory: true },
      [join(pipelinesDir, "orders")]: { isDirectory: true },
      [join(pipelinesDir, "analytics")]: { isDirectory: true },
      [join(pipelinesDir, "orders", "index.tsx")]: {
        isDirectory: false,
        content: "export default null;",
      },
      [join(pipelinesDir, "analytics", "index.tsx")]: {
        isDirectory: false,
        content: "export default null;",
      },
    })

    const result = await Effect.runPromise(
      discoverPipelinesEffect(projectDir).pipe(Effect.provide(layer)),
    )

    expect(result).toHaveLength(2)
    expect(result[0].name).toBe("analytics")
    expect(result[1].name).toBe("orders")
    expect(result[0].entryPoint).toContain("analytics/index.tsx")
  })

  it("returns empty array when directory exists but has no valid pipelines", async () => {
    const layer = mockFileSystem({
      [pipelinesDir]: { isDirectory: true },
      [join(pipelinesDir, "README.md")]: { isDirectory: false, content: "# Pipelines" },
    })

    const result = await Effect.runPromise(
      discoverPipelinesEffect(projectDir).pipe(Effect.provide(layer)),
    )

    expect(result).toEqual([])
  })

  it("fails with ConfigError when pipelines directory is missing", async () => {
    const layer = mockFileSystem({})

    const error = await Effect.runPromise(
      discoverPipelinesEffect(projectDir).pipe(
        Effect.provide(layer),
        Effect.flip,
      ),
    )

    expect(error).toBeInstanceOf(ConfigError)
    expect((error as ConfigError).reason).toBe("missing_directory")
    expect((error as ConfigError).path).toContain("pipelines")
  })

  it("skips directories without index.tsx", async () => {
    const layer = mockFileSystem({
      [pipelinesDir]: { isDirectory: true },
      [join(pipelinesDir, "valid-pipeline")]: { isDirectory: true },
      [join(pipelinesDir, "valid-pipeline", "index.tsx")]: {
        isDirectory: false,
        content: "",
      },
      [join(pipelinesDir, "no-entry-point")]: { isDirectory: true },
    })

    const result = await Effect.runPromise(
      discoverPipelinesEffect(projectDir).pipe(Effect.provide(layer)),
    )

    expect(result).toHaveLength(1)
    expect(result[0].name).toBe("valid-pipeline")
  })

  it("filters by target pipeline name", async () => {
    const layer = mockFileSystem({
      [pipelinesDir]: { isDirectory: true },
      [join(pipelinesDir, "orders")]: { isDirectory: true },
      [join(pipelinesDir, "orders", "index.tsx")]: {
        isDirectory: false,
        content: "",
      },
      [join(pipelinesDir, "analytics")]: { isDirectory: true },
      [join(pipelinesDir, "analytics", "index.tsx")]: {
        isDirectory: false,
        content: "",
      },
    })

    const result = await Effect.runPromise(
      discoverPipelinesEffect(projectDir, "orders").pipe(Effect.provide(layer)),
    )

    expect(result).toHaveLength(1)
    expect(result[0].name).toBe("orders")
  })

  it("skips non-directory entries", async () => {
    const layer = mockFileSystem({
      [pipelinesDir]: { isDirectory: true },
      [join(pipelinesDir, "file.txt")]: { isDirectory: false, content: "" },
      [join(pipelinesDir, "real-pipeline")]: { isDirectory: true },
      [join(pipelinesDir, "real-pipeline", "index.tsx")]: {
        isDirectory: false,
        content: "",
      },
    })

    const result = await Effect.runPromise(
      discoverPipelinesEffect(projectDir).pipe(Effect.provide(layer)),
    )

    expect(result).toHaveLength(1)
    expect(result[0].name).toBe("real-pipeline")
  })
})
