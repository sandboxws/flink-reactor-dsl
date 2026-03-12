import { Effect } from "effect"
import { describe, expect, it } from "vitest"
import { AppLayer, MainLive } from "../layers.js"
import {
  CliOutput,
  ConfigProvider,
  FrFileSystem,
  FrHttpClient,
  PipelineLoader,
  ProcessEnv,
  ProcessRunner,
} from "../services.js"

describe("Layer composition", () => {
  it("MainLive provides all original services", async () => {
    const program = Effect.gen(function* () {
      const fs = yield* FrFileSystem
      const runner = yield* ProcessRunner
      const http = yield* FrHttpClient
      const config = yield* ConfigProvider
      const loader = yield* PipelineLoader
      const output = yield* CliOutput

      // Verify all services are available by checking method presence
      expect(typeof fs.readFile).toBe("function")
      expect(typeof fs.writeFile).toBe("function")
      expect(typeof fs.exists).toBe("function")
      expect(typeof fs.readdir).toBe("function")
      expect(typeof fs.stat).toBe("function")
      expect(typeof fs.mkdir).toBe("function")

      expect(typeof runner.exec).toBe("function")
      expect(typeof http.request).toBe("function")
      expect(typeof config.loadConfig).toBe("function")
      expect(typeof config.resolveConfig).toBe("function")
      expect(typeof loader.loadPipeline).toBe("function")
      expect(typeof output.log).toBe("function")
      expect(typeof output.warn).toBe("function")
      expect(typeof output.error).toBe("function")
      expect(typeof output.success).toBe("function")
    })

    await Effect.runPromise(program.pipe(Effect.provide(MainLive)))
  })

  it("AppLayer provides all services including ProcessEnv", async () => {
    const program = Effect.gen(function* () {
      const fs = yield* FrFileSystem
      const env = yield* ProcessEnv
      const runner = yield* ProcessRunner
      const http = yield* FrHttpClient
      const config = yield* ConfigProvider
      const loader = yield* PipelineLoader
      const output = yield* CliOutput

      expect(typeof fs.readFile).toBe("function")
      expect(typeof env.get).toBe("function")
      expect(typeof runner.exec).toBe("function")
      expect(typeof http.request).toBe("function")
      expect(typeof config.loadConfig).toBe("function")
      expect(typeof loader.loadPipeline).toBe("function")
      expect(typeof output.log).toBe("function")
    })

    await Effect.runPromise(program.pipe(Effect.provide(AppLayer)))
  })

  it("ProcessEnvLive reads from actual process.env", async () => {
    const testKey = "__FR_LAYER_TEST_VAR__"
    process.env[testKey] = "test-value"

    try {
      const program = Effect.gen(function* () {
        const env = yield* ProcessEnv
        const value = yield* env.get(testKey)
        expect(value).toBe("test-value")

        const missing = yield* env.get("__FR_NONEXISTENT__")
        expect(missing).toBeUndefined()
      })

      await Effect.runPromise(program.pipe(Effect.provide(AppLayer)))
    } finally {
      delete process.env[testKey]
    }
  })
})
