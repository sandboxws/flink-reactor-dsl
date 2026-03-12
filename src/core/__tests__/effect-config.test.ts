import { Effect, Exit, Layer } from "effect"
import { describe, expect, it } from "vitest"
import { defineConfig } from "../config.js"
import { resolveConfigEffect, resolveEnvVarEffect } from "../effect-config.js"
import { ConfigError } from "../errors.js"
import { ProcessEnv } from "../services.js"

// ── Mock ProcessEnv layer ───────────────────────────────────────────

function mockProcessEnv(
  vars: Record<string, string | undefined>,
): Layer.Layer<ProcessEnv> {
  return Layer.succeed(ProcessEnv, {
    get: (name: string) => Effect.succeed(vars[name]),
  })
}

// ── resolveEnvVarEffect tests ───────────────────────────────────────

describe("resolveEnvVarEffect()", () => {
  it("returns the variable value when present", async () => {
    const layer = mockProcessEnv({ MY_VAR: "hello" })
    const result = await Effect.runPromise(
      resolveEnvVarEffect("MY_VAR").pipe(Effect.provide(layer)),
    )
    expect(result).toBe("hello")
  })

  it("returns fallback when variable is undefined", async () => {
    const layer = mockProcessEnv({})
    const result = await Effect.runPromise(
      resolveEnvVarEffect("MISSING", { fallback: "default" }).pipe(
        Effect.provide(layer),
      ),
    )
    expect(result).toBe("default")
  })

  it("fails with ConfigError when variable is missing and no fallback", async () => {
    const layer = mockProcessEnv({})
    const exit = await Effect.runPromiseExit(
      resolveEnvVarEffect("MISSING").pipe(Effect.provide(layer)),
    )
    expect(Exit.isFailure(exit)).toBe(true)
    if (Exit.isFailure(exit)) {
      const error = (exit as any).cause?.error ?? exit.cause
      // Extract the ConfigError from the cause
      const result = await Effect.runPromise(
        resolveEnvVarEffect("MISSING").pipe(
          Effect.provide(layer),
          Effect.flip,
        ),
      )
      expect(result).toBeInstanceOf(ConfigError)
      expect(result.reason).toBe("missing_env_var")
      expect(result.varName).toBe("MISSING")
    }
  })

  it("fails when variable is empty and allowEmpty is false", async () => {
    const layer = mockProcessEnv({ EMPTY_VAR: "" })
    const error = await Effect.runPromise(
      resolveEnvVarEffect("EMPTY_VAR", { allowEmpty: false }).pipe(
        Effect.provide(layer),
        Effect.flip,
      ),
    )
    expect(error).toBeInstanceOf(ConfigError)
    expect(error.reason).toBe("missing_env_var")
    expect(error.message).toContain("empty")
  })

  it("returns empty string when allowEmpty is not false", async () => {
    const layer = mockProcessEnv({ EMPTY_VAR: "" })
    const result = await Effect.runPromise(
      resolveEnvVarEffect("EMPTY_VAR").pipe(Effect.provide(layer)),
    )
    expect(result).toBe("")
  })
})

// ── resolveConfigEffect tests ───────────────────────────────────────

describe("resolveConfigEffect()", () => {
  it("resolves a valid config with env vars", async () => {
    const layer = mockProcessEnv({
      FLINK_REST_URL: "https://prod.example.com:8081",
    })

    const config = defineConfig({
      flink: { version: "2.0" },
      environments: {
        production: {
          cluster: {
            url: (await import("../env-var.js")).env("FLINK_REST_URL"),
          },
        },
      },
    })

    const resolved = await Effect.runPromise(
      resolveConfigEffect(config, "production").pipe(Effect.provide(layer)),
    )

    expect(resolved.cluster.url).toBe("https://prod.example.com:8081")
    expect(resolved.flink.version).toBe("2.0")
    expect(resolved.environmentName).toBe("production")
  })

  it("fails with ConfigError on missing config file env var", async () => {
    const layer = mockProcessEnv({})

    const config = defineConfig({
      environments: {
        production: {
          cluster: {
            url: (await import("../env-var.js")).env("FLINK_REST_URL"),
          },
        },
      },
    })

    const error = await Effect.runPromise(
      resolveConfigEffect(config, "production").pipe(
        Effect.provide(layer),
        Effect.flip,
      ),
    )

    expect(error).toBeInstanceOf(ConfigError)
    expect(error.reason).toBe("missing_env_var")
    expect(error.varName).toBe("FLINK_REST_URL")
  })

  it("fails with ConfigError on unknown environment", async () => {
    const layer = mockProcessEnv({})

    const config = defineConfig({
      environments: {
        development: { cluster: { url: "http://localhost:8081" } },
      },
    })

    const error = await Effect.runPromise(
      resolveConfigEffect(config, "staging").pipe(
        Effect.provide(layer),
        Effect.flip,
      ),
    )

    expect(error).toBeInstanceOf(ConfigError)
    expect(error.reason).toBe("unknown_environment")
    expect(error.environmentName).toBe("staging")
  })

  it("applies defaults when no environment is specified", async () => {
    const layer = mockProcessEnv({})

    const config = defineConfig({
      flink: { version: "2.0" },
    })

    const resolved = await Effect.runPromise(
      resolveConfigEffect(config).pipe(Effect.provide(layer)),
    )

    expect(resolved.flink.version).toBe("2.0")
    expect(resolved.kubernetes.namespace).toBe("default")
    expect(resolved.pipelines).toEqual({})
  })
})
