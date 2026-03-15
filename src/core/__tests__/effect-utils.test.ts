import { Effect } from "effect"
import { describe, expect, it } from "vitest"
import {
  fromThrowable,
  fromThrowableAsync,
  runPromise,
  runSync,
  runWithCause,
  toValidationEffect,
} from "../effect-utils.js"
import { ValidationError } from "../errors.js"
import type { ValidationDiagnostic } from "../synth-context.js"

describe("runSync", () => {
  it("returns the success value", () => {
    const result = runSync(Effect.succeed(42))
    expect(result).toBe(42)
  })

  it("throws on failure", () => {
    expect(() => runSync(Effect.fail("boom"))).toThrow()
  })
})

describe("runPromise", () => {
  it("resolves with the success value", async () => {
    const result = await runPromise(Effect.succeed("hello"))
    expect(result).toBe("hello")
  })

  it("rejects on failure", async () => {
    await expect(runPromise(Effect.fail("boom"))).rejects.toBeDefined()
  })
})

describe("fromThrowable", () => {
  it("wraps a successful function into an Effect", () => {
    const effect = fromThrowable(() => JSON.parse('{"a":1}'))
    const result = runSync(effect)
    expect(result).toEqual({ a: 1 })
  })

  it("captures thrown errors as Effect failures", async () => {
    const effect = fromThrowable(() => JSON.parse("invalid json"))
    await expect(runPromise(effect)).rejects.toBeInstanceOf(Error)
  })
})

describe("fromThrowableAsync", () => {
  it("wraps a successful async function into an Effect", async () => {
    const effect = fromThrowableAsync(() => Promise.resolve("async-value"))
    const result = await runPromise(effect)
    expect(result).toBe("async-value")
  })

  it("captures rejected promises as Effect failures", async () => {
    const effect = fromThrowableAsync(() =>
      Promise.reject(new Error("async fail")),
    )
    await expect(runPromise(effect)).rejects.toBeInstanceOf(Error)
  })
})

describe("toValidationEffect", () => {
  it("succeeds with warnings when no errors are present", () => {
    const diagnostics: ValidationDiagnostic[] = [
      {
        severity: "warning",
        message: "unused field",
        nodeId: "n1",
        ruleId: "r1",
      },
    ]
    const result = runSync(toValidationEffect(diagnostics))
    expect(result).toHaveLength(1)
    expect(result[0].severity).toBe("warning")
  })

  it("succeeds with empty array when no diagnostics", () => {
    const result = runSync(toValidationEffect([]))
    expect(result).toEqual([])
  })

  it("fails with ValidationError when errors are present", async () => {
    const diagnostics: ValidationDiagnostic[] = [
      {
        severity: "error",
        message: "type mismatch",
        nodeId: "n1",
        ruleId: "r1",
      },
      { severity: "warning", message: "unused", nodeId: "n2", ruleId: "r2" },
    ]
    const effect = toValidationEffect(diagnostics)
    const result = await Effect.runPromise(
      effect.pipe(
        Effect.catchTag("ValidationError", (err) => Effect.succeed(err)),
      ),
    )
    expect(result).toBeInstanceOf(ValidationError)
    if (result instanceof ValidationError) {
      expect(result.diagnostics).toHaveLength(1)
      expect(result.diagnostics[0].severity).toBe("error")
    }
  })
})

describe("runWithCause", () => {
  it("returns success result for successful effects", async () => {
    const result = await runWithCause(Effect.succeed(42))
    expect(result._tag).toBe("success")
    if (result._tag === "success") {
      expect(result.value).toBe(42)
    }
  })

  it("returns failure result with cause for failed effects", async () => {
    const result = await runWithCause(Effect.fail("boom"))
    expect(result._tag).toBe("failure")
    if (result._tag === "failure") {
      expect(result.cause).toBeDefined()
    }
  })
})
