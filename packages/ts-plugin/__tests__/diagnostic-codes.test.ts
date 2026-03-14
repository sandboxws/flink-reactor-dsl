import { describe, expect, it } from "vitest"
import {
  DIAGNOSTIC_SOURCE,
  DiagnosticCodes,
  invalidNestingMessage,
} from "../src/diagnostic-codes"

describe("diagnostic conventions", () => {
  it("DIAGNOSTIC_SOURCE is flink-reactor", () => {
    expect(DIAGNOSTIC_SOURCE).toBe("flink-reactor")
  })

  it("INVALID_NESTING code is 90100", () => {
    expect(DiagnosticCodes.INVALID_NESTING).toBe(90100)
  })

  it("all diagnostic codes are in the 90xxx range", () => {
    for (const [name, code] of Object.entries(DiagnosticCodes)) {
      expect(code, `${name} should be in 90xxx range`).toBeGreaterThanOrEqual(
        90000,
      )
      expect(code, `${name} should be in 90xxx range`).toBeLessThan(100000)
    }
  })

  it("all diagnostic codes are unique", () => {
    const codes = Object.values(DiagnosticCodes)
    const unique = new Set(codes)
    expect(unique.size).toBe(codes.length)
  })

  it("invalidNestingMessage follows the convention", () => {
    const msg = invalidNestingMessage("Filter", "Route", [
      "Route.Branch",
      "Route.Default",
    ])
    expect(msg).toBe(
      "'Filter' is not a valid child of 'Route'. Expected: Route.Branch, Route.Default",
    )
  })
})
