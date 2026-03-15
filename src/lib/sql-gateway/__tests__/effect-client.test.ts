import { Effect, Exit } from "effect"
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import * as EffectClient from "../effect-client.js"
import type {
  RawFetchResultsResponse,
  RawOpenSessionResponse,
} from "../types.js"

// ── Mock fetch ──────────────────────────────────────────────────────────────

let mockFetch: ReturnType<typeof vi.fn>

beforeEach(() => {
  mockFetch = vi.fn()
  vi.stubGlobal("fetch", mockFetch)
})

afterEach(() => {
  vi.restoreAllMocks()
})

function mockJsonResponse<T>(data: T, status = 200): Response {
  return {
    ok: status >= 200 && status < 300,
    status,
    statusText: status === 200 ? "OK" : "Error",
    json: () => Promise.resolve(data),
    text: () => Promise.resolve(JSON.stringify(data)),
    headers: new Headers({ "Content-Type": "application/json" }),
  } as unknown as Response
}

// ── Tests ───────────────────────────────────────────────────────────────────

const BASE_URL = "http://localhost:8083"

describe("Effect-based SQL Gateway Client — typed errors", () => {
  describe("connection refused", () => {
    it("returns SqlGatewayConnectionError when fetch rejects", async () => {
      mockFetch.mockRejectedValue(new Error("connect ECONNREFUSED"))

      const exit = await Effect.runPromiseExit(
        EffectClient.openSession(BASE_URL).pipe(
          // Disable retry for this test
          Effect.catchAll((e) => Effect.fail(e)),
        ),
      )

      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) {
        const error = exit.cause
        // The cause should contain a SqlGatewayConnectionError
        const flat = JSON.stringify(error)
        expect(flat).toContain("SqlGatewayConnectionError")
        expect(flat).toContain("ECONNREFUSED")
      }
    })
  })

  describe("401 unauthorized", () => {
    it("returns SqlGatewayResponseError with status 401", async () => {
      mockFetch.mockResolvedValue(
        mockJsonResponse({ errors: ["Authentication required"] }, 401),
      )

      const exit = await Effect.runPromiseExit(
        EffectClient.openSession(BASE_URL),
      )

      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) {
        const flat = JSON.stringify(exit.cause)
        expect(flat).toContain("SqlGatewayResponseError")
        expect(flat).toContain("Authentication required")
        expect(flat).toContain("401")
      }
    })
  })

  describe("session expired (404)", () => {
    it("returns SqlGatewayResponseError when session handle is invalid", async () => {
      mockFetch.mockResolvedValue(
        mockJsonResponse(
          { errors: ["Session 'expired-handle' does not exist"] },
          404,
        ),
      )

      const exit = await Effect.runPromiseExit(
        EffectClient.submitStatement(BASE_URL, "expired-handle", "SELECT 1"),
      )

      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) {
        const flat = JSON.stringify(exit.cause)
        expect(flat).toContain("SqlGatewayResponseError")
        expect(flat).toContain("does not exist")
        expect(flat).toContain("404")
      }
    })
  })

  describe("statement syntax error (400)", () => {
    it("returns SqlGatewayResponseError with Flink parser message", async () => {
      mockFetch.mockResolvedValue(
        mockJsonResponse(
          {
            errors: [
              "Encountered 'SELCT' at line 1, column 0. Did you mean 'SELECT'?",
            ],
          },
          400,
        ),
      )

      const exit = await Effect.runPromiseExit(
        EffectClient.submitStatement(BASE_URL, "sess-123", "SELCT 1"),
      )

      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) {
        const flat = JSON.stringify(exit.cause)
        expect(flat).toContain("SqlGatewayResponseError")
        expect(flat).toContain("SELCT")
        expect(flat).toContain("400")
      }
    })
  })

  describe("500 server error with retry", () => {
    it("retries transient 500 errors and succeeds when server recovers", async () => {
      const errorResponse = mockJsonResponse(
        { errors: ["Internal server error"] },
        500,
      )
      const successResponse = mockJsonResponse<RawOpenSessionResponse>({
        sessionHandle: "sess-recovered",
      })

      mockFetch
        .mockResolvedValueOnce(errorResponse)
        .mockResolvedValueOnce(errorResponse)
        .mockResolvedValueOnce(successResponse)

      const result = await Effect.runPromise(EffectClient.openSession(BASE_URL))

      expect(result).toBe("sess-recovered")
      expect(mockFetch).toHaveBeenCalledTimes(3)
    })
  })

  describe("non-JSON error response", () => {
    it("falls back to status text when body is not JSON", async () => {
      const res = {
        ok: false,
        status: 502,
        statusText: "Bad Gateway",
        json: () => Promise.reject(new Error("not json")),
        headers: new Headers(),
      } as unknown as Response
      mockFetch.mockResolvedValue(res)

      const exit = await Effect.runPromiseExit(
        EffectClient.openSession(BASE_URL),
      )

      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) {
        const flat = JSON.stringify(exit.cause)
        expect(flat).toContain("502")
      }
    })
  })

  describe("JSON parse error on success response", () => {
    it("returns SqlGatewayResponseError when response body is malformed", async () => {
      const res = {
        ok: true,
        status: 200,
        statusText: "OK",
        json: () => Promise.reject(new Error("Unexpected token")),
        headers: new Headers(),
      } as unknown as Response
      mockFetch.mockResolvedValue(res)

      const exit = await Effect.runPromiseExit(
        EffectClient.openSession(BASE_URL),
      )

      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) {
        const flat = JSON.stringify(exit.cause)
        expect(flat).toContain("Failed to parse response")
      }
    })
  })

  describe("successful operations", () => {
    it("openSession returns session handle", async () => {
      mockFetch.mockResolvedValue(
        mockJsonResponse<RawOpenSessionResponse>({ sessionHandle: "sess-ok" }),
      )

      const result = await Effect.runPromise(EffectClient.openSession(BASE_URL))
      expect(result).toBe("sess-ok")
    })

    it("fetchResults normalizes result page", async () => {
      const raw: RawFetchResultsResponse = {
        results: {
          columns: [
            { name: "id", logicalType: { type: "BIGINT", nullable: false } },
          ],
          data: [{ kind: "INSERT", fields: [42] }],
        },
        resultType: "PAYLOAD",
        nextResultUri: "/v1/sessions/s/operations/o/result/1",
      }
      mockFetch.mockResolvedValue(mockJsonResponse(raw))

      const page = await Effect.runPromise(
        EffectClient.fetchResults(BASE_URL, "s", "o", 0),
      )

      expect(page.columns).toEqual([
        { columnName: "id", dataType: "BIGINT", nullable: false },
      ])
      expect(page.rows).toEqual([{ id: 42 }])
      expect(page.isEndOfStream).toBe(false)
    })
  })
})
