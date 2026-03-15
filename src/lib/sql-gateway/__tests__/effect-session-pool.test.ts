import { Effect } from "effect"
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { makeSessionPool } from "../effect-session-pool.js"
import type { RawOpenSessionResponse } from "../types.js"

// ── Mock fetch ──────────────────────────────────────────────────────────────

let mockFetch: ReturnType<typeof vi.fn>
let sessionCounter: number

beforeEach(() => {
  sessionCounter = 0
  mockFetch = vi.fn(async (url: string, init?: RequestInit) => {
    const method = init?.method ?? "GET"

    // POST /v1/sessions → open session
    if (method === "POST" && url.endsWith("/v1/sessions")) {
      sessionCounter++
      return {
        ok: true,
        status: 200,
        statusText: "OK",
        json: () =>
          Promise.resolve({
            sessionHandle: `session-${sessionCounter}`,
          } satisfies RawOpenSessionResponse),
        headers: new Headers(),
      } as Response
    }

    // DELETE /v1/sessions/{handle} → close session
    if (method === "DELETE" && url.includes("/v1/sessions/")) {
      return {
        ok: true,
        status: 204,
        statusText: "No Content",
        json: () => Promise.resolve(undefined),
        headers: new Headers(),
      } as Response
    }

    // GET /v1/sessions/{handle} → heartbeat/touch
    if (method === "GET" && url.includes("/v1/sessions/")) {
      return {
        ok: true,
        status: 200,
        statusText: "OK",
        json: () => Promise.resolve({ properties: {} }),
        headers: new Headers(),
      } as Response
    }

    return {
      ok: false,
      status: 404,
      statusText: "Not Found",
      json: () => Promise.resolve({ errors: ["Not found"] }),
      headers: new Headers(),
    } as unknown as Response
  })
  vi.stubGlobal("fetch", mockFetch)
})

afterEach(() => {
  vi.restoreAllMocks()
})

// ── Tests ───────────────────────────────────────────────────────────────────

const BASE_URL = "http://localhost:8083"

describe("Effect-based SessionPool", () => {
  describe("acquire", () => {
    it("creates a new session when pool is empty", async () => {
      const pool = await Effect.runPromise(
        makeSessionPool({
          baseUrl: BASE_URL,
          maxSessions: 5,
          heartbeatIntervalMs: 0,
        }),
      )

      const handle = await Effect.runPromise(pool.acquire())
      expect(handle).toBe("session-1")

      const active = await Effect.runPromise(pool.activeCount)
      expect(active).toBe(1)

      await Effect.runPromise(pool.dispose())
    })

    it("reuses idle sessions before creating new ones", async () => {
      const pool = await Effect.runPromise(
        makeSessionPool({
          baseUrl: BASE_URL,
          maxSessions: 5,
          heartbeatIntervalMs: 0,
        }),
      )

      // Acquire and release
      const h1 = await Effect.runPromise(pool.acquire())
      await Effect.runPromise(pool.release(h1))

      // Acquire again — should reuse
      const h2 = await Effect.runPromise(pool.acquire())
      expect(h2).toBe(h1)

      // openSession should have been called only once
      const openCalls = mockFetch.mock.calls.filter(
        ([, init]: [string, RequestInit | undefined]) =>
          init?.method === "POST",
      )
      expect(openCalls).toHaveLength(1)

      await Effect.runPromise(pool.dispose())
    })
  })

  describe("max connections", () => {
    it("fails with PoolExhaustedError when max sessions exceeded", async () => {
      const pool = await Effect.runPromise(
        makeSessionPool({
          baseUrl: BASE_URL,
          maxSessions: 2,
          heartbeatIntervalMs: 0,
        }),
      )

      await Effect.runPromise(pool.acquire())
      await Effect.runPromise(pool.acquire())

      const exit = await Effect.runPromiseExit(pool.acquire())
      expect(exit._tag).toBe("Failure")

      const flat = JSON.stringify(exit)
      expect(flat).toContain("PoolExhaustedError")

      await Effect.runPromise(pool.dispose())
    })

    it("allows acquiring after releasing when at max capacity", async () => {
      const pool = await Effect.runPromise(
        makeSessionPool({
          baseUrl: BASE_URL,
          maxSessions: 2,
          heartbeatIntervalMs: 0,
        }),
      )

      const h1 = await Effect.runPromise(pool.acquire())
      await Effect.runPromise(pool.acquire())

      // Release one
      await Effect.runPromise(pool.release(h1))

      // Should succeed now
      const h3 = await Effect.runPromise(pool.acquire())
      expect(h3).toBe(h1) // Reused

      await Effect.runPromise(pool.dispose())
    })
  })

  describe("release", () => {
    it("transitions session from active to idle", async () => {
      const pool = await Effect.runPromise(
        makeSessionPool({
          baseUrl: BASE_URL,
          maxSessions: 5,
          heartbeatIntervalMs: 0,
        }),
      )

      const handle = await Effect.runPromise(pool.acquire())
      expect(await Effect.runPromise(pool.activeCount)).toBe(1)
      expect(await Effect.runPromise(pool.idleCount)).toBe(0)

      await Effect.runPromise(pool.release(handle))
      expect(await Effect.runPromise(pool.activeCount)).toBe(0)
      expect(await Effect.runPromise(pool.idleCount)).toBe(1)

      await Effect.runPromise(pool.dispose())
    })
  })

  describe("dispose", () => {
    it("closes all sessions (active and idle)", async () => {
      const pool = await Effect.runPromise(
        makeSessionPool({
          baseUrl: BASE_URL,
          maxSessions: 5,
          heartbeatIntervalMs: 0,
        }),
      )

      const h1 = await Effect.runPromise(pool.acquire())
      await Effect.runPromise(pool.acquire())
      await Effect.runPromise(pool.release(h1))

      await Effect.runPromise(pool.dispose())

      // closeSession should have been called for both
      const deleteCalls = mockFetch.mock.calls.filter(
        ([, init]: [string, RequestInit | undefined]) =>
          init?.method === "DELETE",
      )
      expect(deleteCalls).toHaveLength(2)

      expect(await Effect.runPromise(pool.activeCount)).toBe(0)
      expect(await Effect.runPromise(pool.idleCount)).toBe(0)
    })
  })

  describe("counts", () => {
    it("tracks active and idle counts accurately", async () => {
      const pool = await Effect.runPromise(
        makeSessionPool({
          baseUrl: BASE_URL,
          maxSessions: 5,
          heartbeatIntervalMs: 0,
        }),
      )

      expect(await Effect.runPromise(pool.activeCount)).toBe(0)
      expect(await Effect.runPromise(pool.idleCount)).toBe(0)

      const h1 = await Effect.runPromise(pool.acquire())
      const h2 = await Effect.runPromise(pool.acquire())
      expect(await Effect.runPromise(pool.activeCount)).toBe(2)
      expect(await Effect.runPromise(pool.idleCount)).toBe(0)

      await Effect.runPromise(pool.release(h1))
      expect(await Effect.runPromise(pool.activeCount)).toBe(1)
      expect(await Effect.runPromise(pool.idleCount)).toBe(1)

      await Effect.runPromise(pool.release(h2))
      expect(await Effect.runPromise(pool.activeCount)).toBe(0)
      expect(await Effect.runPromise(pool.idleCount)).toBe(2)

      await Effect.runPromise(pool.dispose())
    })
  })
})
