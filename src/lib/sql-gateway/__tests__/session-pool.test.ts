import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import type { SqlGatewayClient } from "../client.js"
import { SessionPool } from "../session-pool.js"

// ── Mock client ─────────────────────────────────────────────────────────────

function createMockClient(): SqlGatewayClient {
  let counter = 0
  return {
    openSession: vi.fn(async () => `session-${++counter}`),
    closeSession: vi.fn(async () => {}),
  } as unknown as SqlGatewayClient
}

describe("SessionPool", () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  describe("acquire", () => {
    it("reuses idle sessions before creating new ones", async () => {
      const client = createMockClient()
      const pool = new SessionPool(client, { maxSessions: 5 })

      // Acquire a session
      const handle1 = await pool.acquire()
      expect(handle1).toBe("session-1")
      expect(client.openSession).toHaveBeenCalledTimes(1)

      // Release it
      pool.release(handle1)
      expect(pool.idleCount).toBe(1)

      // Acquire again — should reuse
      const handle2 = await pool.acquire()
      expect(handle2).toBe("session-1")
      expect(client.openSession).toHaveBeenCalledTimes(1) // Still 1, no new session
      expect(pool.idleCount).toBe(0)
      expect(pool.activeCount).toBe(1)

      await pool.dispose()
    })

    it("throws when max sessions exceeded", async () => {
      const client = createMockClient()
      const pool = new SessionPool(client, { maxSessions: 2 })

      await pool.acquire()
      await pool.acquire()

      await expect(pool.acquire()).rejects.toThrow(/Session pool exhausted/)

      await pool.dispose()
    })

    it("creates new session when no idle sessions available", async () => {
      const client = createMockClient()
      const pool = new SessionPool(client, { maxSessions: 3 })

      const h1 = await pool.acquire()
      const h2 = await pool.acquire()

      expect(h1).toBe("session-1")
      expect(h2).toBe("session-2")
      expect(client.openSession).toHaveBeenCalledTimes(2)
      expect(pool.activeCount).toBe(2)

      await pool.dispose()
    })
  })

  describe("idle timeout", () => {
    it("closes sessions after configured idle delay", async () => {
      const client = createMockClient()
      const pool = new SessionPool(client, {
        maxSessions: 5,
        idleTimeoutMs: 10_000,
      })

      const handle = await pool.acquire()
      pool.release(handle)

      expect(pool.idleCount).toBe(1)

      // Advance time past idle timeout
      await vi.advanceTimersByTimeAsync(10_001)

      // Session should have been closed
      expect(client.closeSession).toHaveBeenCalledWith("session-1")
      expect(pool.idleCount).toBe(0)

      await pool.dispose()
    })
  })

  describe("dispose", () => {
    it("closes all sessions (active and idle)", async () => {
      const client = createMockClient()
      const pool = new SessionPool(client, { maxSessions: 5 })

      const h1 = await pool.acquire()
      const _h2 = await pool.acquire()
      pool.release(h1) // h1 is idle

      expect(pool.activeCount).toBe(1)
      expect(pool.idleCount).toBe(1)

      await pool.dispose()

      expect(client.closeSession).toHaveBeenCalledWith("session-1")
      expect(client.closeSession).toHaveBeenCalledWith("session-2")
      expect(pool.activeCount).toBe(0)
      expect(pool.idleCount).toBe(0)
    })
  })

  describe("activeCount and idleCount", () => {
    it("tracks counts accurately", async () => {
      const client = createMockClient()
      const pool = new SessionPool(client, { maxSessions: 5 })

      expect(pool.activeCount).toBe(0)
      expect(pool.idleCount).toBe(0)

      const h1 = await pool.acquire()
      expect(pool.activeCount).toBe(1)
      expect(pool.idleCount).toBe(0)

      const h2 = await pool.acquire()
      expect(pool.activeCount).toBe(2)
      expect(pool.idleCount).toBe(0)

      pool.release(h1)
      expect(pool.activeCount).toBe(1)
      expect(pool.idleCount).toBe(1)

      pool.release(h2)
      expect(pool.activeCount).toBe(0)
      expect(pool.idleCount).toBe(2)

      await pool.dispose()
    })
  })
})
