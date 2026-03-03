import type { SqlGatewayClient } from "./client.js"
import type { SessionConfig } from "./types.js"

export interface SessionPoolOptions {
  /** Maximum concurrent sessions (default: 5) */
  maxSessions?: number
  /** Idle timeout before closing unused sessions in ms (default: 300_000 = 5 min) */
  idleTimeoutMs?: number
}

interface PooledSession {
  handle: string
  state: "active" | "idle"
  idleTimer?: ReturnType<typeof setTimeout>
}

/**
 * Manages a pool of SQL Gateway sessions, reusing idle sessions
 * before creating new ones and closing sessions after an idle timeout.
 */
export class SessionPool {
  private readonly client: SqlGatewayClient
  private readonly maxSessions: number
  private readonly idleTimeoutMs: number
  private readonly sessions = new Map<string, PooledSession>()

  constructor(client: SqlGatewayClient, options?: SessionPoolOptions) {
    this.client = client
    this.maxSessions = options?.maxSessions ?? 5
    this.idleTimeoutMs = options?.idleTimeoutMs ?? 300_000
  }

  /** Acquire a session — reuses an idle session or creates a new one */
  async acquire(config?: SessionConfig): Promise<string> {
    // Try to reuse an idle session
    for (const [handle, session] of this.sessions) {
      if (session.state === "idle") {
        if (session.idleTimer) clearTimeout(session.idleTimer)
        session.state = "active"
        session.idleTimer = undefined
        return handle
      }
    }

    // Check if we can create a new session
    if (this.sessions.size >= this.maxSessions) {
      throw new Error(
        `Session pool exhausted: ${this.sessions.size}/${this.maxSessions} sessions in use`,
      )
    }

    // Create a new session
    const handle = await this.client.openSession(config)
    this.sessions.set(handle, { handle, state: "active" })
    return handle
  }

  /** Release a session back to the pool */
  release(sessionHandle: string): void {
    const session = this.sessions.get(sessionHandle)
    if (!session) return

    session.state = "idle"
    session.idleTimer = setTimeout(() => {
      this.closeAndRemove(sessionHandle)
    }, this.idleTimeoutMs)
  }

  /** Close all sessions and stop idle timers */
  async dispose(): Promise<void> {
    const handles = [...this.sessions.keys()]
    for (const session of this.sessions.values()) {
      if (session.idleTimer) clearTimeout(session.idleTimer)
    }

    await Promise.allSettled(
      handles.map((handle) => this.client.closeSession(handle)),
    )

    this.sessions.clear()
  }

  /** Number of active (in-use) sessions */
  get activeCount(): number {
    let count = 0
    for (const session of this.sessions.values()) {
      if (session.state === "active") count++
    }
    return count
  }

  /** Number of idle (available) sessions */
  get idleCount(): number {
    let count = 0
    for (const session of this.sessions.values()) {
      if (session.state === "idle") count++
    }
    return count
  }

  private async closeAndRemove(handle: string): Promise<void> {
    const session = this.sessions.get(handle)
    if (!session) return

    if (session.idleTimer) clearTimeout(session.idleTimer)
    this.sessions.delete(handle)

    try {
      await this.client.closeSession(handle)
    } catch {
      // Session may already be expired on the Gateway side
    }
  }
}
