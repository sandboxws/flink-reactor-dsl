// ── Effect-based session pool ───────────────────────────────────────
// Manages a pool of SQL Gateway sessions using Effect for lifecycle.
// Uses Effect.acquireRelease for guaranteed cleanup and Ref for
// thread-safe state management.

import { Duration, Effect, Fiber, Ref, Schedule } from "effect"
import type { SqlGatewayError } from "../../core/errors.js"
import * as EffectClient from "./effect-client.js"
import type { SessionConfig } from "./types.js"

interface PooledSession {
  readonly handle: string
  readonly state: "active" | "idle"
}

export interface SessionPoolConfig {
  readonly baseUrl: string
  readonly maxSessions?: number
  readonly idleTimeoutMs?: number
  /** Heartbeat interval in ms. Set to 0 to disable. Default: 60_000 (1 min) */
  readonly heartbeatIntervalMs?: number
}

export interface EffectSessionPool {
  /** Acquire a session — reuses idle or creates new */
  readonly acquire: (
    config?: SessionConfig,
  ) => Effect.Effect<string, SqlGatewayError | PoolExhaustedError>
  /** Release a session back to the pool */
  readonly release: (handle: string) => Effect.Effect<void>
  /** Close all sessions */
  readonly dispose: () => Effect.Effect<void>
  /** Number of active sessions */
  readonly activeCount: Effect.Effect<number>
  /** Number of idle sessions */
  readonly idleCount: Effect.Effect<number>
}

export class PoolExhaustedError {
  readonly _tag = "PoolExhaustedError" as const
  constructor(
    readonly current: number,
    readonly max: number,
  ) {}

  get message(): string {
    return `Session pool exhausted: ${this.current}/${this.max} sessions in use`
  }
}

/**
 * Create a managed session pool with optional heartbeat.
 * The heartbeat runs as a forked fiber that periodically pings
 * idle sessions to keep them alive on the server.
 */
export function makeSessionPool(
  config: SessionPoolConfig,
): Effect.Effect<EffectSessionPool> {
  const maxSessions = config.maxSessions ?? 5
  const baseUrl = config.baseUrl.replace(/\/+$/, "")
  const heartbeatIntervalMs = config.heartbeatIntervalMs ?? 60_000

  return Effect.gen(function* () {
    const sessionsRef = yield* Ref.make(new Map<string, PooledSession>())

    const acquire = (
      sessionConfig?: SessionConfig,
    ): Effect.Effect<string, SqlGatewayError | PoolExhaustedError> =>
      Ref.get(sessionsRef).pipe(
        Effect.flatMap(
          (
            sessions,
          ): Effect.Effect<string, SqlGatewayError | PoolExhaustedError> => {
            // Try to reuse an idle session
            for (const [handle, session] of sessions) {
              if (session.state === "idle") {
                return Ref.update(sessionsRef, (s) => {
                  const next = new Map(s)
                  next.set(handle, { handle, state: "active" })
                  return next
                }).pipe(Effect.as(handle))
              }
            }

            // Check capacity
            if (sessions.size >= maxSessions) {
              return Effect.fail(
                new PoolExhaustedError(sessions.size, maxSessions),
              )
            }

            // Create new session
            return EffectClient.openSession(baseUrl, sessionConfig).pipe(
              Effect.tap((handle) =>
                Ref.update(sessionsRef, (s) => {
                  const next = new Map(s)
                  next.set(handle, { handle, state: "active" })
                  return next
                }),
              ),
            )
          },
        ),
      )

    const release = (handle: string): Effect.Effect<void> =>
      Ref.update(sessionsRef, (s) => {
        const next = new Map(s)
        const session = next.get(handle)
        if (session) {
          next.set(handle, { ...session, state: "idle" })
        }
        return next
      })

    // Heartbeat: ping all idle sessions to keep them alive on the server
    const heartbeatOnce = Ref.get(sessionsRef).pipe(
      Effect.flatMap((sessions) => {
        const idleHandles = [...sessions.entries()]
          .filter(([, s]) => s.state === "idle")
          .map(([h]) => h)
        if (idleHandles.length === 0) return Effect.void
        return Effect.all(
          idleHandles.map((handle) =>
            EffectClient.heartbeatSession(baseUrl, handle).pipe(
              Effect.catchAll(() => Effect.void),
            ),
          ),
          { concurrency: "unbounded" },
        ).pipe(Effect.asVoid)
      }),
    )

    // Fork heartbeat fiber if enabled
    const heartbeatFiber =
      heartbeatIntervalMs > 0
        ? yield* heartbeatOnce.pipe(
            Effect.repeat(
              Schedule.spaced(Duration.millis(heartbeatIntervalMs)),
            ),
            Effect.catchAll(() => Effect.void),
            Effect.fork,
          )
        : null

    const dispose = (): Effect.Effect<void> =>
      Effect.gen(function* () {
        // Stop heartbeat fiber
        if (heartbeatFiber) {
          yield* Fiber.interrupt(heartbeatFiber)
        }
        // Close all sessions
        const sessions = yield* Ref.getAndSet(sessionsRef, new Map())
        yield* Effect.all(
          [...sessions.keys()].map((handle) =>
            EffectClient.closeSession(baseUrl, handle).pipe(
              Effect.catchAll(() => Effect.void),
            ),
          ),
          { concurrency: "unbounded" },
        )
      })

    const activeCount = Ref.get(sessionsRef).pipe(
      Effect.map((sessions) => {
        let count = 0
        for (const session of sessions.values()) {
          if (session.state === "active") count++
        }
        return count
      }),
    )

    const idleCount = Ref.get(sessionsRef).pipe(
      Effect.map((sessions) => {
        let count = 0
        for (const session of sessions.values()) {
          if (session.state === "idle") count++
        }
        return count
      }),
    )

    return { acquire, release, dispose, activeCount, idleCount }
  })
}
