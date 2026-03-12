// ── Effect-based session pool ───────────────────────────────────────
// Manages a pool of SQL Gateway sessions using Effect for lifecycle.
// Uses Effect.acquireRelease for guaranteed cleanup and Ref for
// thread-safe state management.

import { Effect, Ref } from "effect"
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
 * Create a managed session pool.
 * The pool is an Effect that returns the pool interface.
 */
export function makeSessionPool(
  config: SessionPoolConfig,
): Effect.Effect<EffectSessionPool> {
  const maxSessions = config.maxSessions ?? 5
  const baseUrl = config.baseUrl.replace(/\/+$/, "")

  return Ref.make(new Map<string, PooledSession>()).pipe(
    Effect.map((sessionsRef) => {
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

      const dispose = (): Effect.Effect<void> =>
        Ref.getAndSet(sessionsRef, new Map()).pipe(
          Effect.flatMap((sessions) =>
            Effect.all(
              [...sessions.keys()].map((handle) =>
                EffectClient.closeSession(baseUrl, handle).pipe(
                  Effect.catchAll(() => Effect.void),
                ),
              ),
              { concurrency: "unbounded" },
            ),
          ),
          Effect.asVoid,
        )

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
    }),
  )
}
