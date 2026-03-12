// ── Effect-based health check ───────────────────────────────────────
// Replaces manual polling loops with Effect.retry + Schedule.
// Parallel health checks via Effect.all with unbounded concurrency.

import { Duration, Effect, Schedule } from "effect"
import { ClusterError } from "../../core/errors.js"

// ── Individual service checks ───────────────────────────────────────

function checkHttp(
  url: string,
  serviceName: string,
): Effect.Effect<void, ClusterError> {
  return Effect.tryPromise({
    try: () => fetch(url),
    catch: () =>
      new ClusterError({
        reason: "timeout",
        message: `${serviceName} not reachable at ${url}`,
      }),
  }).pipe(
    Effect.flatMap((res) =>
      res.ok
        ? Effect.void
        : Effect.fail(
            new ClusterError({
              reason: "timeout",
              message: `${serviceName} returned ${res.status} at ${url}`,
            }),
          ),
    ),
  )
}

function checkTcp(
  port: number,
  serviceName: string,
): Effect.Effect<void, ClusterError> {
  return Effect.async<void, ClusterError>((resume) => {
    import("node:net").then(({ createConnection }) => {
      const socket = createConnection({ host: "localhost", port }, () => {
        socket.destroy()
        resume(Effect.void)
      })
      socket.on("error", () => {
        socket.destroy()
        resume(
          Effect.fail(
            new ClusterError({
              reason: "timeout",
              message: `${serviceName} not reachable on port ${port}`,
            }),
          ),
        )
      })
      socket.setTimeout(2000, () => {
        socket.destroy()
        resume(
          Effect.fail(
            new ClusterError({
              reason: "timeout",
              message: `${serviceName} timed out on port ${port}`,
            }),
          ),
        )
      })
    })
  })
}

// ── Health check with retry ─────────────────────────────────────────

function checkWithRetry(
  check: Effect.Effect<void, ClusterError>,
  intervalMs: number,
  timeoutMs: number,
): Effect.Effect<void, ClusterError> {
  return check.pipe(
    Effect.retry(
      Schedule.spaced(Duration.millis(intervalMs)).pipe(
        Schedule.compose(Schedule.elapsed),
        Schedule.whileOutput(Duration.lessThan(Duration.millis(timeoutMs))),
      ),
    ),
  )
}

// ── Public API ──────────────────────────────────────────────────────

export interface HealthCheckOptions {
  readonly flinkPort: number
  readonly sqlGatewayPort: number
  readonly kafkaPort: number
  readonly postgresPort?: number
  readonly timeoutMs?: number
  readonly intervalMs?: number
}

/**
 * Effect-based service health check.
 * Checks all services in parallel with retry + schedule.
 * Returns a record of service name → ready status.
 */
export function waitForServicesEffect(
  opts: HealthCheckOptions,
): Effect.Effect<void, ClusterError> {
  const timeoutMs = opts.timeoutMs ?? 60_000
  const intervalMs = opts.intervalMs ?? 2_000

  const checks = [
    checkWithRetry(
      checkHttp(
        `http://localhost:${opts.flinkPort}/overview`,
        "Flink JobManager",
      ),
      intervalMs,
      timeoutMs,
    ),
    checkWithRetry(
      checkHttp(`http://localhost:${opts.sqlGatewayPort}/info`, "SQL Gateway"),
      intervalMs,
      timeoutMs,
    ),
    checkWithRetry(checkTcp(opts.kafkaPort, "Kafka"), intervalMs, timeoutMs),
    checkWithRetry(
      checkTcp(opts.postgresPort ?? 5432, "PostgreSQL"),
      intervalMs,
      timeoutMs,
    ),
  ]

  return Effect.all(checks, { concurrency: "unbounded" }).pipe(Effect.asVoid)
}

/**
 * Check if Flink cluster is running (non-blocking, no retry).
 */
export function isClusterRunningEffect(
  flinkPort: number,
): Effect.Effect<boolean> {
  return checkHttp(
    `http://localhost:${flinkPort}/overview`,
    "Flink JobManager",
  ).pipe(
    Effect.as(true),
    Effect.catchAll(() => Effect.succeed(false)),
  )
}
