// ── SQL Gateway health check ────────────────────────────────────────
// Effect-based health check with exponential backoff retry and
// structured health status reporting.

import { Duration, Effect, Schedule } from "effect"
import {
  SqlGatewayConnectionError,
  type SqlGatewayError,
  SqlGatewayResponseError,
} from "../../core/errors.js"

// ── Health status types ─────────────────────────────────────────────

export interface HealthStatus {
  readonly status: "healthy" | "degraded" | "unhealthy"
  readonly latencyMs: number
  readonly message?: string
  readonly checkedAt: Date
}

export interface HealthCheckConfig {
  readonly baseUrl: string
  /** Max retries before reporting unhealthy. Default: 3 */
  readonly maxRetries?: number
  /** Base delay for exponential backoff in ms. Default: 500 */
  readonly baseDelayMs?: number
  /** Max delay cap in ms. Default: 10_000 */
  readonly maxDelayMs?: number
  /** Latency threshold in ms above which status is "degraded". Default: 2_000 */
  readonly degradedThresholdMs?: number
}

// ── Implementation ──────────────────────────────────────────────────

/**
 * Probe the SQL Gateway /info endpoint.
 * Returns the response latency in ms.
 */
function probeGateway(
  baseUrl: string,
): Effect.Effect<number, SqlGatewayError> {
  const url = `${baseUrl.replace(/\/+$/, "")}/info`
  const start = Date.now()

  return Effect.tryPromise({
    try: () => fetch(url, { headers: { Accept: "application/json" } }),
    catch: (err) =>
      new SqlGatewayConnectionError({
        message: `SQL Gateway unreachable at ${baseUrl}: ${(err as Error).message}`,
        baseUrl,
      }),
  }).pipe(
    Effect.flatMap((res) => {
      if (!res.ok) {
        return Effect.fail(
          new SqlGatewayResponseError({
            message: `SQL Gateway health check failed: ${res.status} ${res.statusText}`,
            statusCode: res.status,
            baseUrl,
          }),
        )
      }
      return Effect.succeed(Date.now() - start)
    }),
  )
}

/**
 * Check SQL Gateway health with exponential backoff retry.
 * Returns a structured HealthStatus — never fails (unhealthy is a value, not an error).
 */
export function checkHealth(
  config: HealthCheckConfig,
): Effect.Effect<HealthStatus> {
  const maxRetries = config.maxRetries ?? 3
  const baseDelayMs = config.baseDelayMs ?? 500
  const maxDelayMs = config.maxDelayMs ?? 10_000
  const degradedThresholdMs = config.degradedThresholdMs ?? 2_000

  const retrySchedule = Schedule.exponential(Duration.millis(baseDelayMs)).pipe(
    Schedule.jittered,
    Schedule.either(Schedule.spaced(Duration.millis(maxDelayMs))),
    Schedule.intersect(Schedule.recurs(maxRetries)),
  )

  return probeGateway(config.baseUrl).pipe(
    Effect.retry(retrySchedule),
    Effect.map(
      (latencyMs): HealthStatus => ({
        status: latencyMs > degradedThresholdMs ? "degraded" : "healthy",
        latencyMs,
        checkedAt: new Date(),
      }),
    ),
    Effect.catchAll(
      (err): Effect.Effect<HealthStatus> =>
        Effect.succeed({
          status: "unhealthy",
          latencyMs: -1,
          message: err.message,
          checkedAt: new Date(),
        }),
    ),
  )
}

/**
 * Wait for the SQL Gateway to become healthy.
 * Retries with exponential backoff until healthy or timeout.
 */
export function waitForHealthy(
  config: HealthCheckConfig & { readonly timeoutMs?: number },
): Effect.Effect<HealthStatus, SqlGatewayError> {
  const timeoutMs = config.timeoutMs ?? 60_000
  const baseDelayMs = config.baseDelayMs ?? 1_000

  const schedule = Schedule.exponential(Duration.millis(baseDelayMs)).pipe(
    Schedule.jittered,
    Schedule.either(Schedule.spaced(Duration.millis(5_000))),
    Schedule.compose(Schedule.elapsed),
    Schedule.whileOutput(Duration.lessThan(Duration.millis(timeoutMs))),
  )

  return probeGateway(config.baseUrl).pipe(
    Effect.retry(schedule),
    Effect.map(
      (latencyMs): HealthStatus => ({
        status: "healthy",
        latencyMs,
        checkedAt: new Date(),
      }),
    ),
  )
}
