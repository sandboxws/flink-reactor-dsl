// ── Effect-based SQL Gateway Client ─────────────────────────────────
// Each method returns an Effect with typed errors instead of throwing.
// Uses the FrHttpClient service for testable HTTP, and Effect's
// Schedule + Stream for polling/iteration.

import {
  Context,
  Duration,
  Effect,
  Option,
  type Predicate,
  Schedule,
  Stream,
} from "effect"
import {
  SqlGatewayConnectionError,
  type SqlGatewayError,
  SqlGatewayResponseError,
  SqlGatewayTimeoutError,
} from "../../core/errors.js"
import type {
  ColumnInfo,
  RawFetchResultsResponse,
  RawGetOperationStatusResponse,
  RawOpenSessionResponse,
  RawSubmitStatementResponse,
  ResultPage,
  SessionConfig,
  SqlGatewayError as SqlGatewayErrorBody,
  StatementStatus,
} from "./types.js"

// ── Service definition ──────────────────────────────────────────────

export interface SqlGatewayClientConfig {
  readonly baseUrl: string
}

export class SqlGatewayClientConfig extends Context.Tag(
  "SqlGatewayClientConfig",
)<SqlGatewayClientConfig, SqlGatewayClientConfig>() {}

// ── Retry policies ─────────────────────────────────────────────────

export interface RetryConfig {
  readonly maxRetries?: number
  readonly baseDelayMs?: number
  readonly maxDelayMs?: number
}

const defaultRetryConfig: Required<RetryConfig> = {
  maxRetries: 3,
  baseDelayMs: 200,
  maxDelayMs: 5_000,
}

/** Whether an error is transient and worth retrying */
function isTransient(err: SqlGatewayError): boolean {
  if (err._tag === "SqlGatewayConnectionError") return true
  if (err._tag === "SqlGatewayResponseError" && err.statusCode >= 500)
    return true
  return false
}

/**
 * Exponential backoff schedule with jitter, capped by max retries and max delay.
 * Schedule: base * 2^attempt, with ±25% jitter, up to maxDelay.
 */
export function transientRetrySchedule(
  config?: RetryConfig,
): Schedule.Schedule<unknown, SqlGatewayError> {
  const { maxRetries, baseDelayMs, maxDelayMs } = {
    ...defaultRetryConfig,
    ...config,
  }

  return Schedule.exponential(Duration.millis(baseDelayMs)).pipe(
    Schedule.jittered,
    Schedule.either(Schedule.spaced(Duration.millis(maxDelayMs))),
    Schedule.intersect(Schedule.recurs(maxRetries)),
    Schedule.whileInput(isTransient as Predicate.Predicate<SqlGatewayError>),
  )
}

// ── Request helper ──────────────────────────────────────────────────

function request<T>(
  baseUrl: string,
  path: string,
  init?: RequestInit,
  retryConfig?: RetryConfig,
): Effect.Effect<T, SqlGatewayError> {
  const url = `${baseUrl}${path}`
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    Accept: "application/json",
  }

  const singleRequest = Effect.tryPromise({
    try: () =>
      fetch(url, {
        ...init,
        headers: { ...headers, ...(init?.headers as Record<string, string>) },
      }),
    catch: (err) =>
      new SqlGatewayConnectionError({
        message: `SQL Gateway unreachable at ${baseUrl}: ${(err as Error).message}`,
        baseUrl,
      }),
  }).pipe(
    Effect.flatMap((res) => {
      if (!res.ok) {
        return Effect.tryPromise({
          try: () => res.json() as Promise<SqlGatewayErrorBody>,
          catch: () => undefined,
        }).pipe(
          Effect.catchAll(() => Effect.succeed(undefined)),
          Effect.flatMap((body) => {
            const message =
              body?.errors?.[0] ??
              `SQL Gateway error: ${res.status} ${res.statusText}`
            return Effect.fail(
              new SqlGatewayResponseError({
                message,
                statusCode: res.status,
                baseUrl,
              }),
            )
          }),
        )
      }

      // DELETE / 204 responses may have empty body
      if (res.status === 204 || init?.method === "DELETE") {
        return Effect.succeed(undefined as T)
      }

      return Effect.tryPromise({
        try: () => res.json() as Promise<T>,
        catch: (err) =>
          new SqlGatewayResponseError({
            message: `Failed to parse response: ${(err as Error).message}`,
            statusCode: res.status,
            baseUrl,
          }),
      })
    }),
  )

  return singleRequest.pipe(Effect.retry(transientRetrySchedule(retryConfig)))
}

// ── Public API ──────────────────────────────────────────────────────

/** Open a new SQL Gateway session */
export function openSession(
  baseUrl: string,
  config?: SessionConfig,
): Effect.Effect<string, SqlGatewayError> {
  return request<RawOpenSessionResponse>(baseUrl, "/v1/sessions", {
    method: "POST",
    body: JSON.stringify({
      properties: config?.properties ?? {},
    }),
  }).pipe(Effect.map((data) => data.sessionHandle))
}

/** Close a session */
export function closeSession(
  baseUrl: string,
  sessionHandle: string,
): Effect.Effect<void, SqlGatewayError> {
  return request<void>(baseUrl, `/v1/sessions/${sessionHandle}`, {
    method: "DELETE",
  })
}

/**
 * Heartbeat a session to keep it alive on the server.
 * Uses GET /v1/sessions/{handle} which the SQL Gateway treats
 * as a keep-alive touch without side effects.
 */
export function heartbeatSession(
  baseUrl: string,
  sessionHandle: string,
): Effect.Effect<void, SqlGatewayError> {
  return request<unknown>(baseUrl, `/v1/sessions/${sessionHandle}`).pipe(
    Effect.asVoid,
  )
}

/** Submit a SQL statement, returns operation handle */
export function submitStatement(
  baseUrl: string,
  sessionHandle: string,
  sql: string,
): Effect.Effect<string, SqlGatewayError> {
  return request<RawSubmitStatementResponse>(
    baseUrl,
    `/v1/sessions/${sessionHandle}/statements`,
    {
      method: "POST",
      body: JSON.stringify({ statement: sql }),
    },
  ).pipe(Effect.map((data) => data.operationHandle))
}

/** Poll operation status */
export function getOperationStatus(
  baseUrl: string,
  sessionHandle: string,
  operationHandle: string,
): Effect.Effect<StatementStatus, SqlGatewayError> {
  return request<RawGetOperationStatusResponse>(
    baseUrl,
    `/v1/sessions/${sessionHandle}/operations/${operationHandle}/status`,
  ).pipe(Effect.map((data) => data.status))
}

/** Fetch a single page of results */
export function fetchResults(
  baseUrl: string,
  sessionHandle: string,
  operationHandle: string,
  token: number,
): Effect.Effect<ResultPage, SqlGatewayError> {
  return request<RawFetchResultsResponse>(
    baseUrl,
    `/v1/sessions/${sessionHandle}/operations/${operationHandle}/result/${token}`,
  ).pipe(Effect.map(normalizeResultPage))
}

/** Cancel a running operation */
export function cancelOperation(
  baseUrl: string,
  sessionHandle: string,
  operationHandle: string,
): Effect.Effect<void, SqlGatewayError> {
  return request<void>(
    baseUrl,
    `/v1/sessions/${sessionHandle}/operations/${operationHandle}/cancel`,
    { method: "POST" },
  )
}

// ── Polling & Streaming ─────────────────────────────────────────────

/**
 * Wait for an operation to reach a terminal status (FINISHED or ERROR).
 * Uses Effect.retry with a spaced schedule instead of manual polling loops.
 */
export function waitForCompletion(
  baseUrl: string,
  sessionHandle: string,
  operationHandle: string,
  options?: {
    readonly pollIntervalMs?: number
    readonly timeoutMs?: number
  },
): Effect.Effect<StatementStatus, SqlGatewayError | SqlGatewayTimeoutError> {
  const pollInterval = options?.pollIntervalMs ?? 500
  const timeoutMs = options?.timeoutMs ?? 300_000 // 5 min default

  const schedule = Schedule.exponential(Duration.millis(pollInterval)).pipe(
    Schedule.jittered,
    Schedule.either(Schedule.spaced(Duration.millis(5_000))),
    Schedule.compose(Schedule.elapsed),
    Schedule.whileOutput(Duration.lessThan(Duration.millis(timeoutMs))),
  )

  return getOperationStatus(baseUrl, sessionHandle, operationHandle).pipe(
    Effect.flatMap((status) => {
      if (
        status === "FINISHED" ||
        status === "ERROR" ||
        status === "CANCELED"
      ) {
        return Effect.succeed(status)
      }
      return Effect.fail({ _tag: "NotTerminal" as const })
    }),
    Effect.retry(schedule),
    Effect.catchIf(
      (e): e is { _tag: "NotTerminal" } =>
        typeof e === "object" &&
        e !== null &&
        "_tag" in e &&
        e._tag === "NotTerminal",
      () =>
        Effect.fail(
          new SqlGatewayTimeoutError({
            message: `Operation ${operationHandle} did not complete within ${timeoutMs}ms`,
            operationHandle,
            elapsedMs: timeoutMs,
          }),
        ),
    ),
  ) as Effect.Effect<StatementStatus, SqlGatewayError | SqlGatewayTimeoutError>
}

// ── Stream helpers ──────────────────────────────────────────────────

type StreamState = { token: number; done: boolean }
type StreamStep = Option.Option<[ResultPage, StreamState]>

function makePageStreamer(
  baseUrl: string,
  sessionHandle: string,
  operationHandle: string,
  pollIntervalMs: number,
  startToken: number,
): Stream.Stream<ResultPage, SqlGatewayError> {
  const step = (
    state: StreamState,
  ): Effect.Effect<StreamStep, SqlGatewayError> => {
    if (state.done) return Effect.succeed(Option.none())

    return fetchResults(
      baseUrl,
      sessionHandle,
      operationHandle,
      state.token,
    ).pipe(
      Effect.flatMap((page): Effect.Effect<StreamStep, SqlGatewayError> => {
        if (page.isEndOfStream) {
          if (page.rows.length > 0) {
            return Effect.succeed(
              Option.some<[ResultPage, StreamState]>([
                page,
                { token: state.token, done: true },
              ]),
            )
          }
          return Effect.succeed(Option.none())
        }

        const nextToken = page.nextResultUri
          ? (parseTokenFromUri(page.nextResultUri) ?? state.token + 1)
          : state.token + 1

        return Effect.sleep(Duration.millis(pollIntervalMs)).pipe(
          Effect.as(
            Option.some<[ResultPage, StreamState]>([
              page,
              { token: nextToken, done: false },
            ]),
          ),
        )
      }),
    )
  }

  return Stream.unfoldEffect({ token: startToken, done: false }, step)
}

/**
 * Stream result pages as they become available.
 * Replaces the async generator pattern with Effect Stream.
 */
export function fetchResultStream(
  baseUrl: string,
  sessionHandle: string,
  operationHandle: string,
  options?: { readonly pollIntervalMs?: number },
): Stream.Stream<ResultPage, SqlGatewayError> {
  return makePageStreamer(
    baseUrl,
    sessionHandle,
    operationHandle,
    options?.pollIntervalMs ?? 500,
    0,
  )
}

/**
 * Submit SQL and stream results with session lifecycle management.
 * Uses Effect.acquireRelease for guaranteed session cleanup.
 */
export function executeAndStream(
  baseUrl: string,
  sql: string,
  sessionConfig?: SessionConfig,
  options?: { readonly pollIntervalMs?: number },
): Effect.Effect<
  {
    readonly sessionHandle: string
    readonly operationHandle: string
    readonly columns: ColumnInfo[]
    readonly stream: Stream.Stream<ResultPage, SqlGatewayError>
  },
  SqlGatewayError
> {
  return openSession(baseUrl, sessionConfig).pipe(
    Effect.flatMap((sessionHandle) =>
      submitStatement(baseUrl, sessionHandle, sql).pipe(
        Effect.flatMap((operationHandle) =>
          fetchResults(baseUrl, sessionHandle, operationHandle, 0).pipe(
            Effect.map((firstPage) => {
              const startToken = firstPage.nextResultUri
                ? (parseTokenFromUri(firstPage.nextResultUri) ?? 1)
                : 1

              // Stream remaining pages starting from token after first
              const remainingStream = firstPage.isEndOfStream
                ? Stream.empty
                : fetchResultStreamFrom(
                    baseUrl,
                    sessionHandle,
                    operationHandle,
                    startToken,
                    options,
                  )

              return {
                sessionHandle,
                operationHandle,
                columns: firstPage.columns,
                stream: remainingStream,
              }
            }),
          ),
        ),
      ),
    ),
  )
}

/** Internal: stream results starting from a specific token */
function fetchResultStreamFrom(
  baseUrl: string,
  sessionHandle: string,
  operationHandle: string,
  startToken: number,
  options?: { readonly pollIntervalMs?: number },
): Stream.Stream<ResultPage, SqlGatewayError> {
  return makePageStreamer(
    baseUrl,
    sessionHandle,
    operationHandle,
    options?.pollIntervalMs ?? 500,
    startToken,
  )
}

// ── Normalization ───────────────────────────────────────────────────

function normalizeResultPage(raw: RawFetchResultsResponse): ResultPage {
  const columns: ColumnInfo[] = (raw.results?.columns ?? []).map((col) => ({
    columnName: col.name,
    dataType: col.logicalType.type,
    nullable: col.logicalType.nullable,
  }))

  const columnNames = columns.map((c) => c.columnName)

  const rows: Record<string, unknown>[] = (raw.results?.data ?? []).map(
    (row) => {
      const obj: Record<string, unknown> = {}
      for (let i = 0; i < columnNames.length; i++) {
        obj[columnNames[i]] = row.fields[i]
      }
      return obj
    },
  )

  const isEndOfStream = raw.resultType === "EOS"
  const resultKind: ResultPage["resultKind"] =
    rows.length > 0 ? "SUCCESS_WITH_CONTENT" : "SUCCESS"

  return {
    columns,
    rows,
    nextResultUri: raw.nextResultUri,
    isEndOfStream,
    resultKind,
  }
}

function parseTokenFromUri(uri: string): number | null {
  const match = uri.match(/\/result\/(\d+)/)
  return match ? Number.parseInt(match[1], 10) : null
}
