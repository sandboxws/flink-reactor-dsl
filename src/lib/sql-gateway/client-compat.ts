// ── Backward-compatible wrapper ─────────────────────────────────────
// Presents the old async/Promise API by calling Effect.runPromise
// on the new Effect-based client internally. Allows existing CLI
// commands to keep working during the migration.

import { Cause, Chunk, Effect, Stream } from "effect"
import * as EffectClient from "./effect-client.js"
import type {
  ColumnInfo,
  ResultPage,
  SessionConfig,
  StatementStatus,
} from "./types.js"

/**
 * Error thrown by the compat layer — wraps Effect errors back to
 * the original SqlGatewayClientError shape for backward compatibility.
 */
export class SqlGatewayClientError extends Error {
  constructor(
    message: string,
    public readonly statusCode: number,
  ) {
    super(message)
    this.name = "SqlGatewayClientError"
  }
}

export interface SqlGatewayClientOptions {
  signal?: AbortSignal
}

/**
 * Backward-compatible SQL Gateway client that delegates to the
 * Effect-based implementation. Drop-in replacement for the original
 * SqlGatewayClient class.
 */
export class SqlGatewayCompatClient {
  private readonly baseUrl: string

  constructor(baseUrl: string, _options?: SqlGatewayClientOptions) {
    this.baseUrl = baseUrl.replace(/\/+$/, "")
  }

  async openSession(config?: SessionConfig): Promise<string> {
    return this.run(EffectClient.openSession(this.baseUrl, config))
  }

  async closeSession(sessionHandle: string): Promise<void> {
    return this.run(EffectClient.closeSession(this.baseUrl, sessionHandle))
  }

  async submitStatement(sessionHandle: string, sql: string): Promise<string> {
    return this.run(
      EffectClient.submitStatement(this.baseUrl, sessionHandle, sql),
    )
  }

  async getOperationStatus(
    sessionHandle: string,
    operationHandle: string,
  ): Promise<StatementStatus> {
    return this.run(
      EffectClient.getOperationStatus(
        this.baseUrl,
        sessionHandle,
        operationHandle,
      ),
    )
  }

  async fetchResults(
    sessionHandle: string,
    operationHandle: string,
    token: number,
  ): Promise<ResultPage> {
    return this.run(
      EffectClient.fetchResults(
        this.baseUrl,
        sessionHandle,
        operationHandle,
        token,
      ),
    )
  }

  async cancelOperation(
    sessionHandle: string,
    operationHandle: string,
  ): Promise<void> {
    return this.run(
      EffectClient.cancelOperation(
        this.baseUrl,
        sessionHandle,
        operationHandle,
      ),
    )
  }

  async *fetchResultStream(
    sessionHandle: string,
    operationHandle: string,
    options?: { pollIntervalMs?: number; signal?: AbortSignal },
  ): AsyncGenerator<ResultPage, void, unknown> {
    const stream = EffectClient.fetchResultStream(
      this.baseUrl,
      sessionHandle,
      operationHandle,
      { pollIntervalMs: options?.pollIntervalMs },
    )

    const chunks = await Effect.runPromise(Stream.runCollect(stream))

    for (const page of chunks) {
      if (options?.signal?.aborted) return
      yield page
    }
  }

  async executeAndStream(
    sql: string,
    sessionConfig?: SessionConfig,
    options?: { pollIntervalMs?: number; signal?: AbortSignal },
  ): Promise<{
    sessionHandle: string
    operationHandle: string
    columns: ColumnInfo[]
    stream: AsyncGenerator<ResultPage, void, unknown>
  }> {
    const result = await this.run(
      EffectClient.executeAndStream(this.baseUrl, sql, sessionConfig, {
        pollIntervalMs: options?.pollIntervalMs,
      }),
    )
    async function* asyncStream(): AsyncGenerator<ResultPage, void, unknown> {
      const chunks = await Effect.runPromise(Stream.runCollect(result.stream))
      for (const page of chunks) {
        if (options?.signal?.aborted) return
        yield page
      }
    }

    return {
      sessionHandle: result.sessionHandle,
      operationHandle: result.operationHandle,
      columns: result.columns,
      stream: asyncStream(),
    }
  }

  private async run<A>(effect: Effect.Effect<A, unknown>): Promise<A> {
    const exit = await Effect.runPromiseExit(effect)
    if (exit._tag === "Success") return exit.value

    // Extract the typed error from the Cause
    const failures = Chunk.toArray(Cause.failures(exit.cause))
    const error =
      failures.length > 0
        ? (failures[0] as Record<string, unknown>)
        : undefined

    if (error?._tag === "SqlGatewayConnectionError") {
      throw new SqlGatewayClientError(error.message as string, 0)
    }
    if (error?._tag === "SqlGatewayResponseError") {
      throw new SqlGatewayClientError(
        error.message as string,
        error.statusCode as number,
      )
    }
    if (error?._tag === "SqlGatewayTimeoutError") {
      throw new SqlGatewayClientError(error.message as string, 0)
    }

    // Re-throw as FiberFailure for unexpected errors
    throw Cause.squash(exit.cause)
  }
}
