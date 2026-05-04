/**
 * CLI SQL Gateway Client — re-exports the shared SqlGatewayClient from
 * src/lib/sql-gateway/ and extends it with CLI-specific methods (submitSqlFile).
 *
 * All existing imports from this module continue to work unchanged.
 */

// Re-export shared types for consumers that import from this module
export { SqlGatewayClientError } from "../../lib/sql-gateway/client.js"
// Re-export shared utilities
export { splitSqlStatements } from "../../lib/sql-gateway/sql-utils.js"
export type {
  ColumnInfo,
  ResultPage,
  SessionConfig,
  StatementStatus,
} from "../../lib/sql-gateway/types.js"

import { SqlGatewayClient as SharedSqlGatewayClient } from "../../lib/sql-gateway/client.js"
import { splitSqlStatements } from "../../lib/sql-gateway/sql-utils.js"

export interface SubmitResult {
  sessionHandle: string
  operationHandle: string
  status: "RUNNING" | "FINISHED" | "ERROR"
  jobId?: string
  errorMessage?: string
}

/**
 * Extended SQL Gateway Client for CLI use.
 *
 * Inherits all shared client methods (openSession, closeSession, submitStatement,
 * getOperationStatus, fetchResults, cancelOperation, fetchResultStream, executeAndStream)
 * and adds CLI-specific methods for file-based SQL submission.
 */
export class SqlGatewayClient extends SharedSqlGatewayClient {
  /**
   * Poll operation status — CLI-friendly alias for getOperationStatus.
   * Maps INITIALIZED → RUNNING for CLI consumers that expect 3-state results.
   */
  async pollStatus(
    sessionHandle: string,
    operationHandle: string,
  ): Promise<"RUNNING" | "FINISHED" | "ERROR"> {
    const status = await this.getOperationStatus(sessionHandle, operationHandle)
    if (status === "INITIALIZED" || status === "PENDING") {
      return "RUNNING"
    }
    if (status === "CANCELED") {
      return "ERROR"
    }
    return status as "RUNNING" | "FINISHED" | "ERROR"
  }

  /**
   * Fetch the result of an operation to extract error details.
   * CLI-friendly wrapper around fetchResults that parses Java exception chains.
   */
  async fetchResult(
    sessionHandle: string,
    operationHandle: string,
  ): Promise<{ error?: string }> {
    try {
      const page = await this.fetchResults(sessionHandle, operationHandle, 0)
      if (page.resultKind === "ERROR") {
        return { error: "Statement execution failed" }
      }
      return {}
    } catch (err) {
      if (err instanceof Error) {
        // Extract root cause from Java exception chain
        const fullError = err.message
        const causeMatch = fullError.match(
          /Caused by: [^\n]*TableException: ([^\n]+)/,
        )
        return { error: causeMatch ? causeMatch[1] : fullError }
      }
      return {}
    }
  }

  /**
   * Read a SQL file, split into statements, and submit each one.
   * Polls status for each statement, handling streaming vs batch differently.
   *
   * Opens the session with `pipeline.sql` set to the full SQL file content so
   * every job created in this session preserves the source SQL on its
   * user-config map (visible via /jobs/:id/config and the dashboard's SQL
   * tab). Flink propagates session properties to job-level config.
   */
  async submitSqlFile(filePath: string): Promise<SubmitResult> {
    const { readFileSync } = await import("node:fs")
    const sql = readFileSync(filePath, "utf-8")
    const statements = splitSqlStatements(sql)

    const sessionHandle = await this.openSession({
      properties: { "pipeline.sql": sql },
    })

    let lastOperationHandle = ""
    let lastStatus: "RUNNING" | "FINISHED" | "ERROR" = "RUNNING"
    let errorMessage: string | undefined

    for (const stmt of statements) {
      lastOperationHandle = await this.submitStatement(sessionHandle, stmt)

      if (isStreamingStatement(stmt)) {
        // Streaming statements stay RUNNING indefinitely when healthy.
        // Poll a few times to catch fast failures (e.g. changelog mode errors,
        // missing connectors, schema mismatches) that happen within seconds.
        const settleAttempts = 5
        for (let i = 0; i < settleAttempts; i++) {
          await sleep(1000)
          lastStatus = await this.pollStatus(sessionHandle, lastOperationHandle)
          if (lastStatus === "ERROR" || lastStatus === "FINISHED") {
            break
          }
        }
      } else {
        // DDL/batch statements: poll until terminal state
        const maxAttempts = 60
        for (let i = 0; i < maxAttempts; i++) {
          lastStatus = await this.pollStatus(sessionHandle, lastOperationHandle)
          if (lastStatus !== "RUNNING") {
            break
          }
          await sleep(1000)
        }
      }

      if (lastStatus === "ERROR") {
        // Fetch the actual error details from the SQL Gateway
        const result = await this.fetchResult(
          sessionHandle,
          lastOperationHandle,
        )
        errorMessage = result.error
        break
      }
    }

    // Leave session open for streaming jobs (closing kills the job)
    // Close batch sessions after completion
    if (lastStatus === "FINISHED" || lastStatus === "ERROR") {
      await this.closeSession(sessionHandle)
    }

    return {
      sessionHandle,
      operationHandle: lastOperationHandle,
      status: lastStatus,
      errorMessage,
    }
  }
}

function isStreamingStatement(sql: string): boolean {
  const upper = sql.toUpperCase()
  return (
    upper.includes("INSERT INTO") || upper.includes("EXECUTE STATEMENT SET")
  )
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}
