export interface SubmitResult {
  sessionHandle: string
  operationHandle: string
  status: "RUNNING" | "FINISHED" | "ERROR"
  jobId?: string
  errorMessage?: string
}

interface OpenSessionResponse {
  sessionHandle: string
}

interface SubmitStatementResponse {
  operationHandle: string
}

interface OperationStatusResponse {
  status: "RUNNING" | "FINISHED" | "ERROR" | "INITIALIZED"
  results?: {
    columns?: Array<{ name: string }>
    data?: Array<{ fields: unknown[] }>
  }
}

export class SqlGatewayClient {
  constructor(private baseUrl: string) {}

  async openSession(): Promise<string> {
    const res = await fetch(`${this.baseUrl}/sessions`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ properties: {} }),
    })

    if (!res.ok) {
      throw new Error(`Failed to open session: ${res.status} ${res.statusText}`)
    }

    const data = (await res.json()) as OpenSessionResponse
    return data.sessionHandle
  }

  async submitStatement(sessionHandle: string, sql: string): Promise<string> {
    const res = await fetch(
      `${this.baseUrl}/sessions/${sessionHandle}/statements`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ statement: sql }),
      },
    )

    if (!res.ok) {
      const body = await res.text()
      throw new Error(`Failed to submit statement: ${res.status} ${body}`)
    }

    const data = (await res.json()) as SubmitStatementResponse
    return data.operationHandle
  }

  async pollStatus(
    sessionHandle: string,
    operationHandle: string,
  ): Promise<"RUNNING" | "FINISHED" | "ERROR"> {
    const res = await fetch(
      `${this.baseUrl}/sessions/${sessionHandle}/operations/${operationHandle}/status`,
    )

    if (!res.ok) {
      throw new Error(`Failed to poll status: ${res.status} ${res.statusText}`)
    }

    const data = (await res.json()) as OperationStatusResponse
    if (data.status === "INITIALIZED") {
      return "RUNNING"
    }
    return data.status as "RUNNING" | "FINISHED" | "ERROR"
  }

  /**
   * Fetch the result of an operation, which may contain error details.
   */
  async fetchResult(
    sessionHandle: string,
    operationHandle: string,
  ): Promise<{ error?: string }> {
    const res = await fetch(
      `${this.baseUrl}/sessions/${sessionHandle}/operations/${operationHandle}/result/0`,
    )

    if (!res.ok) {
      return {}
    }

    const data = (await res.json()) as {
      errors?: string[]
      results?: { data?: Array<{ fields: unknown[] }> }
    }

    if (data.errors && data.errors.length > 0) {
      // Extract the root cause from the Java exception chain
      const fullError = data.errors.join("\n")
      const causeMatch = fullError.match(
        /Caused by: [^\n]*TableException: ([^\n]+)/,
      )
      return { error: causeMatch ? causeMatch[1] : data.errors[0] }
    }

    return {}
  }

  async closeSession(sessionHandle: string): Promise<void> {
    const res = await fetch(`${this.baseUrl}/sessions/${sessionHandle}`, {
      method: "DELETE",
    })

    if (!res.ok) {
      throw new Error(
        `Failed to close session: ${res.status} ${res.statusText}`,
      )
    }
  }

  async submitSqlFile(filePath: string): Promise<SubmitResult> {
    const { readFileSync } = await import("node:fs")
    const sql = readFileSync(filePath, "utf-8")
    const statements = splitSqlStatements(sql)

    const sessionHandle = await this.openSession()

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

/**
 * Split a SQL file into individual statements, keeping
 * `EXECUTE STATEMENT SET BEGIN ... END;` as a single block.
 */
export function splitSqlStatements(sql: string): string[] {
  const statements: string[] = []
  const lines = sql.split("\n")
  let current = ""
  let inStatementSet = false

  for (const line of lines) {
    const trimmed = line.trim()

    // Skip empty lines and comments
    if (!trimmed || trimmed.startsWith("--")) {
      continue
    }

    // Detect STATEMENT SET block start (may be on one or two lines)
    if (/^EXECUTE\s+STATEMENT\s+SET\s*$/i.test(trimmed)) {
      // Flush any pending statement
      const pending = current.trim().replace(/;$/, "").trim()
      if (pending) statements.push(pending)
      current = `${trimmed}\n`
      inStatementSet = true
      continue
    }

    if (
      !inStatementSet &&
      /^EXECUTE\s+STATEMENT\s+SET\s+BEGIN\s*$/i.test(trimmed)
    ) {
      const pending = current.trim().replace(/;$/, "").trim()
      if (pending) statements.push(pending)
      current = `${trimmed}\n`
      inStatementSet = true
      continue
    }

    if (inStatementSet) {
      current += `${line}\n`
      // Detect STATEMENT SET block end
      if (/^END\s*;?\s*$/i.test(trimmed)) {
        inStatementSet = false
        statements.push(current.trim())
        current = ""
      }
      continue
    }

    // Regular statement: accumulate until semicolon
    current += `${line}\n`
    if (trimmed.endsWith(";")) {
      const stmt = current.trim().replace(/;$/, "").trim()
      if (stmt) {
        statements.push(stmt)
      }
      current = ""
    }
  }

  // Handle final statement without trailing semicolon
  const remaining = current.trim().replace(/;$/, "").trim()
  if (remaining) {
    statements.push(remaining)
  }

  return statements
}

function isStreamingStatement(sql: string): boolean {
  // INSERT INTO with unbounded source = streaming
  // EXECUTE STATEMENT SET = streaming
  const upper = sql.toUpperCase()
  return (
    upper.includes("INSERT INTO") || upper.includes("EXECUTE STATEMENT SET")
  )
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}
