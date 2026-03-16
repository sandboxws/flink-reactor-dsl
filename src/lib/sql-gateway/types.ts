// ─── SQL Gateway v1 REST API Types ──────────────────────────────────────────

/** SQL Gateway session handle returned by POST /v1/sessions */
export interface SessionHandle {
  sessionHandle: string
}

/** SQL Gateway operation handle returned by POST /v1/sessions/{sessionHandle}/statements */
export interface OperationHandle {
  operationHandle: string
}

/** Normalized column metadata extracted from result set */
export interface ColumnInfo {
  columnName: string
  dataType: string
  nullable: boolean
}

/** Normalized result page from fetchResults */
export interface ResultPage {
  columns: ColumnInfo[]
  rows: Record<string, unknown>[]
  nextResultUri: string | null
  isEndOfStream: boolean
  resultKind: "SUCCESS" | "SUCCESS_WITH_CONTENT" | "ERROR"
}

/** Statement lifecycle status */
export type StatementStatus =
  | "RUNNING"
  | "FINISHED"
  | "ERROR"
  | "CANCELED"
  | "PENDING"
  | "INITIALIZED"

/** Session configuration for openSession */
export interface SessionConfig {
  /** Flink configuration properties for this session */
  properties?: Record<string, string>
}

/** SQL Gateway REST API error response body */
export interface SqlGatewayError {
  errors: string[]
}

// ─── Statement Error Detail ─────────────────────────────────────────────────

/** Structured detail for a statement that failed during execution */
export interface StatementErrorDetail {
  /** The SQL statement that was submitted */
  statement: string
  /** Primary error message from Flink (first line / summary) */
  message: string
  /** Full error text including root causes and stack traces */
  fullMessage: string
  /** Extracted root cause message, if parseable from the error chain */
  rootCause: string | null
}

// ─── Raw REST API Response Types ────────────────────────────────────────────
// These match the actual JSON shapes returned by the SQL Gateway v1 REST API.

/** Raw response from POST /v1/sessions */
export interface RawOpenSessionResponse {
  sessionHandle: string
}

/** Raw response from POST /v1/sessions/{sessionHandle}/statements */
export interface RawSubmitStatementResponse {
  operationHandle: string
}

/** Raw column info from result set */
export interface RawColumnInfo {
  name: string
  logicalType: {
    type: string
    nullable: boolean
  }
}

/** Raw row data from result set */
export interface RawRowData {
  kind: "INSERT" | "UPDATE_BEFORE" | "UPDATE_AFTER" | "DELETE"
  fields: unknown[]
}

/** Raw response from GET /v1/sessions/{sessionHandle}/operations/{operationHandle}/result/{token} */
export interface RawFetchResultsResponse {
  results: {
    columns: RawColumnInfo[]
    data: RawRowData[]
  }
  resultType: "PAYLOAD" | "EOS" | "NOT_READY"
  nextResultUri: string | null
}

/** Raw response from GET /v1/sessions/{sessionHandle}/operations/{operationHandle}/status */
export interface RawGetOperationStatusResponse {
  status: StatementStatus
}
