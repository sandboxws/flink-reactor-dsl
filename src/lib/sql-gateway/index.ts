export type { SqlGatewayClientOptions } from "./client.js"
export {
  SqlGatewayClient,
  SqlGatewayClientError,
  StatementExecutionError,
} from "./client.js"
export { SqlGatewayCompatClient } from "./client-compat.js"
// ── Effect-based exports ────────────────────────────────────────────
export * as EffectClient from "./effect-client.js"
export {
  type EffectSessionPool,
  makeSessionPool,
  type SessionPoolConfig,
} from "./effect-session-pool.js"
export {
  checkHealth,
  type HealthCheckConfig,
  type HealthStatus,
  waitForHealthy,
} from "./health-check.js"
export type { SessionPoolOptions } from "./session-pool.js"
export { SessionPool } from "./session-pool.js"
export { splitSqlStatements } from "./sql-utils.js"
export type {
  ColumnInfo,
  OperationHandle,
  RawColumnInfo,
  RawFetchResultsResponse,
  RawGetOperationStatusResponse,
  RawOpenSessionResponse,
  RawRowData,
  RawSubmitStatementResponse,
  ResultPage,
  SessionConfig,
  SessionHandle,
  SqlGatewayError,
  StatementErrorDetail,
  StatementStatus,
} from "./types.js"
