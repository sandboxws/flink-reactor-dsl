// ── Cause → terminal error renderer ────────────────────────────────
// Converts an Effect Cause into structured, color-coded terminal output.
// Each error type gets a tailored display showing its relevant context
// (config path, gateway endpoint, validation diagnostics, etc.).

import { Cause } from "effect"
import pc from "picocolors"

interface RenderOptions {
  /** When true, show full Cause tree for unexpected defects */
  readonly verbose?: boolean
}

/**
 * Render an Effect Cause as user-friendly terminal output.
 *
 * Returns the suggested process exit code:
 * - 1 for known errors and defects
 * - 130 for interruption (Ctrl+C)
 */
export function renderCause(
  cause: Cause.Cause<unknown>,
  options: RenderOptions = {},
): number {
  // Handle interruption first (Ctrl+C)
  if (Cause.isInterruptedOnly(cause)) {
    console.error(pc.yellow("Operation cancelled."))
    return 130
  }

  // Render typed failures
  const failures = Cause.failures(cause)
  for (const failure of failures) {
    renderFailure(failure)
  }

  // Render defects (unexpected exceptions)
  const defects = Cause.defects(cause)
  for (const defect of defects) {
    renderDefect(defect, options.verbose ?? false)
  }

  return 1
}

function renderFailure(failure: unknown): void {
  const error = failure as Record<string, unknown>

  switch (error._tag) {
    case "ConfigError":
      renderConfigError(error)
      break

    case "ValidationError":
      renderValidationError(error)
      break

    case "DiscoveryError":
      console.error(pc.red(`Discovery error: ${error.message}`))
      if (error.path) {
        console.error(pc.dim(`  path: ${error.path}`))
      }
      break

    case "FileSystemError":
      console.error(
        pc.red(`File system error (${error.operation}): ${error.message}`),
      )
      console.error(pc.dim(`  path: ${error.path}`))
      break

    case "SqlGatewayConnectionError":
      console.error(pc.red(`SQL Gateway connection error: ${error.message}`))
      if (error.baseUrl) {
        console.error(pc.dim(`  endpoint: ${error.baseUrl}`))
      }
      break

    case "SqlGatewayResponseError":
      console.error(pc.red(`SQL Gateway error: ${error.message}`))
      if (error.statusCode) {
        console.error(pc.dim(`  HTTP status: ${error.statusCode}`))
      }
      if (error.baseUrl) {
        console.error(pc.dim(`  endpoint: ${error.baseUrl}`))
      }
      break

    case "SqlGatewayTimeoutError":
      console.error(pc.red(`SQL Gateway timeout: ${error.message}`))
      if (error.operationHandle) {
        console.error(pc.dim(`  operation: ${error.operationHandle}`))
      }
      console.error(pc.dim(`  elapsed: ${error.elapsedMs}ms`))
      break

    case "SqlGenerationError":
      console.error(pc.red(`SQL generation error: ${error.message}`))
      if (error.component) {
        console.error(pc.dim(`  component: ${error.component}`))
      }
      break

    case "CrdGenerationError":
      console.error(pc.red(`CRD generation error: ${error.message}`))
      if (error.pipelineName) {
        console.error(pc.dim(`  pipeline: ${error.pipelineName}`))
      }
      break

    case "ClusterError":
      console.error(pc.red(`Cluster error: ${error.message}`))
      if (error.command) {
        console.error(pc.dim(`  command: ${error.command}`))
      }
      break

    case "CliError":
      console.error(pc.red(`${error.message}`))
      break

    default:
      if (error.message) {
        console.error(pc.red(`Error: ${error.message}`))
      } else {
        console.error(pc.red("An unexpected error occurred."))
      }
  }
}

function renderConfigError(error: Record<string, unknown>): void {
  console.error(pc.red(`Configuration error: ${error.message}`))

  if (error.varName) {
    console.error(pc.dim(`  env var: ${error.varName}`))
  }
  if (error.path) {
    console.error(pc.dim(`  path: ${error.path}`))
  }
  if (error.environmentName) {
    console.error(pc.dim(`  environment: ${error.environmentName}`))
  }
}

function renderValidationError(error: Record<string, unknown>): void {
  const diagnostics = error.diagnostics as Array<{
    severity: string
    message: string
    component?: string
    nodeId?: string
  }>

  for (const d of diagnostics) {
    const prefix =
      d.severity === "error" ? pc.red("error:") : pc.yellow("warning:")
    console.error(`  ${prefix} ${d.message}`)
    if (d.component) {
      console.error(pc.dim(`    component: ${d.component}`))
    }
  }
}

function renderDefect(defect: unknown, verbose: boolean): void {
  if (verbose) {
    console.error(pc.red("\nUnexpected error (full trace):"))
    if (defect instanceof Error) {
      console.error(pc.dim(defect.stack ?? defect.message))
    } else {
      console.error(pc.dim(String(defect)))
    }
  } else {
    const msg = defect instanceof Error ? defect.message : String(defect)
    console.error(pc.red(`Unexpected error: ${msg}`))
    console.error(pc.dim("  Run with --verbose for full error trace."))
  }
}
