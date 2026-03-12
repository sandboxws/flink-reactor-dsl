// ── CLI Effect runner ───────────────────────────────────────────────
// Shared entry point for running Effect programs in CLI commands.
// Provides the MainLive layer and handles error display from Cause.

import { Cause, Effect } from "effect"
import pc from "picocolors"
import type { SynthError } from "../core/errors.js"
import { MainLive } from "../core/layers.js"

/**
 * Run an Effect program as a CLI command action.
 *
 * Provides MainLive, runs the effect, and handles failures
 * by displaying user-friendly error messages derived from
 * Effect's Cause.
 */
export async function runEffectCommand<A>(
  program: Effect.Effect<A, SynthError>,
): Promise<A | undefined> {
  const result = await Effect.runPromise(
    program.pipe(
      Effect.map((value) => ({ _tag: "success" as const, value })),
      Effect.catchAllCause((cause) =>
        Effect.succeed({
          _tag: "failure" as const,
          cause,
        }),
      ),
      Effect.provide(MainLive),
    ),
  )

  if (result._tag === "success") {
    return result.value
  }

  displayCauseError(result.cause)
  process.exitCode = 1
  return undefined
}

/**
 * Display a user-friendly error message from an Effect Cause.
 */
function displayCauseError(cause: Cause.Cause<unknown>): void {
  const failures = Cause.failures(cause)

  for (const failure of failures) {
    const error = failure as Record<string, unknown>

    switch (error._tag) {
      case "ConfigError":
        console.error(pc.red(`Configuration error: ${error.message}`))
        if (error.varName) {
          console.error(pc.dim(`  Missing env var: ${error.varName}`))
        }
        break

      case "ValidationError": {
        const diagnostics = error.diagnostics as Array<{
          severity: string
          message: string
          component?: string
        }>
        for (const d of diagnostics) {
          const prefix =
            d.severity === "error" ? pc.red("error:") : pc.yellow("warning:")
          console.error(`  ${prefix} ${d.message}`)
          if (d.component) {
            console.error(pc.dim(`    component: ${d.component}`))
          }
        }
        break
      }

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
      case "SqlGatewayResponseError":
      case "SqlGatewayTimeoutError":
        console.error(pc.red(`SQL Gateway error: ${error.message}`))
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

  // Handle defects (unexpected exceptions)
  const defects = Cause.defects(cause)
  for (const defect of defects) {
    const err = defect instanceof Error ? defect.message : String(defect)
    console.error(pc.red(`Unexpected error: ${err}`))
  }
}
