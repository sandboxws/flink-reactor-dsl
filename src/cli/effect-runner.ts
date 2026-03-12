// ── CLI Effect runner ───────────────────────────────────────────────
// Shared entry point for running Effect programs in CLI commands.
// Provides the AppLayer and handles error display from Cause.

import { Cause, Effect, type Layer } from "effect"
import { renderCause } from "./error-display.js"
import type { SynthError } from "../core/errors.js"
import { AppLayer } from "../core/layers.js"

/** All services provided by AppLayer */
type AppServices = Layer.Layer.Success<typeof AppLayer>

/**
 * Run an Effect program as a CLI command action.
 *
 * Provides AppLayer (all services including ProcessEnv),
 * runs the effect, and handles failures by displaying
 * user-friendly error messages derived from Effect's Cause.
 */
/**
 * Bridge an Effect program into a Commander action.
 *
 * Convenience wrapper that reads --verbose from process.argv.
 * Use this from command action handlers.
 */
export function runCommand<A>(
  program: Effect.Effect<A, SynthError, AppServices>,
): Promise<A | undefined> {
  const verbose = process.argv.includes("--verbose")
  return runEffectCommand(program, { verbose })
}

export async function runEffectCommand<A>(
  program: Effect.Effect<A, SynthError, AppServices>,
  options?: { verbose?: boolean },
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
      Effect.provide(AppLayer),
    ),
  )

  if (result._tag === "success") {
    return result.value
  }

  process.exitCode = renderCause(result.cause, {
    verbose: options?.verbose,
  })
  return undefined
}
