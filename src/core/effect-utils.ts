// ── Bridge utilities ────────────────────────────────────────────────
// Functions that bridge Effect code with non-Effect callers.
// Used during the incremental migration — existing imperative code
// can call these to enter the Effect world and get results back.

import { Effect } from "effect"
import { ValidationError } from "./errors.js"
import type { ValidationDiagnostic } from "./synth-context.js"

/**
 * Run an Effect synchronously, throwing on failure.
 *
 * Use this at boundaries where non-Effect code needs to call
 * Effect code and cannot propagate Effect values.
 *
 * @example
 * ```ts
 * const result = runSync(myEffect)
 * ```
 */
export function runSync<A, E>(effect: Effect.Effect<A, E>): A {
  return Effect.runSync(effect)
}

/**
 * Run an Effect as a Promise, rejecting on failure.
 *
 * Use this at async boundaries (CLI command handlers, tests)
 * where the caller expects a Promise.
 *
 * @example
 * ```ts
 * const result = await runPromise(myEffect)
 * ```
 */
export function runPromise<A, E>(effect: Effect.Effect<A, E>): Promise<A> {
  return Effect.runPromise(effect)
}

/**
 * Wrap a throwing function into an Effect that captures the error.
 *
 * @example
 * ```ts
 * const readJson = fromThrowable(() => JSON.parse(raw))
 * ```
 */
export function fromThrowable<A>(fn: () => A): Effect.Effect<A, Error> {
  return Effect.try({ try: fn, catch: (error) => error as Error })
}

/**
 * Wrap an async throwing function into an Effect.
 *
 * @example
 * ```ts
 * const data = fromThrowableAsync(() => fetch(url).then(r => r.json()))
 * ```
 */
export function fromThrowableAsync<A>(
  fn: () => Promise<A>,
): Effect.Effect<A, Error> {
  return Effect.tryPromise({ try: fn, catch: (error) => error as Error })
}

/**
 * Convert validation diagnostics into an Effect that succeeds (no errors)
 * or fails with a ValidationError (if errors are present).
 *
 * Warnings are returned in the success channel.
 *
 * @example
 * ```ts
 * const program = toValidationEffect(diagnostics)
 *   .pipe(Effect.map(warnings => { ... }))
 * ```
 */
export function toValidationEffect(
  diagnostics: readonly ValidationDiagnostic[],
): Effect.Effect<readonly ValidationDiagnostic[], ValidationError> {
  const errors = diagnostics.filter((d) => d.severity === "error")
  const warnings = diagnostics.filter((d) => d.severity === "warning")

  if (errors.length > 0) {
    return Effect.fail(new ValidationError({ diagnostics: errors }))
  }

  return Effect.succeed(warnings)
}

/**
 * Run an Effect and extract the Cause on failure for structured
 * error display in CLI commands.
 *
 * Returns an Either-like result:
 * - `{ _tag: "success", value: A }`
 * - `{ _tag: "failure", cause: Cause<E> }`
 */
export async function runWithCause<A, E>(
  effect: Effect.Effect<A, E>,
): Promise<
  | { readonly _tag: "success"; readonly value: A }
  | { readonly _tag: "failure"; readonly cause: unknown }
> {
  return Effect.runPromise(
    effect.pipe(
      Effect.map((value) => ({
        _tag: "success" as const,
        value,
      })),
      Effect.catchAllCause((cause) =>
        Effect.succeed({
          _tag: "failure" as const,
          cause,
        }),
      ),
    ),
  )
}
