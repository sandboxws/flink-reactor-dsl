import type { ResolvedConfig } from "@/core/config-resolver.js"

/**
 * Resolve the console URL from multiple sources.
 *
 * Precedence:
 * 1. --console-url CLI flag (highest)
 * 2. REACTOR_CONSOLE_URL environment variable
 * 3. config.console.url from resolved config
 */
export function resolveConsoleUrl(opts: {
  consoleUrl?: string
  resolvedConfig?: ResolvedConfig
}): string | undefined {
  if (opts.consoleUrl) return opts.consoleUrl

  const envUrl = process.env.REACTOR_CONSOLE_URL
  if (envUrl) return envUrl

  return opts.resolvedConfig?.console?.url
}
