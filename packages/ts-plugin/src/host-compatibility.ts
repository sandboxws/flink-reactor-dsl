/**
 * Host compatibility definitions for ts-plugin.
 *
 * Defines supported editor host pathways, TypeScript version policy,
 * and compatibility validation utilities.
 */

/** Supported editor host pathways for tsserver plugin loading */
export const SUPPORTED_HOSTS = ["vscode", "ts_ls", "vtsls"] as const
export type SupportedHost = (typeof SUPPORTED_HOSTS)[number]

/** Host-specific plugin loading mechanisms */
export const HOST_LOADING_PATHS: Record<SupportedHost, string> = {
  vscode: "compilerOptions.plugins in tsconfig.json",
  ts_ls: "init_options.plugins in LSP client configuration",
  vtsls: "tsserver.globalPlugins in vtsls settings",
}

/**
 * Supported TypeScript version range.
 *
 * Minimum: 5.0.0 (matches peerDependencies)
 * Maximum: latest stable (no upper bound)
 *
 * The plugin uses only stable tsserver plugin APIs that have been
 * available since TypeScript 5.0.
 */
export const TS_VERSION_POLICY = {
  minimum: "5.0.0",
  minimumMajor: 5,
  minimumMinor: 0,
} as const

/**
 * Parse a TypeScript version string into major.minor.patch components.
 * Returns undefined if the version string cannot be parsed.
 */
export function parseTypeScriptVersion(
  version: string,
): { major: number; minor: number; patch: number } | undefined {
  const match = version.match(/^(\d+)\.(\d+)\.(\d+)/)
  if (!match) return undefined
  return {
    major: Number(match[1]),
    minor: Number(match[2]),
    patch: Number(match[3]),
  }
}

/**
 * Check whether a TypeScript version meets the minimum supported version.
 */
export function isTypeScriptVersionSupported(version: string): boolean {
  const parsed = parseTypeScriptVersion(version)
  if (!parsed) return false
  if (parsed.major > TS_VERSION_POLICY.minimumMajor) return true
  if (parsed.major < TS_VERSION_POLICY.minimumMajor) return false
  return parsed.minor >= TS_VERSION_POLICY.minimumMinor
}

/** Compatibility matrix summary for documentation/logging */
export const COMPATIBILITY_MATRIX: ReadonlyArray<{
  host: SupportedHost
  loadingPath: string
  notes: string
}> = [
  {
    host: "vscode",
    loadingPath: HOST_LOADING_PATHS.vscode,
    notes:
      "Workspace TypeScript version must be selected if VS Code bundles an older TS",
  },
  {
    host: "ts_ls",
    loadingPath: HOST_LOADING_PATHS.ts_ls,
    notes: "Plugin path must resolve to the installed package location",
  },
  {
    host: "vtsls",
    loadingPath: HOST_LOADING_PATHS.vtsls,
    notes: "Plugin registered via globalPlugins; workspace TS must be >=5.0",
  },
]
