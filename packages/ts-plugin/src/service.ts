/**
 * Language service proxy creation and integration.
 *
 * Wraps the TypeScript language service with a proxy that intercepts
 * specific methods (e.g., getSemanticDiagnostics) to inject
 * flink-reactor behavior.
 */
import type ts from "typescript"
import type { ComponentRulesRegistry } from "./types"
import { getNestingDiagnostics } from "./diagnostics"

export interface PluginConfig {
  /** Override or extend component hierarchy rules */
  rules?: Record<string, string[] | "*">
  /** Disable nesting diagnostics */
  disableDiagnostics?: boolean
}

export type PluginLogger = (msg: string) => void

/**
 * Create a language service proxy that delegates to the original service
 * while injecting flink-reactor diagnostics on .tsx files.
 */
export function createLanguageServiceProxy(
  info: ts.server.PluginCreateInfo,
  registry: ComponentRulesRegistry,
  tsModule: typeof ts,
  config: PluginConfig,
  log: PluginLogger,
): ts.LanguageService {
  const proxy = Object.create(null) as ts.LanguageService
  for (const key of Object.keys(info.languageService) as Array<
    keyof ts.LanguageService
  >) {
    const original = info.languageService[key]
    // biome-ignore lint/suspicious/noExplicitAny: dynamic proxy assignment requires any cast
    ;(proxy as any)[key] =
      typeof original === "function"
        ? original.bind(info.languageService)
        : original
  }

  if (!config.disableDiagnostics) {
    proxy.getSemanticDiagnostics = (fileName: string): ts.Diagnostic[] => {
      const original = info.languageService.getSemanticDiagnostics(fileName)
      if (!fileName.endsWith(".tsx")) return original

      const program = info.languageService.getProgram()
      const sourceFile = program?.getSourceFile(fileName)
      if (!sourceFile) return original

      const nestingDiags = getNestingDiagnostics(sourceFile, registry, tsModule)
      log(`Found ${nestingDiags.length} nesting diagnostic(s) in ${fileName}`)
      return [...original, ...nestingDiags]
    }
  }

  return proxy
}
