/**
 * Language service proxy creation and integration.
 *
 * Wraps the TypeScript language service with a proxy that intercepts
 * specific methods (e.g., getSemanticDiagnostics) to inject
 * flink-reactor behavior.
 */
import type ts from "typescript"
import {
  filterCompletionsByContext,
  rankCompletionsByContext,
} from "./completions"
import { getParentTagAtPosition } from "./context-detector"
import { getNestingDiagnostics } from "./diagnostics"
import type { ComponentRulesRegistry } from "./types"

export interface PluginConfig {
  /** Override or extend component hierarchy rules */
  rules?: Record<string, string[] | "*">
  /** Disable nesting diagnostics */
  disableDiagnostics?: boolean
  /** Disable context-aware completions */
  disableCompletions?: boolean
  /** Completion strategy: "rank" (default) preserves all entries, "filter" removes invalid ones */
  completionStrategy?: "rank" | "filter"
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

      try {
        const program = info.languageService.getProgram()
        const sourceFile = program?.getSourceFile(fileName)
        if (!sourceFile) return original

        const nestingDiags = getNestingDiagnostics(
          sourceFile,
          registry,
          tsModule,
        )
        log(`Found ${nestingDiags.length} nesting diagnostic(s) in ${fileName}`)
        return [...original, ...nestingDiags]
      } catch (e) {
        log(
          `ERROR in diagnostics for ${fileName}: ${e instanceof Error ? e.message : String(e)}. Falling back to baseline.`,
        )
        return original
      }
    }
  }

  if (!config.disableCompletions) {
    const strategy = config.completionStrategy ?? "rank"

    proxy.getCompletionsAtPosition = (
      fileName: string,
      position: number,
      options?: ts.GetCompletionsAtPositionOptions,
    ): ts.WithMetadata<ts.CompletionInfo> | undefined => {
      const original = info.languageService.getCompletionsAtPosition(
        fileName,
        position,
        options,
      )
      if (!original || !fileName.endsWith(".tsx")) return original

      try {
        const program = info.languageService.getProgram()
        const sourceFile = program?.getSourceFile(fileName)
        if (!sourceFile) return original

        const parentTag = getParentTagAtPosition(sourceFile, position, tsModule)
        if (!parentTag) return original

        const processed =
          strategy === "filter"
            ? filterCompletionsByContext(
                original.entries,
                parentTag,
                registry,
                tsModule,
              )
            : rankCompletionsByContext(
                original.entries,
                parentTag,
                registry,
                tsModule,
              )

        log(
          `Completions: ${strategy} mode, ${original.entries.length} → ${processed.length} entries`,
        )
        return { ...original, entries: processed }
      } catch (e) {
        log(
          `ERROR in completions for ${fileName}: ${e instanceof Error ? e.message : String(e)}. Falling back to baseline.`,
        )
        return original
      }
    }
  }

  return proxy
}
