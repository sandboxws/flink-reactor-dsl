/**
 * TypeScript language service plugin for flink-reactor.
 *
 * Provides context-aware JSX completion filtering and invalid nesting
 * diagnostics based on the flink-reactor component hierarchy.
 *
 * Activated via tsconfig.json:
 * {
 *   "compilerOptions": {
 *     "plugins": [{ "name": "@flink-reactor/ts-plugin" }]
 *   }
 * }
 */
import type ts from 'typescript';
import { createRulesRegistry } from './component-rules';
import { findParentJsxComponent } from './context-detector';
import { filterCompletions } from './completion-filter';
import { getNestingDiagnostics } from './diagnostics';

interface PluginConfig {
  /** Override or extend component hierarchy rules */
  rules?: Record<string, string[] | '*'>;
  /** Disable completion filtering (keep only diagnostics) */
  disableCompletionFilter?: boolean;
  /** Disable nesting diagnostics (keep only completion filter) */
  disableDiagnostics?: boolean;
}

function init(modules: { typescript: typeof ts }): ts.server.PluginModule {
  const tsModule = modules.typescript;

  function create(info: ts.server.PluginCreateInfo): ts.LanguageService {
    const config: PluginConfig = info.config ?? {};
    const registry = createRulesRegistry(config.rules);
    const log = (msg: string) => {
      info.project.projectService.logger.info(`[flink-reactor] ${msg}`);
    };

    log('Plugin initialized');

    // Create a proxy that delegates everything to the original language service
    const proxy = Object.create(null) as ts.LanguageService;
    for (const key of Object.keys(info.languageService) as Array<keyof ts.LanguageService>) {
      const original = info.languageService[key];
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (proxy as any)[key] = typeof original === 'function' ? original.bind(info.languageService) : original;
    }

    // Override: getCompletionsAtPosition
    if (!config.disableCompletionFilter) {
      proxy.getCompletionsAtPosition = (
        fileName: string,
        position: number,
        options: ts.GetCompletionsAtPositionOptions | undefined,
      ): ts.CompletionInfo | undefined => {
        const original = info.languageService.getCompletionsAtPosition(
          fileName,
          position,
          options,
        );
        if (!original) return original;
        if (!fileName.endsWith('.tsx')) return original;

        const program = info.languageService.getProgram();
        const sourceFile = program?.getSourceFile(fileName);
        if (!sourceFile) return original;

        const parentName = findParentJsxComponent(sourceFile, position, tsModule);
        return filterCompletions(original, parentName, registry);
      };
    }

    // Override: getSemanticDiagnostics
    if (!config.disableDiagnostics) {
      proxy.getSemanticDiagnostics = (fileName: string): ts.Diagnostic[] => {
        const original = info.languageService.getSemanticDiagnostics(fileName);
        if (!fileName.endsWith('.tsx')) return original;

        const program = info.languageService.getProgram();
        const sourceFile = program?.getSourceFile(fileName);
        if (!sourceFile) return original;

        const nestingDiags = getNestingDiagnostics(sourceFile, registry, tsModule);
        return [...original, ...nestingDiags];
      };
    }

    return proxy;
  }

  return { create };
}

export = init;
