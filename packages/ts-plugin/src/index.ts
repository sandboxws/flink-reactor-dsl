/**
 * TypeScript language service plugin for flink-reactor.
 *
 * Provides JSX nesting diagnostics and context-aware completions
 * for FlinkReactor DSL components.
 *
 * Activated via tsconfig.json:
 * {
 *   "compilerOptions": {
 *     "plugins": [{ "name": "@flink-reactor/ts-plugin" }]
 *   }
 * }
 */
import type ts from "typescript"
import { createRulesRegistry } from "./component-rules"
import {
  isTypeScriptVersionSupported,
  TS_VERSION_POLICY,
} from "./host-compatibility"
import { createLanguageServiceProxy, type PluginConfig } from "./service"

const VALID_STRATEGIES = ["rank", "filter"] as const

function init(modules: { typescript: typeof ts }): ts.server.PluginModule {
  const tsModule = modules.typescript

  function create(info: ts.server.PluginCreateInfo): ts.LanguageService {
    const config: PluginConfig = info.config ?? {}
    const registry = createRulesRegistry(config.rules)
    const log = (msg: string) => {
      info.project.projectService.logger.info(`[flink-reactor] ${msg}`)
    }

    // --- Startup checks ---
    const tsVersion = tsModule.version
    log(`TypeScript version: ${tsVersion}`)

    if (!isTypeScriptVersionSupported(tsVersion)) {
      log(
        `WARNING: TypeScript ${tsVersion} is below minimum supported version ${TS_VERSION_POLICY.minimum}. Plugin behavior may be degraded.`,
      )
    }

    // Validate completion strategy
    if (
      config.completionStrategy &&
      !VALID_STRATEGIES.includes(config.completionStrategy)
    ) {
      log(
        `Invalid completionStrategy "${config.completionStrategy}", falling back to "rank"`,
      )
      config.completionStrategy = "rank"
    }

    // Report active features
    const features = {
      diagnostics: !config.disableDiagnostics,
      completions: !config.disableCompletions,
      completionStrategy: config.completionStrategy ?? "rank",
      customRules: config.rules ? Object.keys(config.rules).length : 0,
    }
    log(
      `Activation: diagnostics=${features.diagnostics}, completions=${features.completions}, strategy=${features.completionStrategy}, customRules=${features.customRules}`,
    )

    log("Plugin initialized successfully")

    return createLanguageServiceProxy(info, registry, tsModule, config, log)
  }

  return { create }
}

export = init
