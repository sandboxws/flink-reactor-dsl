/**
 * TypeScript language service plugin for flink-reactor.
 *
 * Provides nesting diagnostics that warn when JSX children are placed
 * inside invalid parents (e.g., `<Filter />` inside `<Route>`).
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
import { type PluginConfig, createLanguageServiceProxy } from "./service"

function init(modules: { typescript: typeof ts }): ts.server.PluginModule {
  const tsModule = modules.typescript

  function create(info: ts.server.PluginCreateInfo): ts.LanguageService {
    const config: PluginConfig = info.config ?? {}
    const registry = createRulesRegistry(config.rules)
    const log = (msg: string) => {
      info.project.projectService.logger.info(`[flink-reactor] ${msg}`)
    }

    log("Plugin initialized")

    return createLanguageServiceProxy(info, registry, tsModule, config, log)
  }

  return { create }
}

export = init
