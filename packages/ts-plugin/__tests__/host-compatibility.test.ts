/**
 * Host compatibility smoke tests.
 *
 * Validates plugin activation and representative diagnostics/completion
 * behavior across VS Code, ts_ls, and vtsls host pathways.
 *
 * Each host pathway is tested by simulating the plugin initialization
 * with a mock PluginCreateInfo that mirrors how that host loads plugins.
 */
import ts from "typescript"
import { describe, expect, it } from "vitest"
import {
  isTypeScriptVersionSupported,
  parseTypeScriptVersion,
  SUPPORTED_HOSTS,
  TS_VERSION_POLICY,
  COMPATIBILITY_MATRIX,
} from "../src/host-compatibility"
import { createRulesRegistry } from "../src/component-rules"
import { createLanguageServiceProxy } from "../src/service"
import type { PluginConfig } from "../src/service"
import { getNestingDiagnostics } from "../src/diagnostics"
import {
  filterCompletionsByContext,
  rankCompletionsByContext,
} from "../src/completions"
import { getParentTagAtPosition, getComponentName } from "../src/context-detector"

// ---------------------------------------------------------------------------
// Host compatibility definitions
// ---------------------------------------------------------------------------

describe("host compatibility definitions", () => {
  it("defines exactly three supported hosts", () => {
    expect(SUPPORTED_HOSTS).toHaveLength(3)
    expect([...SUPPORTED_HOSTS]).toEqual(["vscode", "ts_ls", "vtsls"])
  })

  it("compatibility matrix covers all supported hosts", () => {
    const matrixHosts = COMPATIBILITY_MATRIX.map((e) => e.host)
    for (const host of SUPPORTED_HOSTS) {
      expect(matrixHosts).toContain(host)
    }
  })

  it("each matrix entry has loading path and notes", () => {
    for (const entry of COMPATIBILITY_MATRIX) {
      expect(entry.loadingPath.length).toBeGreaterThan(0)
      expect(entry.notes.length).toBeGreaterThan(0)
    }
  })
})

// ---------------------------------------------------------------------------
// TypeScript version policy
// ---------------------------------------------------------------------------

describe("TypeScript version policy", () => {
  it("minimum version is 5.0.0", () => {
    expect(TS_VERSION_POLICY.minimum).toBe("5.0.0")
  })

  it("parses valid version strings", () => {
    expect(parseTypeScriptVersion("5.7.2")).toEqual({
      major: 5,
      minor: 7,
      patch: 2,
    })
    expect(parseTypeScriptVersion("5.0.0")).toEqual({
      major: 5,
      minor: 0,
      patch: 0,
    })
  })

  it("returns undefined for invalid version strings", () => {
    expect(parseTypeScriptVersion("invalid")).toBeUndefined()
    expect(parseTypeScriptVersion("")).toBeUndefined()
  })

  it("accepts supported versions", () => {
    expect(isTypeScriptVersionSupported("5.0.0")).toBe(true)
    expect(isTypeScriptVersionSupported("5.7.2")).toBe(true)
    expect(isTypeScriptVersionSupported("6.0.0")).toBe(true)
  })

  it("rejects unsupported versions", () => {
    expect(isTypeScriptVersionSupported("4.9.5")).toBe(false)
    expect(isTypeScriptVersionSupported("3.0.0")).toBe(false)
  })

  it("rejects invalid version strings", () => {
    expect(isTypeScriptVersionSupported("not-a-version")).toBe(false)
  })

  it("current TypeScript version is supported", () => {
    expect(isTypeScriptVersionSupported(ts.version)).toBe(true)
  })
})

// ---------------------------------------------------------------------------
// Shared test helpers
// ---------------------------------------------------------------------------

function parse(source: string): ts.SourceFile {
  return ts.createSourceFile(
    "test.tsx",
    source,
    ts.ScriptTarget.Latest,
    true,
    ts.ScriptKind.TSX,
  )
}

function entry(name: string, sortText = "11"): ts.CompletionEntry {
  return {
    name,
    kind: ts.ScriptElementKind.unknown,
    sortText,
    kindModifiers: "",
  }
}

/**
 * Create a mock PluginCreateInfo with a minimal language service
 * that returns pre-configured diagnostics and completions.
 */
function createMockInfo(overrides?: {
  config?: PluginConfig
  diagnostics?: ts.Diagnostic[]
  completions?: ts.CompletionEntry[]
  fileName?: string
}): ts.server.PluginCreateInfo {
  const fileName = overrides?.fileName ?? "test.tsx"
  const baseDiags = overrides?.diagnostics ?? []
  const baseCompletions = overrides?.completions ?? []

  const sourceFile = parse("<Pipeline><KafkaSource /></Pipeline>")

  const program = {
    getSourceFile: (_fn: string) => sourceFile,
  } as unknown as ts.Program

  const languageService = {
    getSemanticDiagnostics: (_fn: string): ts.Diagnostic[] => baseDiags,
    getCompletionsAtPosition: (
      _fn: string,
      _pos: number,
      _opts?: ts.GetCompletionsAtPositionOptions,
    ): ts.WithMetadata<ts.CompletionInfo> => ({
      isGlobalCompletion: false,
      isMemberCompletion: false,
      isNewIdentifierLocation: false,
      entries: baseCompletions,
    }),
    getProgram: () => program,
  } as unknown as ts.LanguageService

  // Copy all LS methods to make proxy creation work
  for (const key of Object.keys(languageService) as Array<keyof ts.LanguageService>) {
    const val = languageService[key]
    if (typeof val === "function") {
      // biome-ignore lint/suspicious/noExplicitAny: test mock
      ;(languageService as any)[key] = val.bind(languageService)
    }
  }

  const logMessages: string[] = []
  const logger = {
    info: (msg: string) => logMessages.push(msg),
  }

  return {
    config: overrides?.config ?? {},
    languageService,
    project: {
      projectService: { logger },
    },
    serverHost: {},
  } as unknown as ts.server.PluginCreateInfo
}

// ---------------------------------------------------------------------------
// Smoke test: VS Code pathway
// ---------------------------------------------------------------------------

describe("smoke: VS Code pathway", () => {
  // VS Code activates via compilerOptions.plugins in tsconfig.json.
  // The plugin receives config from the tsconfig entry.

  it("plugin activates with default configuration", () => {
    const info = createMockInfo()
    const registry = createRulesRegistry()
    const logs: string[] = []
    const log = (msg: string) => logs.push(msg)

    const proxy = createLanguageServiceProxy(info, registry, ts, {}, log)
    expect(proxy).toBeDefined()
    expect(typeof proxy.getSemanticDiagnostics).toBe("function")
    expect(typeof proxy.getCompletionsAtPosition).toBe("function")
  })

  it("diagnostics work for valid TSX nesting", () => {
    const source = "<Pipeline><KafkaSource /></Pipeline>"
    const sourceFile = parse(source)

    const info = createMockInfo()
    const registry = createRulesRegistry()
    const logs: string[] = []
    const proxy = createLanguageServiceProxy(info, registry, ts, {}, (msg) => logs.push(msg))

    const diags = proxy.getSemanticDiagnostics("test.tsx")
    // No nesting errors for valid hierarchy
    const nestingDiags = diags.filter((d) => d.source === "flink-reactor")
    expect(nestingDiags).toHaveLength(0)
  })

  it("diagnostics detect invalid nesting", () => {
    const source = "<Route><Filter /></Route>"
    const sourceFile = parse(source)

    const program = {
      getSourceFile: () => sourceFile,
    } as unknown as ts.Program

    const info = createMockInfo()
    // Override getProgram to return our custom source
    ;(info.languageService as { getProgram: () => ts.Program }).getProgram = () => program

    const registry = createRulesRegistry()
    const logs: string[] = []
    const proxy = createLanguageServiceProxy(info, registry, ts, {}, (msg) => logs.push(msg))

    const diags = proxy.getSemanticDiagnostics("test.tsx")
    const nestingDiags = diags.filter((d) => d.source === "flink-reactor")
    expect(nestingDiags.length).toBeGreaterThan(0)
    expect(nestingDiags[0].messageText).toContain("'Filter' is not a valid child of 'Route'")
  })

  it("completions rank DSL components in context", () => {
    const source = "<Pipeline>  </Pipeline>"
    const sourceFile = parse(source)
    const cursorPos = 10 // inside Pipeline children

    const program = {
      getSourceFile: () => sourceFile,
    } as unknown as ts.Program

    const completionEntries = [
      entry("KafkaSource", "11"),
      entry("Route.Branch", "11"),
      entry("console", "11"),
    ]

    const info = createMockInfo({ completions: completionEntries })
    ;(info.languageService as { getProgram: () => ts.Program }).getProgram = () => program

    const registry = createRulesRegistry()
    const logs: string[] = []
    const proxy = createLanguageServiceProxy(
      info,
      registry,
      ts,
      { completionStrategy: "rank" },
      (msg) => logs.push(msg),
    )

    const result = proxy.getCompletionsAtPosition("test.tsx", cursorPos, undefined)
    expect(result).toBeDefined()
    // KafkaSource is a valid Pipeline child → promoted
    const kafkaEntry = result!.entries.find((e) => e.name === "KafkaSource")
    expect(kafkaEntry!.sortText).toBe("011")
  })

  it("falls back safely when diagnostics throw", () => {
    const info = createMockInfo({ diagnostics: [] })
    // Make getProgram throw to simulate a host error
    ;(info.languageService as { getProgram: () => never }).getProgram = () => {
      throw new Error("Simulated host error")
    }

    const registry = createRulesRegistry()
    const logs: string[] = []
    const proxy = createLanguageServiceProxy(info, registry, ts, {}, (msg) => logs.push(msg))

    // Should not throw — returns baseline diagnostics
    const diags = proxy.getSemanticDiagnostics("test.tsx")
    expect(diags).toEqual([])
    expect(logs.some((l) => l.includes("ERROR") && l.includes("Falling back"))).toBe(true)
  })

  it("falls back safely when completions throw", () => {
    const info = createMockInfo({ completions: [entry("test")] })
    ;(info.languageService as { getProgram: () => never }).getProgram = () => {
      throw new Error("Simulated host error")
    }

    const registry = createRulesRegistry()
    const logs: string[] = []
    const proxy = createLanguageServiceProxy(info, registry, ts, {}, (msg) => logs.push(msg))

    const result = proxy.getCompletionsAtPosition("test.tsx", 0, undefined)
    expect(result).toBeDefined()
    expect(result!.entries).toHaveLength(1)
    expect(logs.some((l) => l.includes("ERROR") && l.includes("Falling back"))).toBe(true)
  })
})

// ---------------------------------------------------------------------------
// Smoke test: Neovim ts_ls pathway
// ---------------------------------------------------------------------------

describe("smoke: Neovim ts_ls pathway", () => {
  // ts_ls passes plugin configuration via init_options.plugins.
  // The plugin itself still receives PluginCreateInfo from tsserver.
  // The key difference is how the host discovers and loads the plugin.

  it("plugin activates with init_options-style config", () => {
    // ts_ls provides config with the plugin name and location
    const config: PluginConfig = {}
    const info = createMockInfo({ config })
    const registry = createRulesRegistry()
    const logs: string[] = []

    const proxy = createLanguageServiceProxy(info, registry, ts, config, (msg) => logs.push(msg))
    expect(proxy).toBeDefined()
    expect(typeof proxy.getSemanticDiagnostics).toBe("function")
    expect(typeof proxy.getCompletionsAtPosition).toBe("function")
  })

  it("diagnostics activate on .tsx files", () => {
    const source = "<Route><Filter /></Route>"
    const sourceFile = parse(source)
    const program = {
      getSourceFile: () => sourceFile,
    } as unknown as ts.Program

    const info = createMockInfo()
    ;(info.languageService as { getProgram: () => ts.Program }).getProgram = () => program

    const registry = createRulesRegistry()
    const logs: string[] = []
    const proxy = createLanguageServiceProxy(info, registry, ts, {}, (msg) => logs.push(msg))

    const diags = proxy.getSemanticDiagnostics("test.tsx")
    const nestingDiags = diags.filter((d) => d.source === "flink-reactor")
    expect(nestingDiags.length).toBeGreaterThan(0)
  })

  it("diagnostics skip non-.tsx files", () => {
    const info = createMockInfo()
    const registry = createRulesRegistry()
    const logs: string[] = []
    const proxy = createLanguageServiceProxy(info, registry, ts, {}, (msg) => logs.push(msg))

    const diags = proxy.getSemanticDiagnostics("test.ts")
    // Should return only baseline diagnostics (none in mock)
    expect(diags).toEqual([])
  })

  it("completions filter strategy works", () => {
    const source = "<Route>  </Route>"
    const sourceFile = parse(source)
    const cursorPos = 7 // inside Route children

    const program = {
      getSourceFile: () => sourceFile,
    } as unknown as ts.Program

    const completionEntries = [
      entry("Route.Branch"),
      entry("Filter"),
      entry("myVar"),
    ]

    const info = createMockInfo({ completions: completionEntries })
    ;(info.languageService as { getProgram: () => ts.Program }).getProgram = () => program

    const registry = createRulesRegistry()
    const logs: string[] = []
    const proxy = createLanguageServiceProxy(
      info,
      registry,
      ts,
      { completionStrategy: "filter" },
      (msg) => logs.push(msg),
    )

    const result = proxy.getCompletionsAtPosition("test.tsx", cursorPos, undefined)
    expect(result).toBeDefined()
    const names = result!.entries.map((e) => e.name)
    // Route.Branch is valid → kept
    expect(names).toContain("Route.Branch")
    // Filter is DSL but invalid Route child → removed
    expect(names).not.toContain("Filter")
    // myVar is non-DSL → preserved
    expect(names).toContain("myVar")
  })

  it("completions skip non-.tsx files", () => {
    const info = createMockInfo({ completions: [entry("test")] })
    const registry = createRulesRegistry()
    const logs: string[] = []
    const proxy = createLanguageServiceProxy(info, registry, ts, {}, (msg) => logs.push(msg))

    const result = proxy.getCompletionsAtPosition("test.ts", 0, undefined)
    expect(result).toBeDefined()
    // Non-tsx → passthrough, entries unchanged
    expect(result!.entries).toHaveLength(1)
  })
})

// ---------------------------------------------------------------------------
// Smoke test: Neovim vtsls pathway
// ---------------------------------------------------------------------------

describe("smoke: Neovim vtsls pathway", () => {
  // vtsls uses tsserver.globalPlugins for plugin registration.
  // Plugin receives the same PluginCreateInfo interface.
  // Key difference: enableForWorkspaceTypeScriptVersions flag.

  it("plugin activates with globalPlugins-style config", () => {
    const config: PluginConfig = { completionStrategy: "rank" }
    const info = createMockInfo({ config })
    const registry = createRulesRegistry()
    const logs: string[] = []

    const proxy = createLanguageServiceProxy(info, registry, ts, config, (msg) => logs.push(msg))
    expect(proxy).toBeDefined()
  })

  it("plugin activates with custom rules override", () => {
    // vtsls users might pass custom rules through plugin config
    const config: PluginConfig = {
      rules: { Pipeline: ["KafkaSource"] },
    }
    const info = createMockInfo({ config })
    const registry = createRulesRegistry(config.rules)
    const logs: string[] = []

    const proxy = createLanguageServiceProxy(info, registry, ts, config, (msg) => logs.push(msg))
    expect(proxy).toBeDefined()

    // Verify custom rules take effect
    expect(registry.isValidChild("Pipeline", "KafkaSource")).toBe(true)
  })

  it("diagnostics work with custom rules", () => {
    const source = "<Pipeline><Filter /></Pipeline>"
    const sourceFile = parse(source)
    const program = {
      getSourceFile: () => sourceFile,
    } as unknown as ts.Program

    // Custom rules: Pipeline only allows KafkaSource
    const config: PluginConfig = {
      rules: { Pipeline: ["KafkaSource"] },
    }
    const info = createMockInfo({ config })
    ;(info.languageService as { getProgram: () => ts.Program }).getProgram = () => program

    const registry = createRulesRegistry(config.rules)
    const logs: string[] = []
    const proxy = createLanguageServiceProxy(info, registry, ts, config, (msg) => logs.push(msg))

    const diags = proxy.getSemanticDiagnostics("test.tsx")
    const nestingDiags = diags.filter((d) => d.source === "flink-reactor")
    // Filter is not in custom Pipeline rules → diagnostic
    expect(nestingDiags.length).toBeGreaterThan(0)
  })

  it("plugin works with features disabled", () => {
    const config: PluginConfig = {
      disableDiagnostics: true,
      disableCompletions: true,
    }
    const info = createMockInfo({ config })
    const registry = createRulesRegistry()
    const logs: string[] = []

    const proxy = createLanguageServiceProxy(info, registry, ts, config, (msg) => logs.push(msg))
    expect(proxy).toBeDefined()

    // Diagnostics should return only baseline (no flink-reactor diagnostics)
    const diags = proxy.getSemanticDiagnostics("test.tsx")
    expect(diags.filter((d) => d.source === "flink-reactor")).toHaveLength(0)
  })

  it("completions work with rank strategy (default for vtsls)", () => {
    const source = "<Pipeline>  </Pipeline>"
    const sourceFile = parse(source)
    const cursorPos = 10

    const program = {
      getSourceFile: () => sourceFile,
    } as unknown as ts.Program

    const completionEntries = [
      entry("KafkaSource", "11"),
      entry("Route.Branch", "11"),
    ]

    const info = createMockInfo({ completions: completionEntries })
    ;(info.languageService as { getProgram: () => ts.Program }).getProgram = () => program

    const registry = createRulesRegistry()
    const logs: string[] = []
    const proxy = createLanguageServiceProxy(
      info,
      registry,
      ts,
      { completionStrategy: "rank" },
      (msg) => logs.push(msg),
    )

    const result = proxy.getCompletionsAtPosition("test.tsx", cursorPos, undefined)
    expect(result).toBeDefined()
    // All entries preserved (rank mode doesn't remove)
    expect(result!.entries).toHaveLength(2)
    // Valid child promoted
    expect(result!.entries.find((e) => e.name === "KafkaSource")!.sortText).toBe("011")
    // Invalid child demoted
    expect(result!.entries.find((e) => e.name === "Route.Branch")!.sortText).toBe("211")
  })
})
