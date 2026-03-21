import { execSync } from "node:child_process"
import { mkdtempSync, rmSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { beforeEach, describe, expect, it } from "vitest"
import { introspectProgram } from "@/cli/completion/introspect.js"
import { generateZshCompletion } from "@/cli/completion/zsh.js"
import { createProgram } from "@/cli/program.js"

describe("completion zsh", () => {
  let script: string

  beforeEach(() => {
    const program = createProgram()
    const tree = introspectProgram(program)
    script = generateZshCompletion(tree)
  })

  it("starts with #compdef and ends with entry point call", () => {
    expect(script.startsWith("#compdef flink-reactor")).toBe(true)
    expect(script.trimEnd().endsWith('_flink_reactor "$@"')).toBe(true)
  })

  it("contains all top-level command names", () => {
    const commands = [
      "new",
      "doctor",
      "install",
      "generate",
      "synth",
      "validate",
      "graph",
      "dev",
      "deploy",
      "cluster",
      "schema",
      "upgrade",
      "stop",
      "resume",
      "savepoint",
      "status",
      "completion",
    ]
    for (const cmd of commands) {
      expect(script).toContain(`'${cmd}:`)
    }
  })

  it("contains the generate alias 'g'", () => {
    expect(script).toContain("generate|g)")
    expect(script).toContain("'g:Generate a new component'")
  })

  it("contains cluster subcommands", () => {
    const subs = ["up", "down", "seed", "status", "submit"]
    for (const sub of subs) {
      expect(script).toContain(`'${sub}:`)
    }
  })

  it("contains generate subcommands", () => {
    const subs = ["pipeline", "schema", "env", "pattern", "app", "package"]
    for (const sub of subs) {
      expect(script).toContain(`'${sub}:`)
    }
  })

  it("contains all template names from TEMPLATE_FACTORIES", () => {
    const templates = [
      "starter",
      "minimal",
      "cdc-lakehouse",
      "realtime-analytics",
      "monorepo",
      "ecommerce",
      "ride-sharing",
      "grocery-delivery",
      "banking",
      "iot-factory",
      "lakehouse-ingestion",
      "lakehouse-analytics",
    ]
    for (const t of templates) {
      expect(script).toContain(t)
    }
  })

  it("contains package manager choices", () => {
    expect(script).toContain("(pnpm npm yarn)")
  })

  it("contains Flink version choices", () => {
    expect(script).toContain("(1.20 2.0 2.1 2.2)")
  })

  it("contains graph format choices", () => {
    expect(script).toContain("(ascii dot svg)")
  })

  it("contains cluster seed --only choices", () => {
    expect(script).toContain("(streaming batch cdc cdc-kafka)")
  })

  it("contains cluster domain choices", () => {
    expect(script).toContain("(ecommerce iot all)")
  })

  it("contains install method choices", () => {
    expect(script).toContain("(docker homebrew binary)")
  })

  it("contains generate pipeline template choices", () => {
    expect(script).toContain("(blank kafka jdbc)")
  })

  it("uses _files for sql-file arguments", () => {
    expect(script).toContain('_files -g "*.sql"')
  })

  it("uses _files for tsx path arguments", () => {
    expect(script).toContain('_files -g "*.tsx"')
  })

  it("uses _files -/ for outdir", () => {
    expect(script).toContain("_files -/")
  })

  it("handles negated options correctly (--no-git, not --no-no-git)", () => {
    expect(script).toContain("--no-git")
    expect(script).toContain("--no-install")
    expect(script).not.toContain("--no-no-git")
    expect(script).not.toContain("--no-no-install")
  })

  it("produces valid zsh syntax", () => {
    let tempDir: string | undefined
    try {
      tempDir = mkdtempSync(join(tmpdir(), "fr-zsh-test-"))
      const scriptPath = join(tempDir, "_flink-reactor")
      writeFileSync(scriptPath, script, "utf-8")
      execSync(`zsh -n "${scriptPath}"`, { stdio: "pipe" })
    } finally {
      if (tempDir) rmSync(tempDir, { recursive: true, force: true })
    }
  })

  it("matches snapshot", () => {
    expect(script).toMatchSnapshot()
  })
})

describe("introspectProgram", () => {
  it("returns all top-level commands", () => {
    const program = createProgram()
    const tree = introspectProgram(program)
    const names = tree.subcommands.map((c) => c.name)
    expect(names).toContain("new")
    expect(names).toContain("cluster")
    expect(names).toContain("generate")
    expect(names).toContain("completion")
  })

  it("captures choices on options", () => {
    const program = createProgram()
    const tree = introspectProgram(program)
    const newCmd = tree.subcommands.find((c) => c.name === "new")
    const templateOpt = newCmd?.options.find((o) => o.long === "template")
    expect(templateOpt?.choices).toBeTruthy()
    expect(templateOpt?.choices?.length).toBeGreaterThanOrEqual(12)
    expect(templateOpt?.choices).toContain("starter")
  })

  it("captures subcommands for cluster", () => {
    const program = createProgram()
    const tree = introspectProgram(program)
    const cluster = tree.subcommands.find((c) => c.name === "cluster")
    const subNames = cluster?.subcommands.map((s) => s.name)
    expect(subNames).toContain("up")
    expect(subNames).toContain("down")
    expect(subNames).toContain("seed")
    expect(subNames).toContain("status")
    expect(subNames).toContain("submit")
  })

  it("captures aliases for generate", () => {
    const program = createProgram()
    const tree = introspectProgram(program)
    const gen = tree.subcommands.find((c) => c.name === "generate")
    expect(gen?.aliases).toContain("g")
  })
})
