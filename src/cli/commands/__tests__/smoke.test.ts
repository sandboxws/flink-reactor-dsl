import { Command } from "commander"
import { describe, expect, it } from "vitest"
import { registerDeployCommand } from "@/cli/commands/deploy.js"
import { registerDevCommand } from "@/cli/commands/dev.js"
import { registerDownCommand } from "@/cli/commands/down.js"
import { registerResumeCommand } from "@/cli/commands/resume.js"
import { registerSavepointCommand } from "@/cli/commands/savepoint.js"
import { registerStopCommand } from "@/cli/commands/stop.js"

// Smoke-level coverage for CLI commands that do real I/O (kubectl,
// docker, watchers) and aren't easy to test end-to-end. We assert the
// CLI surface: command name, description present, expected option
// names registered. Catches regressions like accidental option
// renames or removed flags without spinning up a runtime.

type RegisterFn = (program: Command) => void

function freshProgram(): Command {
  return new Command().exitOverride().configureOutput({
    // Silence Commander's stderr writes during tests.
    writeOut: () => {},
    writeErr: () => {},
  })
}

function registeredCommand(register: RegisterFn, name: string): Command {
  const program = freshProgram()
  register(program)
  const cmd = program.commands.find((c) => c.name() === name)
  if (!cmd) {
    throw new Error(
      `expected '${name}' to be registered; got [${program.commands.map((c) => c.name()).join(", ")}]`,
    )
  }
  return cmd
}

function optionFlagSet(cmd: Command): Set<string> {
  const flags = new Set<string>()
  for (const opt of cmd.options) {
    if (opt.short) flags.add(opt.short)
    if (opt.long) flags.add(opt.long)
  }
  return flags
}

describe("CLI command registration smoke tests", () => {
  it("deploy: registers with expected options", () => {
    const cmd = registeredCommand(registerDeployCommand, "deploy")
    expect(cmd.description()).toMatch(/deploy/i)
    const opts = optionFlagSet(cmd)
    expect(opts).toContain("--pipeline")
    expect(opts).toContain("--env")
    expect(opts).toContain("--dry-run")
    expect(opts).toContain("--runtime")
  })

  it("dev: registers with expected options", () => {
    const cmd = registeredCommand(registerDevCommand, "dev")
    expect(cmd.description()).toMatch(/dev/i)
    const opts = optionFlagSet(cmd)
    expect(opts).toContain("--pipeline")
    expect(opts).toContain("--env")
    expect(opts).toContain("--port")
  })

  it("down: registers", () => {
    const cmd = registeredCommand(registerDownCommand, "down")
    expect(cmd.description().length).toBeGreaterThan(0)
  })

  it("resume: registers", () => {
    const cmd = registeredCommand(registerResumeCommand, "resume")
    expect(cmd.description().length).toBeGreaterThan(0)
  })

  it("savepoint: registers", () => {
    const cmd = registeredCommand(registerSavepointCommand, "savepoint")
    expect(cmd.description().length).toBeGreaterThan(0)
  })

  it("stop: registers", () => {
    const cmd = registeredCommand(registerStopCommand, "stop")
    expect(cmd.description().length).toBeGreaterThan(0)
  })

  it("multiple commands coexist on the same program", () => {
    const program = freshProgram()
    registerDeployCommand(program)
    registerDevCommand(program)
    registerDownCommand(program)
    registerResumeCommand(program)
    registerSavepointCommand(program)
    registerStopCommand(program)
    const names = program.commands.map((c) => c.name())
    expect(names).toEqual(
      expect.arrayContaining([
        "deploy",
        "dev",
        "down",
        "resume",
        "savepoint",
        "stop",
      ]),
    )
  })
})
