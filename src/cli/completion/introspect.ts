import type { Command, Option } from "commander"

// ── Types ────────────────────────────────────────────────────────────

export interface IntrospectedOption {
  short: string | null
  long: string
  description: string
  choices: string[] | null
  expectsValue: boolean
  isNegated: boolean
  defaultValue: unknown
}

export interface IntrospectedArgument {
  name: string
  description: string
  required: boolean
  completionHint: "file" | "directory" | "sql-file" | "tsx-file" | null
}

export interface IntrospectedCommand {
  name: string
  aliases: string[]
  description: string
  options: IntrospectedOption[]
  args: IntrospectedArgument[]
  subcommands: IntrospectedCommand[]
}

// ── File-completion heuristics ───────────────────────────────────────

function inferCompletionHint(
  name: string,
  description: string,
): IntrospectedArgument["completionHint"] {
  const lower = `${name} ${description}`.toLowerCase()
  if (lower.includes("sql-file") || lower.includes(".sql")) return "sql-file"
  if (lower.includes(".tsx") || lower.includes("tsx file")) return "tsx-file"
  if (lower.includes("dir") || lower.includes("directory")) return "directory"
  if (lower.includes("path") || lower.includes("file")) return "file"
  return null
}

function inferOptionCompletionHint(
  flags: string,
  description: string,
): IntrospectedArgument["completionHint"] {
  const lower = `${flags} ${description}`.toLowerCase()
  if (lower.includes("outdir") || lower.includes("output directory"))
    return "directory"
  if (
    lower.includes("<path>") ||
    lower.includes("<file>") ||
    lower.includes("file")
  )
    return "file"
  if (lower.includes("<dir>")) return "directory"
  return null
}

// ── Introspection ────────────────────────────────────────────────────

function introspectOption(opt: Option): IntrospectedOption {
  const flags = opt.flags
  const shortMatch = flags.match(/^-(\w)/)
  const longMatch = flags.match(/--([\w-]+)/)

  return {
    short: shortMatch ? shortMatch[1] : null,
    long: longMatch ? longMatch[1] : "",
    description: opt.description ?? "",
    choices: (opt as Option & { argChoices?: string[] }).argChoices ?? null,
    expectsValue: opt.required || opt.optional || false,
    isNegated: opt.negate ?? false,
    defaultValue: opt.defaultValue,
  }
}

function introspectCommand(cmd: Command): IntrospectedCommand {
  const options = cmd.options.map(introspectOption)

  // Commander stores registered arguments
  const registeredArgs =
    (
      cmd as Command & {
        registeredArguments?: Array<{
          name: () => string
          description: string
          required: boolean
        }>
      }
    ).registeredArguments ??
    (
      cmd as Command & {
        _args?: Array<{
          name: () => string
          description: string
          required: boolean
        }>
      }
    )._args ??
    []

  const args: IntrospectedArgument[] = registeredArgs.map(
    (arg: { name: () => string; description: string; required: boolean }) => {
      const name =
        typeof arg.name === "function"
          ? arg.name()
          : (arg as unknown as { _name: string })._name
      const desc = arg.description ?? ""
      return {
        name,
        description: desc,
        required: arg.required,
        completionHint: inferCompletionHint(name, desc),
      }
    },
  )

  const subcommands = cmd.commands.map(introspectCommand)

  return {
    name: cmd.name(),
    aliases: cmd.aliases(),
    description: cmd.description(),
    options,
    args,
    subcommands,
  }
}

export function introspectProgram(program: Command): IntrospectedCommand {
  return introspectCommand(program)
}

export { inferOptionCompletionHint }
