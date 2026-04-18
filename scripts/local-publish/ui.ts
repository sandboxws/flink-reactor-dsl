import * as clack from "@clack/prompts"
import pc from "picocolors"

export function intro(title: string): void {
  clack.intro(pc.bgCyan(pc.black(` ${title} `)))
}

export function outro(message: string): void {
  clack.outro(pc.green(message))
}

export function info(message: string): void {
  clack.log.info(message)
}

export function success(message: string): void {
  clack.log.success(pc.green(message))
}

export function warn(message: string): void {
  clack.log.warn(pc.yellow(message))
}

export function error(message: string): void {
  clack.log.error(pc.red(message))
}

export function spinner(): ReturnType<typeof clack.spinner> {
  return clack.spinner()
}

export { pc }
