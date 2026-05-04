// Cross-platform helper for opening URLs in the user's default browser.
// Extracted from dev.ts so `fr cluster open <target>` and the dev REPL's
// "f" shortcut share the same implementation.

import { execSync } from "node:child_process"
import pc from "picocolors"

export function openUrl(
  url: string | null,
  label: string,
  hint?: string,
): void {
  if (!url) {
    if (hint) console.log(pc.yellow(`\n${hint}`))
    return
  }

  console.log(pc.dim(`\nOpening ${label} (${url})...`))

  try {
    const platform = process.platform
    const cmd =
      platform === "darwin"
        ? "open"
        : platform === "win32"
          ? "start"
          : "xdg-open"
    execSync(`${cmd} ${url}`, { stdio: "ignore" })
  } catch {
    console.log(pc.dim(`Open manually: ${url}`))
  }
}
