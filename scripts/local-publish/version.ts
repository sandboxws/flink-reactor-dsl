import { readFileSync } from "node:fs"
import { join } from "node:path"

export interface VersionInfo {
  version: string
  distTag: string | undefined
}

export function readRootVersion(projectRoot: string): VersionInfo {
  const pkg = JSON.parse(
    readFileSync(join(projectRoot, "package.json"), "utf8"),
  ) as { version: string }
  return { version: pkg.version, distTag: parseDistTag(pkg.version) }
}

export function parseDistTag(version: string): string | undefined {
  const match = version.match(/^\d+\.\d+\.\d+-(.+)$/)
  if (!match) return undefined
  // "rc.1" → "rc", "alpha" → "alpha", "beta.2.3" → "beta"
  return match[1].split(".")[0]
}
