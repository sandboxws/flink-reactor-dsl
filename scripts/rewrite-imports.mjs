#!/usr/bin/env node

/**
 * Rewrites relative `../` imports in src/ to use the `@/` path alias.
 *
 * Handles:
 *   - Static imports:  import { Foo } from "../../core/types.js"
 *   - Dynamic imports: const m = await import("../cluster/health-check.js")
 *   - Re-exports:      export { Bar } from "../components/pipeline.js"
 *
 * Leaves `./` (same-directory) imports untouched — they're already short.
 *
 * Usage:
 *   node scripts/rewrite-imports.mjs            # rewrites src/
 *   node scripts/rewrite-imports.mjs --dry-run  # preview without writing
 */

import { readdirSync, readFileSync, writeFileSync } from "node:fs"
import { dirname, join, relative, resolve, sep } from "node:path"

const SRC_DIR = resolve("src")
const DRY_RUN = process.argv.includes("--dry-run")

// ── collect files ────────────────────────────────────────────────────

function collectFiles(dir, extensions) {
  const results = []
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const full = join(dir, entry.name)
    if (entry.isDirectory() && entry.name !== "node_modules") {
      results.push(...collectFiles(full, extensions))
    } else if (extensions.some((ext) => entry.name.endsWith(ext))) {
      results.push(full)
    }
  }
  return results
}

// ── rewrite logic ────────────────────────────────────────────────────

// Matches:  from "../path"  or  import("../path")
const IMPORT_RE = /(from\s+["']|import\(["'])(\.\.[^"']+)(["'])/g

function rewriteFile(filePath) {
  const content = readFileSync(filePath, "utf-8")
  const fileDir = dirname(filePath)

  const updated = content.replace(
    IMPORT_RE,
    (match, prefix, relPath, suffix) => {
      const absPath = resolve(fileDir, relPath)
      const srcRelative = relative(SRC_DIR, absPath)

      // If it resolves outside src/, keep original
      if (srcRelative.startsWith("..")) return match

      // Normalise to posix separators (for Windows compat)
      const aliased = srcRelative.split(sep).join("/")
      return `${prefix}@/${aliased}${suffix}`
    },
  )

  if (updated !== content) {
    if (!DRY_RUN) {
      writeFileSync(filePath, updated, "utf-8")
    }
    return true
  }
  return false
}

// ── main ─────────────────────────────────────────────────────────────

const files = collectFiles(SRC_DIR, [".ts", ".tsx"])
let changed = 0

for (const file of files) {
  if (rewriteFile(file)) {
    const rel = relative(process.cwd(), file)
    console.log(DRY_RUN ? `[dry-run] ${rel}` : `  rewrite  ${rel}`)
    changed++
  }
}

console.log(
  `\n${DRY_RUN ? "[dry-run] " : ""}${changed} file${changed === 1 ? "" : "s"} updated`,
)
