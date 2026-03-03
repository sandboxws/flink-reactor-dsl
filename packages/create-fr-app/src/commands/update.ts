import fs from "node:fs"
import path from "node:path"
import * as p from "@clack/prompts"
import pc from "picocolors"
import { COMPONENT_REGISTRY } from "../config.js"

interface UpdateOptions {
  yes: boolean
  force: boolean
}

/**
 * Find the packages/ui source directory
 */
function findUiPackagePath(): string | null {
  const devPath = path.resolve(import.meta.dirname, "../../../ui/src")
  if (fs.existsSync(devPath)) {
    return devPath
  }
  return null
}

/**
 * Simple hash for change detection
 */
function hashContent(content: string): string {
  return Buffer.from(content).toString("base64").slice(0, 12)
}

export async function update(options: UpdateOptions) {
  p.intro(pc.bgMagenta(pc.white(" create-fr-app update ")))

  const cwd = process.cwd()
  const manifestPath = path.join(cwd, ".fr-ui.json")

  // Check for manifest
  if (!fs.existsSync(manifestPath)) {
    p.log.error(
      "No .fr-ui.json found. This doesn't appear to be a FlinkReactor UI project.",
    )
    process.exit(1)
  }

  // Find UI package source
  const uiSrcPath = findUiPackagePath()
  if (!uiSrcPath) {
    p.log.error("Could not find @flink-reactor/ui package source.")
    process.exit(1)
  }

  const manifest = JSON.parse(fs.readFileSync(manifestPath, "utf-8"))
  const installed = manifest.installed as Record<
    string,
    { hash: string; modified: boolean }
  >

  const updates: Array<{ name: string; hasConflict: boolean }> = []

  // Collect updates
  for (const [compName, info] of Object.entries(installed)) {
    const compInfo = COMPONENT_REGISTRY[compName]
    if (!compInfo) continue

    const srcFile = path.join(uiSrcPath, compInfo.path)
    if (!fs.existsSync(srcFile)) continue

    const upstreamContent = fs.readFileSync(srcFile, "utf-8")
    const upstreamHash = hashContent(upstreamContent)

    // Check local version
    let localPath: string
    if (compInfo.path.startsWith("lib/")) {
      localPath = path.join(cwd, "src", compInfo.path)
    } else if (compInfo.path.startsWith("components/ui/")) {
      localPath = path.join(cwd, "src", compInfo.path)
    } else if (compInfo.path.startsWith("layout/")) {
      localPath = path.join(cwd, "src/components", compInfo.path)
    } else if (compInfo.path.startsWith("shared/")) {
      localPath = path.join(cwd, "src/components", compInfo.path)
    } else {
      localPath = path.join(cwd, "src", compInfo.path)
    }

    const hasUpstreamUpdate = upstreamHash !== info.hash
    if (!hasUpstreamUpdate) continue

    const hasLocalModification =
      fs.existsSync(localPath) &&
      hashContent(fs.readFileSync(localPath, "utf-8")) !== info.hash

    updates.push({ name: compName, hasConflict: hasLocalModification })
  }

  if (updates.length === 0) {
    p.outro(pc.green("✓ All components are up to date!"))
    return
  }

  // Show updates
  console.log()
  console.log(`Found ${updates.length} update(s):`)
  for (const { name, hasConflict } of updates) {
    if (hasConflict) {
      console.log(
        `  ${pc.yellow("⚠")} ${pc.cyan(name)} - has local modifications`,
      )
    } else {
      console.log(`  ${pc.green("○")} ${pc.cyan(name)}`)
    }
  }
  console.log()

  // Handle conflicts
  const conflicts = updates.filter((u) => u.hasConflict)
  if (conflicts.length > 0 && !options.force) {
    p.log.warn(`${conflicts.length} component(s) have local modifications.`)

    if (!options.yes) {
      const choice = await p.select({
        message: "How would you like to handle conflicts?",
        options: [
          { value: "skip", label: "Skip conflicting components" },
          { value: "overwrite", label: "Overwrite local changes" },
          { value: "cancel", label: "Cancel update" },
        ],
      })

      if (p.isCancel(choice) || choice === "cancel") {
        p.cancel("Update cancelled.")
        process.exit(0)
      }

      if (choice === "skip") {
        // Remove conflicts from update list
        const conflictNames = new Set(conflicts.map((c) => c.name))
        updates.splice(
          0,
          updates.length,
          ...updates.filter((u) => !conflictNames.has(u.name)),
        )
      }
    }
  }

  if (updates.length === 0) {
    p.outro(pc.yellow("No updates applied (all conflicting)."))
    return
  }

  // Apply updates
  const s = p.spinner()
  s.start("Applying updates...")

  let updated = 0
  for (const { name } of updates) {
    const compInfo = COMPONENT_REGISTRY[name]
    const srcFile = path.join(uiSrcPath, compInfo.path)

    let content = fs.readFileSync(srcFile, "utf-8")
    content = content.replace(/from "\.\.\/\.\.\/lib\/cn"/g, 'from "@/lib/cn"')
    content = content.replace(/from "\.\.\/lib\/cn"/g, 'from "@/lib/cn"')
    content = content.replace(
      /from "\.\.\/components\/ui\//g,
      'from "@/components/ui/',
    )

    let localPath: string
    if (compInfo.path.startsWith("lib/")) {
      localPath = path.join(cwd, "src", compInfo.path)
    } else if (compInfo.path.startsWith("components/ui/")) {
      localPath = path.join(cwd, "src", compInfo.path)
    } else if (compInfo.path.startsWith("layout/")) {
      localPath = path.join(cwd, "src/components", compInfo.path)
    } else if (compInfo.path.startsWith("shared/")) {
      localPath = path.join(cwd, "src/components", compInfo.path)
    } else {
      localPath = path.join(cwd, "src", compInfo.path)
    }

    fs.mkdirSync(path.dirname(localPath), { recursive: true })
    fs.writeFileSync(localPath, content)

    // Update manifest
    installed[name] = {
      hash: hashContent(content),
      modified: false,
    }

    updated++
  }

  // Save manifest
  fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2))

  s.stop(`Updated ${updated} component(s)`)
  p.outro(pc.green("✓ Update complete!"))
}
