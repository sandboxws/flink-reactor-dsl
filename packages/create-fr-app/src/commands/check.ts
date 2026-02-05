import * as p from "@clack/prompts";
import fs from "node:fs";
import path from "node:path";
import pc from "picocolors";
import { COMPONENT_REGISTRY } from "../config.js";

/**
 * Find the packages/ui source directory
 */
function findUiPackagePath(): string | null {
  const devPath = path.resolve(import.meta.dirname, "../../../ui/src");
  if (fs.existsSync(devPath)) {
    return devPath;
  }
  return null;
}

/**
 * Simple hash for change detection
 */
function hashContent(content: string): string {
  return Buffer.from(content).toString("base64").slice(0, 12);
}

export async function check() {
  p.intro(pc.bgMagenta(pc.white(" create-fr-app check ")));

  const cwd = process.cwd();
  const manifestPath = path.join(cwd, ".fr-ui.json");

  // Check for manifest
  if (!fs.existsSync(manifestPath)) {
    p.log.error("No .fr-ui.json found. This doesn't appear to be a FlinkReactor UI project.");
    p.log.info("Run 'create-fr-app init' to create a new project.");
    process.exit(1);
  }

  // Find UI package source
  const uiSrcPath = findUiPackagePath();
  if (!uiSrcPath) {
    p.log.error("Could not find @flink-reactor/ui package source.");
    p.log.info("Make sure you're running from the flink-reactor repository.");
    process.exit(1);
  }

  const manifest = JSON.parse(fs.readFileSync(manifestPath, "utf-8"));
  const installed = manifest.installed as Record<string, { hash: string; modified: boolean }>;

  const updates: string[] = [];
  const modified: string[] = [];
  const upToDate: string[] = [];

  for (const [compName, info] of Object.entries(installed)) {
    const compInfo = COMPONENT_REGISTRY[compName];
    if (!compInfo) {
      p.log.warn(`Unknown component in manifest: ${compName}`);
      continue;
    }

    // Check upstream version
    const srcFile = path.join(uiSrcPath, compInfo.path);
    if (!fs.existsSync(srcFile)) {
      p.log.warn(`Component file not found: ${compInfo.path}`);
      continue;
    }

    const upstreamContent = fs.readFileSync(srcFile, "utf-8");
    const upstreamHash = hashContent(upstreamContent);

    // Check local version
    let localPath: string;
    if (compInfo.path.startsWith("lib/")) {
      localPath = path.join(cwd, "src", compInfo.path);
    } else if (compInfo.path.startsWith("components/ui/")) {
      localPath = path.join(cwd, "src", compInfo.path);
    } else if (compInfo.path.startsWith("layout/")) {
      localPath = path.join(cwd, "src/components", compInfo.path);
    } else if (compInfo.path.startsWith("shared/")) {
      localPath = path.join(cwd, "src/components", compInfo.path);
    } else {
      localPath = path.join(cwd, "src", compInfo.path);
    }

    if (!fs.existsSync(localPath)) {
      updates.push(compName);
      continue;
    }

    const localContent = fs.readFileSync(localPath, "utf-8");
    const localHash = hashContent(localContent);

    const hasUpstreamUpdate = upstreamHash !== info.hash;
    const hasLocalModification = localHash !== info.hash;

    if (hasUpstreamUpdate && hasLocalModification) {
      modified.push(compName);
    } else if (hasUpstreamUpdate) {
      updates.push(compName);
    } else {
      upToDate.push(compName);
    }
  }

  // Report results
  console.log();

  if (updates.length > 0) {
    console.log(pc.yellow(`Updates available (${updates.length}):`));
    for (const comp of updates) {
      console.log(`  ${pc.cyan(comp)}`);
    }
    console.log();
  }

  if (modified.length > 0) {
    console.log(pc.magenta(`Modified locally (${modified.length}):`));
    for (const comp of modified) {
      console.log(`  ${pc.cyan(comp)} - upstream and local both changed`);
    }
    console.log();
  }

  if (upToDate.length > 0) {
    console.log(pc.green(`Up to date (${upToDate.length}):`));
    console.log(`  ${upToDate.join(", ")}`);
    console.log();
  }

  if (updates.length === 0 && modified.length === 0) {
    p.outro(pc.green("✓ All components are up to date!"));
  } else {
    p.outro(pc.yellow(`Run 'create-fr-app update' to apply updates`));
  }
}
