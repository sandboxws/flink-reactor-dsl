import * as p from "@clack/prompts";
import fs from "node:fs";
import path from "node:path";
import pc from "picocolors";
import { COMPONENT_REGISTRY, collectDependencies } from "../config.js";

interface AddOptions {
  yes: boolean;
}

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

export async function add(components: string[], options: AddOptions) {
  p.intro(pc.bgMagenta(pc.white(" create-fr-app add ")));

  // Validate component names
  const invalid = components.filter((c) => !COMPONENT_REGISTRY[c]);
  if (invalid.length > 0) {
    p.log.error(`Unknown components: ${invalid.join(", ")}`);
    p.log.info("Available components:");
    for (const [name, info] of Object.entries(COMPONENT_REGISTRY)) {
      console.log(`  ${pc.cyan(name)} - ${info.description ?? ""}`);
    }
    process.exit(1);
  }

  // Find UI package source
  const uiSrcPath = findUiPackagePath();
  if (!uiSrcPath) {
    p.log.error("Could not find @flink-reactor/ui package source.");
    process.exit(1);
  }

  // Collect all dependencies
  const allComponents = collectDependencies(components);
  const toInstall = [...allComponents];

  p.log.info(`Components to add: ${toInstall.join(", ")}`);

  if (!options.yes) {
    const confirm = await p.confirm({
      message: `Add ${toInstall.length} component(s)?`,
      initialValue: true,
    });
    if (p.isCancel(confirm) || !confirm) {
      p.cancel("Operation cancelled.");
      process.exit(0);
    }
  }

  const s = p.spinner();
  s.start("Adding components...");

  const cwd = process.cwd();
  let added = 0;

  for (const compName of toInstall) {
    const info = COMPONENT_REGISTRY[compName];
    const srcFile = path.join(uiSrcPath, info.path);

    if (!fs.existsSync(srcFile)) {
      p.log.warn(`Component file not found: ${info.path}`);
      continue;
    }

    // Determine target path
    let destPath: string;
    if (info.path.startsWith("lib/")) {
      destPath = path.join(cwd, "src", info.path);
    } else if (info.path.startsWith("components/ui/")) {
      destPath = path.join(cwd, "src", info.path);
    } else if (info.path.startsWith("layout/")) {
      destPath = path.join(cwd, "src/components", info.path);
    } else if (info.path.startsWith("shared/")) {
      destPath = path.join(cwd, "src/components", info.path);
    } else {
      destPath = path.join(cwd, "src", info.path);
    }

    // Read source, update imports
    let content = fs.readFileSync(srcFile, "utf-8");
    content = content.replace(/from "\.\.\/\.\.\/lib\/cn"/g, 'from "@/lib/cn"');
    content = content.replace(/from "\.\.\/lib\/cn"/g, 'from "@/lib/cn"');
    content = content.replace(/from "\.\.\/components\/ui\//g, 'from "@/components/ui/');

    fs.mkdirSync(path.dirname(destPath), { recursive: true });
    fs.writeFileSync(destPath, content);
    added++;
  }

  s.stop(`Added ${added} component(s)`);

  // Update manifest if it exists
  const manifestPath = path.join(cwd, ".fr-ui.json");
  if (fs.existsSync(manifestPath)) {
    const manifest = JSON.parse(fs.readFileSync(manifestPath, "utf-8"));
    for (const compName of toInstall) {
      if (!manifest.installed[compName]) {
        manifest.installed[compName] = {
          hash: "new",
          modified: false,
        };
      }
    }
    fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2));
    p.log.info("Updated .fr-ui.json manifest");
  }

  p.outro(pc.green("✓ Components added successfully!"));
}
