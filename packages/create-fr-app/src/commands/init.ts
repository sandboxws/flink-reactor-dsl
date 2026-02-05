import * as p from "@clack/prompts";
import fs from "node:fs";
import path from "node:path";
import { execSync } from "node:child_process";
import pc from "picocolors";
import { COMPONENT_REGISTRY, STYLE_FILES, collectDependencies, collectPeerDeps } from "../config.js";

interface InitOptions {
  template: string;
  git: boolean;
  install: boolean;
}

/**
 * Find the packages/ui source directory (works in development)
 */
function findUiPackagePath(): string | null {
  // When running in development, resolve relative to this file
  const devPath = path.resolve(import.meta.dirname, "../../../ui/src");
  if (fs.existsSync(devPath)) {
    return devPath;
  }

  // When installed globally, check node_modules
  const nmPath = path.resolve(import.meta.dirname, "../../ui/src");
  if (fs.existsSync(nmPath)) {
    return nmPath;
  }

  return null;
}

export async function init(projectName: string | undefined, options: InitOptions) {
  p.intro(pc.bgMagenta(pc.white(" create-fr-app ")));

  // 1. Get project name
  let name = projectName;
  if (!name) {
    const result = await p.text({
      message: "What is your project name?",
      placeholder: "my-app",
      validate: (value) => {
        if (!value) return "Project name is required";
        if (!/^[a-z0-9-]+$/.test(value)) {
          return "Project name must be lowercase alphanumeric with dashes";
        }
        return undefined;
      },
    });
    if (p.isCancel(result)) {
      p.cancel("Operation cancelled.");
      process.exit(0);
    }
    name = result;
  }

  const targetDir = path.resolve(process.cwd(), name);

  // 2. Check if directory exists
  if (fs.existsSync(targetDir)) {
    const overwrite = await p.confirm({
      message: `Directory "${name}" already exists. Overwrite?`,
      initialValue: false,
    });
    if (p.isCancel(overwrite) || !overwrite) {
      p.cancel("Operation cancelled.");
      process.exit(0);
    }
    fs.rmSync(targetDir, { recursive: true });
  }

  // 3. Find UI package source
  const uiSrcPath = findUiPackagePath();
  if (!uiSrcPath) {
    p.log.error("Could not find @flink-reactor/ui package source.");
    p.log.info("Make sure you're running from the flink-reactor repository.");
    process.exit(1);
  }

  const s = p.spinner();

  // 4. Create project structure
  s.start("Creating project structure...");

  fs.mkdirSync(targetDir, { recursive: true });
  fs.mkdirSync(path.join(targetDir, "src/components/ui"), { recursive: true });
  fs.mkdirSync(path.join(targetDir, "src/components/layout"), { recursive: true });
  fs.mkdirSync(path.join(targetDir, "src/components/shared"), { recursive: true });
  fs.mkdirSync(path.join(targetDir, "src/lib"), { recursive: true });
  fs.mkdirSync(path.join(targetDir, "src/app"), { recursive: true });
  fs.mkdirSync(path.join(targetDir, "src/styles"), { recursive: true });

  s.stop("Project structure created");

  // 5. Copy all components
  s.start("Copying design system components...");

  const allComponents = Object.keys(COMPONENT_REGISTRY);
  const manifest: Record<string, { hash: string; modified: boolean }> = {};

  for (const compName of allComponents) {
    const info = COMPONENT_REGISTRY[compName];
    const srcFile = path.join(uiSrcPath, info.path);

    if (!fs.existsSync(srcFile)) {
      p.log.warn(`Component file not found: ${info.path}`);
      continue;
    }

    // Determine target path based on component type
    let destPath: string;
    if (info.path.startsWith("lib/")) {
      destPath = path.join(targetDir, "src", info.path);
    } else if (info.path.startsWith("components/ui/")) {
      destPath = path.join(targetDir, "src", info.path);
    } else if (info.path.startsWith("layout/")) {
      destPath = path.join(targetDir, "src/components", info.path);
    } else if (info.path.startsWith("shared/")) {
      destPath = path.join(targetDir, "src/components", info.path);
    } else {
      destPath = path.join(targetDir, "src", info.path);
    }

    // Read source, update imports
    let content = fs.readFileSync(srcFile, "utf-8");

    // Update relative imports to use project structure
    content = content.replace(/from "\.\.\/\.\.\/lib\/cn"/g, 'from "@/lib/cn"');
    content = content.replace(/from "\.\.\/lib\/cn"/g, 'from "@/lib/cn"');
    content = content.replace(/from "\.\.\/components\/ui\//g, 'from "@/components/ui/');

    fs.mkdirSync(path.dirname(destPath), { recursive: true });
    fs.writeFileSync(destPath, content);

    // Simple hash for tracking (just file length for now)
    manifest[compName] = {
      hash: Buffer.from(content).toString("base64").slice(0, 12),
      modified: false,
    };
  }

  // Copy style files
  for (const stylePath of STYLE_FILES) {
    const srcFile = path.join(uiSrcPath, stylePath);
    if (fs.existsSync(srcFile)) {
      const destPath = path.join(targetDir, "src", stylePath);
      fs.mkdirSync(path.dirname(destPath), { recursive: true });
      fs.copyFileSync(srcFile, destPath);
    }
  }

  s.stop("Design system components copied");

  // 6. Create package.json
  s.start("Creating package.json...");

  const peerDeps = collectPeerDeps(allComponents);
  const packageJson = {
    name: name,
    version: "0.1.0",
    private: true,
    scripts: {
      dev: "next dev",
      build: "next build",
      start: "next start",
    },
    dependencies: {
      next: "^16.0.0",
      react: "^19.0.0",
      "react-dom": "^19.0.0",
      "lucide-react": "^0.500.0",
      "tailwind-merge": "^3.0.0",
      "@radix-ui/react-collapsible": "^1.1.0",
      "@radix-ui/react-dialog": "^1.1.0",
      "@radix-ui/react-hover-card": "^1.1.0",
      "@radix-ui/react-label": "^2.1.0",
      "@radix-ui/react-popover": "^1.1.0",
      "@radix-ui/react-progress": "^1.1.0",
      "@radix-ui/react-select": "^2.2.0",
      "@radix-ui/react-separator": "^1.1.0",
      "@radix-ui/react-tabs": "^1.1.0",
      "@radix-ui/react-tooltip": "^1.2.0",
      cmdk: "^1.1.0",
      "react-resizable-panels": "^4.5.0",
      geist: "^1.5.0",
    },
    devDependencies: {
      "@tailwindcss/postcss": "^4.1.0",
      tailwindcss: "^4.1.0",
      typescript: "^5.0.0",
      "@types/node": "^22.0.0",
      "@types/react": "^19.0.0",
      "@types/react-dom": "^19.0.0",
    },
  };

  fs.writeFileSync(
    path.join(targetDir, "package.json"),
    JSON.stringify(packageJson, null, 2),
  );

  s.stop("package.json created");

  // 7. Create config files
  s.start("Creating configuration files...");

  // tsconfig.json
  fs.writeFileSync(
    path.join(targetDir, "tsconfig.json"),
    JSON.stringify({
      compilerOptions: {
        target: "ES2022",
        lib: ["ES2022", "DOM", "DOM.Iterable"],
        module: "ESNext",
        moduleResolution: "bundler",
        strict: true,
        esModuleInterop: true,
        skipLibCheck: true,
        jsx: "preserve",
        noEmit: true,
        isolatedModules: true,
        baseUrl: ".",
        paths: { "@/*": ["./src/*"] },
        plugins: [{ name: "next" }],
      },
      include: ["next-env.d.ts", "**/*.ts", "**/*.tsx"],
      exclude: ["node_modules"],
    }, null, 2),
  );

  // postcss.config.mjs
  fs.writeFileSync(
    path.join(targetDir, "postcss.config.mjs"),
    `export default {
  plugins: {
    "@tailwindcss/postcss": {},
  },
};
`,
  );

  // next.config.ts
  fs.writeFileSync(
    path.join(targetDir, "next.config.ts"),
    `import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  reactStrictMode: true,
};

export default nextConfig;
`,
  );

  // Global CSS
  fs.writeFileSync(
    path.join(targetDir, "src/app/global.css"),
    `@import "tailwindcss";
@import "../styles/tokens.css";
@import "../styles/components.css";
`,
  );

  // Layout
  fs.writeFileSync(
    path.join(targetDir, "src/app/layout.tsx"),
    `import { GeistSans } from "geist/font/sans";
import { GeistMono } from "geist/font/mono";
import "./global.css";

export const metadata = {
  title: "${name}",
  description: "Built with FlinkReactor design system",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" className={\`\${GeistSans.variable} \${GeistMono.variable}\`}>
      <body className="bg-fr-bg text-zinc-100 antialiased">{children}</body>
    </html>
  );
}
`,
  );

  // Page
  fs.writeFileSync(
    path.join(targetDir, "src/app/page.tsx"),
    `import { Button } from "@/components/ui/button";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";

export default function Home() {
  return (
    <main className="flex min-h-screen items-center justify-center p-8">
      <Card className="w-full max-w-md">
        <CardHeader>
          <CardTitle>Welcome to ${name}</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <p className="text-sm text-zinc-400">
            Your app is ready with the FlinkReactor design system.
          </p>
          <div className="flex gap-2">
            <Button>Get Started</Button>
            <Button variant="secondary">Learn More</Button>
          </div>
        </CardContent>
      </Card>
    </main>
  );
}
`,
  );

  // cn.ts needs to be at @/lib/cn
  fs.writeFileSync(
    path.join(targetDir, "src/lib/cn.ts"),
    `export { twMerge as cn } from "tailwind-merge";
`,
  );

  s.stop("Configuration files created");

  // 8. Write manifest
  fs.writeFileSync(
    path.join(targetDir, ".fr-ui.json"),
    JSON.stringify({
      version: "0.1.0",
      source: "github:flink-reactor/flink-reactor#packages/ui",
      installed: manifest,
    }, null, 2),
  );

  // 9. Initialize git
  if (options.git) {
    s.start("Initializing git repository...");
    try {
      execSync("git init", { cwd: targetDir, stdio: "ignore" });
      fs.writeFileSync(
        path.join(targetDir, ".gitignore"),
        `node_modules
.next
dist
.env*.local
`,
      );
      s.stop("Git repository initialized");
    } catch {
      s.stop("Git initialization failed (git may not be installed)");
    }
  }

  // 10. Install dependencies
  if (options.install) {
    s.start("Installing dependencies...");
    try {
      execSync("pnpm install", { cwd: targetDir, stdio: "ignore" });
      s.stop("Dependencies installed");
    } catch {
      try {
        execSync("npm install", { cwd: targetDir, stdio: "ignore" });
        s.stop("Dependencies installed with npm");
      } catch {
        s.stop("Dependency installation failed. Run 'pnpm install' manually.");
      }
    }
  }

  // Done!
  p.outro(pc.green("✓ Project created successfully!"));

  console.log();
  console.log(`  ${pc.cyan("cd")} ${name}`);
  if (!options.install) {
    console.log(`  ${pc.cyan("pnpm install")}`);
  }
  console.log(`  ${pc.cyan("pnpm dev")}`);
  console.log();
}
