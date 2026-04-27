import type { ScaffoldOptions, TemplateFile } from "@/cli/commands/new.js"
import { sharedFiles, templateReadme } from "./shared.js"

export function getMinimalTemplates(opts: ScaffoldOptions): TemplateFile[] {
  return [
    ...sharedFiles(opts),
    {
      path: "schemas/.gitkeep",
      content: "",
    },
    {
      path: "pipelines/.gitkeep",
      content: "",
    },
    {
      path: "tests/.gitkeep",
      content: "",
    },
    templateReadme({
      templateName: "minimal",
      tagline:
        "An empty FlinkReactor scaffold — `package.json`, `tsconfig.json`, `flink-reactor.config.ts`, and the canonical `schemas/` / `pipelines/` / `tests/` directories ready for your first pipeline. No example pipelines are emitted; use this when you'd rather start from a blank slate than delete a starter pipeline.",
      pipelines: [],
      gettingStarted: [
        "pnpm install",
        "# Add your first pipeline at pipelines/<name>/index.tsx",
        "# Add a matching schema at schemas/<name>.ts",
        "pnpm synth",
      ],
    }),
  ]
}
