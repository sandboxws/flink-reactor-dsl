import type { ScaffoldOptions, TemplateFile } from "@/cli/commands/new.js"
import { sharedFiles } from "./shared.js"

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
  ]
}
