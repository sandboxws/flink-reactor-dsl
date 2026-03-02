import type { ScaffoldOptions, TemplateFile } from "../commands/new.js"
import { sharedFiles } from "./shared.js"

export function getMinimalTemplates(opts: ScaffoldOptions): TemplateFile[] {
  return [
    ...sharedFiles(opts, { dashboard: false }),
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
