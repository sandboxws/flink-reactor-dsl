import { existsSync } from "node:fs"
import { dirname, join } from "node:path"
import { fileURLToPath } from "node:url"

const __filename = fileURLToPath(import.meta.url)
const __dirname = dirname(__filename)

// When bundled by tsup → dist/cli.js, __dirname = <root>/dist, so we walk to
// <root>/src/cli/cluster. When running from source, this file lives in
// src/cli/cluster/ already and __dirname IS the cluster dir.
export function clusterDir(): string {
  const fromDist = join(__dirname, "..", "src", "cli", "cluster")
  if (existsSync(join(fromDist, "docker-compose.yml"))) {
    return fromDist
  }
  return __dirname
}

export function bundledComposePath(): string {
  return join(clusterDir(), "docker-compose.yml")
}
