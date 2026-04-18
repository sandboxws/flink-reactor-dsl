import { dirname, join, resolve } from "node:path"
import { fileURLToPath } from "node:url"

export interface Paths {
  projectRoot: string
  verdaccioDir: string
  verdaccioConfig: string
  storageDir: string
  pidFile: string
  logFile: string
}

export function resolvePaths(): Paths {
  const scriptsDir = dirname(dirname(fileURLToPath(import.meta.url)))
  const projectRoot = resolve(scriptsDir, "..")
  const verdaccioDir = join(projectRoot, ".verdaccio")
  return {
    projectRoot,
    verdaccioDir,
    verdaccioConfig: join(scriptsDir, "verdaccio.yaml"),
    storageDir: join(verdaccioDir, "storage"),
    pidFile: join(verdaccioDir, "verdaccio.pid"),
    logFile: join(verdaccioDir, "verdaccio.log"),
  }
}
