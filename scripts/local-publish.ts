/**
 * Local publishing CLI for flink-reactor-dsl.
 *
 * Replaces scripts/local-publish.sh with proper Verdaccio lifecycle management:
 * - PID-file-backed daemon tracking so a running instance can always be found
 * - /-/ping readiness probe (dual-stack via undici) to avoid race conditions
 * - `npm unpublish --force` default cleanup (respects Verdaccio's metadata cache)
 *
 * macOS/Linux only. Requires tsx to run.
 */

import { createReadStream, existsSync, statSync, watch } from "node:fs"
import { Command } from "commander"
import { resolvePaths } from "./local-publish/paths.js"
import { type CleanMode, runPublishFlow } from "./local-publish/publisher.js"
import * as ui from "./local-publish/ui.js"
import {
  ensureVerdaccio,
  isVerdaccioRunning,
  stopVerdaccio,
} from "./local-publish/verdaccio.js"
import { readRootVersion } from "./local-publish/version.js"

const REGISTRY = "http://localhost:4873"

const program = new Command()
  .name("local-publish")
  .description("Publish flink-reactor packages to a local Verdaccio registry")

program
  .command("publish", { isDefault: true })
  .description("Build, ensure Verdaccio is running, then publish all packages")
  .option("--no-build", "skip pnpm build")
  .option("--clean-mode <mode>", "unpublish | wipe | none", "unpublish")
  .option("--tag <tag>", "override auto-detected dist-tag")
  .option("--stop-after", "stop Verdaccio on success")
  .option("-v, --verbose", "stream child process output live", false)
  .action(
    async (opts: {
      build: boolean
      cleanMode: string
      tag?: string
      stopAfter?: boolean
      verbose: boolean
    }) => {
      const paths = resolvePaths()
      const { version, distTag: detectedTag } = readRootVersion(
        paths.projectRoot,
      )
      const distTag = opts.tag ?? detectedTag
      const cleanMode = validateCleanMode(opts.cleanMode)

      ui.intro(
        `flink-reactor local publish — ${version}${distTag ? ` (tag: ${distTag})` : ""}`,
      )

      process.on("SIGINT", () => process.exit(130))
      process.on("SIGTERM", () => process.exit(143))

      try {
        await runPublishFlow({
          paths,
          registry: REGISTRY,
          distTag,
          cleanMode,
          build: opts.build,
          verbose: opts.verbose,
        })

        if (opts.stopAfter) {
          const stopped = await stopVerdaccio({ paths })
          if (stopped.stopped) ui.info(`Stopped Verdaccio (pid=${stopped.pid})`)
        }

        ui.outro("Done.")
      } catch (err) {
        ui.error((err as Error).message)
        process.exitCode = 1
      }
    },
  )

const verdaccio = program
  .command("verdaccio")
  .description("Manage the local Verdaccio lifecycle")

verdaccio
  .command("start")
  .description("Start Verdaccio (idempotent — reuses an existing instance)")
  .action(async () => {
    const paths = resolvePaths()
    try {
      const { pid, startedByUs, port } = await ensureVerdaccio({ paths })
      ui.success(
        startedByUs
          ? `Started Verdaccio (pid=${pid}, port=${port})`
          : `Verdaccio already running (pid=${pid || "?"}, port=${port})`,
      )
      ui.info(`Registry: ${REGISTRY}`)
      ui.info(`Log: ${paths.logFile}`)
    } catch (err) {
      ui.error((err as Error).message)
      process.exitCode = 1
    }
  })

verdaccio
  .command("stop")
  .description("Stop Verdaccio and remove the PID file")
  .action(async () => {
    const paths = resolvePaths()
    const { stopped, pid } = await stopVerdaccio({ paths })
    if (stopped) ui.success(`Stopped Verdaccio (pid=${pid})`)
    else if (pid !== undefined)
      ui.info(`No running process for pid=${pid}; cleaned stale PID file.`)
    else ui.info("Verdaccio is not running.")
  })

verdaccio
  .command("status")
  .description("Print Verdaccio status (PID, reachable, ping)")
  .action(async () => {
    const paths = resolvePaths()
    const status = await isVerdaccioRunning({ paths })
    const fmt = (ok: boolean) => (ok ? ui.pc.green("yes") : ui.pc.red("no"))
    console.log(
      `  running:   ${fmt(status.running)}${status.pid ? `  (pid=${status.pid})` : ""}`,
    )
    console.log(`  port:      ${status.port}`)
    console.log(`  pinged:    ${fmt(status.pinged)}`)
    console.log(`  pid file:  ${paths.pidFile}`)
    console.log(`  log file:  ${paths.logFile}`)
  })

verdaccio
  .command("logs")
  .description("Print the Verdaccio log file (use -f to follow)")
  .option("-f, --follow", "follow the log file", false)
  .action(async (opts: { follow: boolean }) => {
    const paths = resolvePaths()
    if (!existsSync(paths.logFile)) {
      ui.info("No log file yet.")
      return
    }
    await streamLog(paths.logFile, opts.follow)
  })

function validateCleanMode(value: string): CleanMode {
  if (value === "unpublish" || value === "wipe" || value === "none")
    return value
  throw new Error(
    `Invalid --clean-mode: ${value}. Use unpublish | wipe | none.`,
  )
}

async function streamLog(path: string, follow: boolean): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const stream = createReadStream(path, { encoding: "utf8" })
    stream.pipe(process.stdout, { end: false })
    stream.on("end", resolve)
    stream.on("error", reject)
  })
  if (!follow) return

  let offset = statSync(path).size
  const watcher = watch(path, async () => {
    const size = statSync(path).size
    if (size <= offset) return
    await new Promise<void>((resolve, reject) => {
      const stream = createReadStream(path, {
        start: offset,
        end: size - 1,
        encoding: "utf8",
      })
      stream.pipe(process.stdout, { end: false })
      stream.on("end", () => {
        offset = size
        resolve()
      })
      stream.on("error", reject)
    })
  })

  await new Promise<void>((resolve) => {
    process.on("SIGINT", () => {
      watcher.close()
      resolve()
    })
  })
}

program.parseAsync(process.argv).catch((err) => {
  ui.error((err as Error).message)
  process.exit(1)
})
