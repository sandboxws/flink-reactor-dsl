import { execSync } from "node:child_process"
import {
  createWriteStream,
  existsSync,
  readFileSync,
  unlinkSync,
  writeFileSync,
} from "node:fs"
import { join } from "node:path"
import { pipeline } from "node:stream/promises"
import * as clack from "@clack/prompts"
import pc from "picocolors"

// ── Constants ──────────────────────────────────────────────────────────

export const SAMPLE_DATABASES = ["pagila", "chinook", "employees"] as const

/** Schema to check for table counts — employees uses a custom schema */
export const DB_SCHEMA: Record<string, string> = {
  pagila: "public",
  chinook: "public",
  employees: "employees",
}

// ── Dump sources ───────────────────────────────────────────────────────

interface DumpSource {
  urls: string[]
  bzip2Last?: boolean
  removeLine?: RegExp
  preamble?: string
  epilogue?: string
}

const DUMP_SOURCES: Record<string, DumpSource> = {
  pagila: {
    urls: [
      "https://raw.githubusercontent.com/devrimgunduz/pagila/master/pagila-schema.sql",
      "https://raw.githubusercontent.com/devrimgunduz/pagila/master/pagila-data.sql",
    ],
    removeLine: /\bOWNER TO\b|\bGRANT\b|\bREVOKE\b/,
    preamble: "SET session_replication_role = 'replica';",
    epilogue: "SET session_replication_role = 'origin';",
  },
  chinook: {
    urls: [
      "https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_PostgreSql.sql",
    ],
    removeLine: /^DROP DATABASE\b|^CREATE DATABASE\b|^\\c\b/,
  },
  employees: {
    // The data dump is a self-contained pg_dump (DDL + COPY data), so we
    // fetch only that file. Concatenating the schema dump with it produced
    // duplicate `CREATE SCHEMA employees;` statements that broke loading.
    urls: [
      "https://raw.githubusercontent.com/h8/employees-database/master/employees_data.sql.bz2",
    ],
    bzip2Last: true,
    removeLine: /SET default_with_oids/,
  },
}

// ── Cache invalidation ─────────────────────────────────────────────────

/**
 * Per-DB markers identifying a cached dump as broken. When found, the dump
 * is deleted so the next `ensureSqlDumps()` call re-fetches from the
 * (corrected) source URLs. Keep markers narrow — they should match only
 * the broken variant, never a valid dump.
 */
const STALE_DUMP_MARKERS: Record<string, RegExp> = {
  // Pre-fix concatenation of schema + data dumps left two `CREATE SCHEMA
  // employees;` statements in the file, which crashed psql at line 277.
  employees: /CREATE SCHEMA employees;[\s\S]+CREATE SCHEMA employees;/,
  // Older chinook dumps still embedded `DROP DATABASE` before our
  // removeLine filter was added.
  chinook: /^DROP DATABASE\b/m,
}

function invalidateStaleDumps(initDir: string): void {
  for (const [db, marker] of Object.entries(STALE_DUMP_MARKERS)) {
    const path = join(initDir, `${db}.sql`)
    if (!existsSync(path)) continue
    // Only read enough to match the marker — employees.sql is ~3.9M lines
    // and reading the whole file just to detect a header-region issue is
    // wasteful. The duplicate CREATE SCHEMA sits within the first ~300
    // lines, and chinook's DROP DATABASE is at the very top.
    const head = readFileSync(path, "utf-8").slice(0, 16_000)
    if (marker.test(head)) unlinkSync(path)
  }
}

// ── Download logic ─────────────────────────────────────────────────────

export async function ensureSqlDumps(initDir: string): Promise<void> {
  // Invalidate cached dumps that match a known-bad marker so they get
  // re-downloaded with the corrected DUMP_SOURCES.
  invalidateStaleDumps(initDir)

  const missing = SAMPLE_DATABASES.filter(
    (db) => !existsSync(join(initDir, `${db}.sql`)),
  )
  if (missing.length === 0) return

  const spinner = clack.spinner()
  spinner.start(`Downloading sample databases: ${missing.join(", ")}...`)

  for (const db of missing) {
    const source = DUMP_SOURCES[db]
    const outPath = join(initDir, `${db}.sql`)

    try {
      spinner.message(`Downloading ${db}...`)
      await downloadDump(source, outPath)
    } catch (err) {
      spinner.stop(pc.yellow(`Failed to download ${db} (non-critical).`))
      if (err instanceof Error) {
        console.log(pc.dim(`  ${err.message}`))
      }
      console.log(
        pc.dim(`  You can manually place the SQL dump at: ${outPath}`),
      )
      return
    }
  }

  spinner.stop(pc.green("Sample database dumps ready."))
}

async function downloadDump(
  source: DumpSource,
  outPath: string,
): Promise<void> {
  const parts: string[] = []

  for (let i = 0; i < source.urls.length; i++) {
    const url = source.urls[i]
    const isBz2 = source.bzip2Last && i === source.urls.length - 1

    if (isBz2) {
      const tmpBz2 = `${outPath}.bz2`
      const tmpDecompressed = `${outPath}.tmp`
      try {
        const response = await fetch(url)
        if (!response.ok || !response.body) {
          throw new Error(`HTTP ${response.status} fetching ${url}`)
        }
        const ws = createWriteStream(tmpBz2)
        await pipeline(response.body as never, ws)
        execSync(`bunzip2 -c "${tmpBz2}" > "${tmpDecompressed}"`, {
          stdio: "pipe",
        })
        parts.push(readFileSync(tmpDecompressed, "utf-8"))
      } finally {
        try {
          unlinkSync(tmpBz2)
        } catch {
          /* ignore */
        }
        try {
          unlinkSync(`${outPath}.tmp`)
        } catch {
          /* ignore */
        }
      }
    } else {
      const response = await fetch(url)
      if (!response.ok) {
        throw new Error(`HTTP ${response.status} fetching ${url}`)
      }
      parts.push(await response.text())
    }
  }

  let content = parts.join("\n")

  if (source.removeLine) {
    content = content
      .split("\n")
      .filter((line) => !source.removeLine?.test(line))
      .join("\n")
  }

  if (source.preamble) {
    content = `${source.preamble}\n${content}`
  }
  if (source.epilogue) {
    content = `${content}\n${source.epilogue}\n`
  }

  writeFileSync(outPath, content, "utf-8")
}

// ── Init directory resolution ──────────────────────────────────────────

export function clusterInitDir(): string {
  const thisDir = new URL(".", import.meta.url).pathname
  // From dist: dist/ → ../src/cli/cluster/init
  const fromDist = join(thisDir, "..", "src", "cli", "cluster", "init")
  if (existsSync(fromDist)) return fromDist
  // From source: src/cli/commands/ → ../cluster/init
  return join(thisDir, "..", "cluster", "init")
}
