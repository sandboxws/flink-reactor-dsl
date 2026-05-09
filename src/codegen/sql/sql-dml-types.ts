import type { SqlFragment } from "../sql-build-context.js"

/**
 * Output shape for DML emission — a single `INSERT INTO …` statement
 * paired with the fragment-attribution metadata that the dashboard uses
 * to highlight per-component contributions inside the rendered SQL.
 *
 * Lives in its own file because both `sql-query-side-paths.ts`
 * (SideOutput, Validate) and `sql-dml-collection.ts` (the main INSERT
 * collector — extracted in C.16) accumulate `DmlEntry[]` arrays, and we
 * want a shared type without forcing a circular import back to the
 * orchestrator.
 */
export interface DmlEntry {
  sql: string
  contributors: SqlFragment[]
}
