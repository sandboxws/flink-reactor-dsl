/**
 * Pure string matchers for join ON-clause analysis.
 *
 * Used by both sink-metadata resolution (to derive a downstream sink's
 * primary key from a join's natural key) and the join builders themselves
 * (to suppress duplicate projection columns when both sides share the same
 * column name). Kept dependency-free so both consumers can import without
 * pulling in unrelated synthesis state.
 */

/**
 * Returns the shared column names when an ON clause joins on the same
 * column on both sides. Accepts three forms:
 *   `col = col`                          — unqualified, common in templates
 *   `left.col = right.col`               — qualified, same column, different qualifiers
 *   `a.c1 = b.c1 AND a.c2 = b.c2 [...]`  — compound of either form above
 *
 * All forms signal "join where both sides share these keys"; the codegen
 * disambiguates by qualifying with side-refs and pruning duplicate
 * projections. Returns null when any sub-clause isn't a same-name equality.
 */
export function sameNameJoinKeys(on: string): string[] | null {
  const clauses = on.split(/\s+AND\s+/i)
  const keys: string[] = []
  for (const clause of clauses) {
    const key = sameNameJoinKeyScalar(clause)
    if (!key) return null
    keys.push(key)
  }
  return keys.length > 0 ? keys : null
}

export function sameNameJoinKeyScalar(on: string): string | null {
  const unq = on.match(/^\s*([A-Za-z_][\w]*)\s*=\s*([A-Za-z_][\w]*)\s*$/)
  if (unq && unq[1] === unq[2]) return unq[1]
  const qual = on.match(
    /^\s*[A-Za-z_][\w]*\.([A-Za-z_][\w]*)\s*=\s*[A-Za-z_][\w]*\.([A-Za-z_][\w]*)\s*$/,
  )
  if (qual && qual[1] === qual[2]) return qual[1]
  return null
}
