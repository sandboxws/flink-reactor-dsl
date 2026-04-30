/**
 * Centralized SQL identifier and literal quoting for Flink SQL emission.
 *
 * Flink SQL identifiers are wrapped in backticks. Embedded backticks in the
 * identifier itself must be doubled (`` `` ``). Failure to escape is a real
 * footgun: a column or table name that legitimately contains a backtick
 * would generate a malformed CREATE/SELECT statement that still parses
 * partially and fails downstream with confusing errors.
 *
 * String literals follow standard SQL: single-quote wrap, double the inner
 * quote. Used for connector option values, comment strings, default values.
 */

/** Wrap an identifier in backticks, escaping any embedded backticks. */
export function quoteIdentifier(name: string): string {
  return `\`${name.replace(/`/g, "``")}\``
}

/**
 * Build a dotted, fully-quoted SQL identifier from its parts.
 *
 *   quoteQualifiedName("cat", "db", "tbl") → `\`cat\`.\`db\`.\`tbl\``
 *
 * Use this instead of `${q(a)}.${q(b)}.${q(c)}` so the dot is uniformly
 * outside the backticks (and so callers don't accidentally introduce
 * spaces or other inconsistencies).
 */
export function quoteQualifiedName(...parts: readonly string[]): string {
  return parts.map(quoteIdentifier).join(".")
}

/** Wrap a string literal in single quotes, escaping embedded single quotes. */
export function quoteStringLiteral(value: string): string {
  return `'${value.replace(/'/g, "''")}'`
}
