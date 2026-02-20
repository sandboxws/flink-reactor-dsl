/**
 * Split a SQL file into individual statements, keeping
 * `EXECUTE STATEMENT SET BEGIN ... END;` as a single block.
 */
export function splitSqlStatements(sql: string): string[] {
  const statements: string[] = [];
  const lines = sql.split('\n');
  let current = '';
  let inStatementSet = false;

  for (const line of lines) {
    const trimmed = line.trim();

    // Skip empty lines and comments
    if (!trimmed || trimmed.startsWith('--')) {
      continue;
    }

    // Detect STATEMENT SET block start (may be on one or two lines)
    if (/^EXECUTE\s+STATEMENT\s+SET\s*$/i.test(trimmed)) {
      // Flush any pending statement
      const pending = current.trim().replace(/;$/, '').trim();
      if (pending) statements.push(pending);
      current = trimmed + '\n';
      inStatementSet = true;
      continue;
    }

    if (!inStatementSet && /^EXECUTE\s+STATEMENT\s+SET\s+BEGIN\s*$/i.test(trimmed)) {
      const pending = current.trim().replace(/;$/, '').trim();
      if (pending) statements.push(pending);
      current = trimmed + '\n';
      inStatementSet = true;
      continue;
    }

    if (inStatementSet) {
      current += line + '\n';
      // Detect STATEMENT SET block end
      if (/^END\s*;?\s*$/i.test(trimmed)) {
        inStatementSet = false;
        statements.push(current.trim());
        current = '';
      }
      continue;
    }

    // Regular statement: accumulate until semicolon
    current += line + '\n';
    if (trimmed.endsWith(';')) {
      const stmt = current.trim().replace(/;$/, '').trim();
      if (stmt) {
        statements.push(stmt);
      }
      current = '';
    }
  }

  // Handle final statement without trailing semicolon
  const remaining = current.trim().replace(/;$/, '').trim();
  if (remaining) {
    statements.push(remaining);
  }

  return statements;
}
