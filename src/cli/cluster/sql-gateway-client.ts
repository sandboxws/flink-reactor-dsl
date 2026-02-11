export interface SubmitResult {
  sessionHandle: string;
  operationHandle: string;
  status: 'RUNNING' | 'FINISHED' | 'ERROR';
  jobId?: string;
}

interface OpenSessionResponse {
  sessionHandle: string;
}

interface SubmitStatementResponse {
  operationHandle: string;
}

interface OperationStatusResponse {
  status: 'RUNNING' | 'FINISHED' | 'ERROR' | 'INITIALIZED';
  results?: {
    columns?: Array<{ name: string }>;
    data?: Array<{ fields: unknown[] }>;
  };
}

export class SqlGatewayClient {
  constructor(private baseUrl: string) {}

  async openSession(): Promise<string> {
    const res = await fetch(`${this.baseUrl}/sessions`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ properties: {} }),
    });

    if (!res.ok) {
      throw new Error(`Failed to open session: ${res.status} ${res.statusText}`);
    }

    const data = (await res.json()) as OpenSessionResponse;
    return data.sessionHandle;
  }

  async submitStatement(sessionHandle: string, sql: string): Promise<string> {
    const res = await fetch(`${this.baseUrl}/sessions/${sessionHandle}/statements`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ statement: sql }),
    });

    if (!res.ok) {
      const body = await res.text();
      throw new Error(`Failed to submit statement: ${res.status} ${body}`);
    }

    const data = (await res.json()) as SubmitStatementResponse;
    return data.operationHandle;
  }

  async pollStatus(
    sessionHandle: string,
    operationHandle: string,
  ): Promise<'RUNNING' | 'FINISHED' | 'ERROR'> {
    const res = await fetch(
      `${this.baseUrl}/sessions/${sessionHandle}/operations/${operationHandle}/status`,
    );

    if (!res.ok) {
      throw new Error(`Failed to poll status: ${res.status} ${res.statusText}`);
    }

    const data = (await res.json()) as OperationStatusResponse;
    if (data.status === 'INITIALIZED') {
      return 'RUNNING';
    }
    return data.status as 'RUNNING' | 'FINISHED' | 'ERROR';
  }

  async closeSession(sessionHandle: string): Promise<void> {
    const res = await fetch(`${this.baseUrl}/sessions/${sessionHandle}`, {
      method: 'DELETE',
    });

    if (!res.ok) {
      throw new Error(`Failed to close session: ${res.status} ${res.statusText}`);
    }
  }

  async submitSqlFile(filePath: string): Promise<SubmitResult> {
    const { readFileSync } = await import('node:fs');
    const sql = readFileSync(filePath, 'utf-8');
    const statements = splitSqlStatements(sql);

    const sessionHandle = await this.openSession();

    let lastOperationHandle = '';
    let lastStatus: 'RUNNING' | 'FINISHED' | 'ERROR' = 'RUNNING';

    for (const stmt of statements) {
      lastOperationHandle = await this.submitStatement(sessionHandle, stmt);

      // Poll until the statement reaches a terminal or running state
      const maxAttempts = 60;
      for (let i = 0; i < maxAttempts; i++) {
        lastStatus = await this.pollStatus(sessionHandle, lastOperationHandle);
        if (lastStatus !== 'RUNNING' || isStreamingStatement(stmt)) {
          break;
        }
        await sleep(1000);
      }

      if (lastStatus === 'ERROR') {
        break;
      }
    }

    // Leave session open for streaming jobs (closing kills the job)
    // Close batch sessions after completion
    if (lastStatus === 'FINISHED') {
      await this.closeSession(sessionHandle);
    }

    return {
      sessionHandle,
      operationHandle: lastOperationHandle,
      status: lastStatus,
    };
  }
}

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

function isStreamingStatement(sql: string): boolean {
  // INSERT INTO with unbounded source = streaming
  // EXECUTE STATEMENT SET = streaming
  const upper = sql.toUpperCase();
  return upper.includes('INSERT INTO') || upper.includes('EXECUTE STATEMENT SET');
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
