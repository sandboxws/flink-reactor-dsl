import type {
  ColumnInfo,
  RawFetchResultsResponse,
  RawGetOperationStatusResponse,
  RawOpenSessionResponse,
  RawSubmitStatementResponse,
  ResultPage,
  SessionConfig,
  SqlGatewayError,
  StatementStatus,
} from './types.js';

/** Error thrown when the SQL Gateway returns an error response */
export class SqlGatewayClientError extends Error {
  constructor(
    message: string,
    public readonly statusCode: number,
  ) {
    super(message);
    this.name = 'SqlGatewayClientError';
  }
}

export interface SqlGatewayClientOptions {
  /** AbortSignal for cancelling all in-flight requests */
  signal?: AbortSignal;
}

/**
 * Client for the Flink SQL Gateway v1 REST API.
 *
 * Provides session management, statement submission, status polling,
 * result fetching with pagination, and streaming result iteration.
 */
export class SqlGatewayClient {
  private readonly baseUrl: string;
  private readonly signal?: AbortSignal;

  constructor(baseUrl: string, options?: SqlGatewayClientOptions) {
    // Strip trailing slash for consistent URL construction
    this.baseUrl = baseUrl.replace(/\/+$/, '');
    this.signal = options?.signal;
  }

  /** Open a new SQL Gateway session */
  async openSession(config?: SessionConfig, signal?: AbortSignal): Promise<string> {
    const data = await this.request<RawOpenSessionResponse>('/v1/sessions', {
      method: 'POST',
      body: JSON.stringify({
        properties: config?.properties ?? {},
      }),
    }, signal);
    return data.sessionHandle;
  }

  /** Close a session */
  async closeSession(sessionHandle: string, signal?: AbortSignal): Promise<void> {
    await this.request<void>(`/v1/sessions/${sessionHandle}`, {
      method: 'DELETE',
    }, signal);
  }

  /** Submit a SQL statement to an open session, returns operation handle */
  async submitStatement(sessionHandle: string, sql: string, signal?: AbortSignal): Promise<string> {
    const data = await this.request<RawSubmitStatementResponse>(
      `/v1/sessions/${sessionHandle}/statements`,
      {
        method: 'POST',
        body: JSON.stringify({ statement: sql }),
      },
      signal,
    );
    return data.operationHandle;
  }

  /** Poll operation status */
  async getOperationStatus(
    sessionHandle: string,
    operationHandle: string,
    signal?: AbortSignal,
  ): Promise<StatementStatus> {
    const data = await this.request<RawGetOperationStatusResponse>(
      `/v1/sessions/${sessionHandle}/operations/${operationHandle}/status`,
      undefined,
      signal,
    );
    return data.status;
  }

  /** Fetch a single page of results */
  async fetchResults(
    sessionHandle: string,
    operationHandle: string,
    token: number,
    signal?: AbortSignal,
  ): Promise<ResultPage> {
    const raw = await this.request<RawFetchResultsResponse>(
      `/v1/sessions/${sessionHandle}/operations/${operationHandle}/result/${token}`,
      undefined,
      signal,
    );
    return normalizeResultPage(raw);
  }

  /** Cancel a running operation */
  async cancelOperation(sessionHandle: string, operationHandle: string, signal?: AbortSignal): Promise<void> {
    await this.request<void>(
      `/v1/sessions/${sessionHandle}/operations/${operationHandle}/cancel`,
      { method: 'POST' },
      signal,
    );
  }

  /**
   * Async generator that yields result pages as they become available.
   * Polls fetchResults with incrementing tokens at a configurable interval.
   * Terminates on end-of-stream or when the abort signal fires.
   */
  async *fetchResultStream(
    sessionHandle: string,
    operationHandle: string,
    options?: { pollIntervalMs?: number; signal?: AbortSignal },
  ): AsyncGenerator<ResultPage, void, unknown> {
    const pollInterval = options?.pollIntervalMs ?? 500;
    const signal = options?.signal;
    let token = 0;

    while (true) {
      if (signal?.aborted) return;

      const page = await this.fetchResults(sessionHandle, operationHandle, token);
      yield page;

      if (page.isEndOfStream) return;

      // Parse next token from nextResultUri, or increment
      if (page.nextResultUri) {
        const nextToken = parseTokenFromUri(page.nextResultUri);
        token = nextToken ?? token + 1;
      } else {
        token++;
      }

      // Wait before next poll
      if (signal?.aborted) return;
      await sleep(pollInterval, signal);
    }
  }

  /**
   * Convenience: submit SQL, wait for first result page, return stream.
   * Combines openSession + submitStatement + fetchResultStream.
   */
  async executeAndStream(
    sql: string,
    sessionConfig?: SessionConfig,
    options?: { pollIntervalMs?: number; signal?: AbortSignal },
  ): Promise<{
    sessionHandle: string;
    operationHandle: string;
    columns: ColumnInfo[];
    stream: AsyncGenerator<ResultPage, void, unknown>;
  }> {
    const sessionHandle = await this.openSession(sessionConfig);
    const operationHandle = await this.submitStatement(sessionHandle, sql);

    // Fetch first page to extract column metadata
    const firstPage = await this.fetchResults(sessionHandle, operationHandle, 0);

    // Create stream starting from token 1 (we already consumed token 0)
    const startToken = firstPage.nextResultUri
      ? (parseTokenFromUri(firstPage.nextResultUri) ?? 1)
      : 1;

    const self = this;
    async function* remainingStream(): AsyncGenerator<ResultPage, void, unknown> {
      if (firstPage.isEndOfStream) return;

      let token = startToken;
      const pollInterval = options?.pollIntervalMs ?? 500;
      const signal = options?.signal;

      while (true) {
        if (signal?.aborted) return;

        const page = await self.fetchResults(sessionHandle, operationHandle, token);
        yield page;

        if (page.isEndOfStream) return;

        if (page.nextResultUri) {
          const nextToken = parseTokenFromUri(page.nextResultUri);
          token = nextToken ?? token + 1;
        } else {
          token++;
        }

        if (signal?.aborted) return;
        await sleep(pollInterval, signal);
      }
    }

    return {
      sessionHandle,
      operationHandle,
      columns: firstPage.columns,
      stream: remainingStream(),
    };
  }

  // ─── Private helpers ────────────────────────────────────────────────────────

  private async request<T>(path: string, init?: RequestInit, methodSignal?: AbortSignal): Promise<T> {
    const url = `${this.baseUrl}${path}`;
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
      Accept: 'application/json',
    };

    // Per-method signal takes priority, falls back to constructor-level signal
    const signal = methodSignal ?? this.signal;

    let res: Response;
    try {
      res = await fetch(url, {
        ...init,
        headers: { ...headers, ...(init?.headers as Record<string, string>) },
        signal,
      });
    } catch (err) {
      if (err instanceof DOMException && err.name === 'AbortError') {
        throw err;
      }
      throw new SqlGatewayClientError(
        `SQL Gateway unreachable at ${this.baseUrl}: ${(err as Error).message}`,
        0,
      );
    }

    if (!res.ok) {
      let message = `SQL Gateway error: ${res.status} ${res.statusText}`;
      try {
        const body = (await res.json()) as SqlGatewayError;
        if (body.errors && body.errors.length > 0) {
          message = body.errors[0];
        }
      } catch {
        // Response body wasn't JSON, use status text
      }
      throw new SqlGatewayClientError(message, res.status);
    }

    // DELETE responses may have empty body
    if (res.status === 204 || init?.method === 'DELETE') {
      return undefined as T;
    }

    return (await res.json()) as T;
  }
}

// ─── Normalization ──────────────────────────────────────────────────────────

/** Normalize raw SQL Gateway result page into typed ResultPage */
function normalizeResultPage(raw: RawFetchResultsResponse): ResultPage {
  const columns: ColumnInfo[] = (raw.results?.columns ?? []).map((col) => ({
    columnName: col.name,
    dataType: col.logicalType.type,
    nullable: col.logicalType.nullable,
  }));

  const columnNames = columns.map((c) => c.columnName);

  const rows: Record<string, unknown>[] = (raw.results?.data ?? []).map((row) => {
    const obj: Record<string, unknown> = {};
    for (let i = 0; i < columnNames.length; i++) {
      obj[columnNames[i]] = row.fields[i];
    }
    return obj;
  });

  const isEndOfStream = raw.resultType === 'EOS';
  const resultKind: ResultPage['resultKind'] =
    rows.length > 0 ? 'SUCCESS_WITH_CONTENT' : 'SUCCESS';

  return {
    columns,
    rows,
    nextResultUri: raw.nextResultUri,
    isEndOfStream,
    resultKind,
  };
}

/** Extract the numeric token from a nextResultUri like "/v1/sessions/.../result/1" */
function parseTokenFromUri(uri: string): number | null {
  const match = uri.match(/\/result\/(\d+)/);
  return match ? parseInt(match[1], 10) : null;
}

/** Sleep for ms, cancellable via AbortSignal */
function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal?.aborted) {
      resolve();
      return;
    }
    const timer = setTimeout(resolve, ms);
    signal?.addEventListener('abort', () => {
      clearTimeout(timer);
      resolve();
    }, { once: true });
  });
}
