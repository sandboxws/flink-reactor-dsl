import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { SqlGatewayClient, SqlGatewayClientError } from '../client.js';
import type {
  RawFetchResultsResponse,
  RawGetOperationStatusResponse,
  RawOpenSessionResponse,
  RawSubmitStatementResponse,
} from '../types.js';

// ── Mock fetch ──────────────────────────────────────────────────────────────

let mockFetch: ReturnType<typeof vi.fn>;

beforeEach(() => {
  mockFetch = vi.fn();
  vi.stubGlobal('fetch', mockFetch);
});

afterEach(() => {
  vi.restoreAllMocks();
});

function mockJsonResponse<T>(data: T, status = 200): Response {
  return {
    ok: status >= 200 && status < 300,
    status,
    statusText: status === 200 ? 'OK' : 'Error',
    json: () => Promise.resolve(data),
    text: () => Promise.resolve(JSON.stringify(data)),
    headers: new Headers({ 'Content-Type': 'application/json' }),
  } as unknown as Response;
}

// ── Tests ───────────────────────────────────────────────────────────────────

describe('SqlGatewayClient', () => {
  const BASE_URL = 'http://localhost:8083';
  let client: SqlGatewayClient;

  beforeEach(() => {
    client = new SqlGatewayClient(BASE_URL);
  });

  describe('openSession', () => {
    it('sends POST to /v1/sessions and returns session handle', async () => {
      const response: RawOpenSessionResponse = { sessionHandle: 'sess-abc-123' };
      mockFetch.mockResolvedValueOnce(mockJsonResponse(response));

      const handle = await client.openSession();

      expect(handle).toBe('sess-abc-123');
      expect(mockFetch).toHaveBeenCalledWith(
        `${BASE_URL}/v1/sessions`,
        expect.objectContaining({
          method: 'POST',
          body: JSON.stringify({ properties: {} }),
        }),
      );
    });

    it('forwards session properties when provided', async () => {
      const response: RawOpenSessionResponse = { sessionHandle: 'sess-xyz' };
      mockFetch.mockResolvedValueOnce(mockJsonResponse(response));

      await client.openSession({ properties: { 'execution.runtime-mode': 'streaming' } });

      expect(mockFetch).toHaveBeenCalledWith(
        `${BASE_URL}/v1/sessions`,
        expect.objectContaining({
          body: JSON.stringify({
            properties: { 'execution.runtime-mode': 'streaming' },
          }),
        }),
      );
    });
  });

  describe('submitStatement', () => {
    it('sends POST with SQL body and returns operation handle', async () => {
      const response: RawSubmitStatementResponse = { operationHandle: 'op-def-456' };
      mockFetch.mockResolvedValueOnce(mockJsonResponse(response));

      const handle = await client.submitStatement('sess-abc', 'SELECT 1');

      expect(handle).toBe('op-def-456');
      expect(mockFetch).toHaveBeenCalledWith(
        `${BASE_URL}/v1/sessions/sess-abc/statements`,
        expect.objectContaining({
          method: 'POST',
          body: JSON.stringify({ statement: 'SELECT 1' }),
        }),
      );
    });
  });

  describe('getOperationStatus', () => {
    it('returns the operation status', async () => {
      const response: RawGetOperationStatusResponse = { status: 'RUNNING' };
      mockFetch.mockResolvedValueOnce(mockJsonResponse(response));

      const status = await client.getOperationStatus('sess-abc', 'op-def');

      expect(status).toBe('RUNNING');
      expect(mockFetch).toHaveBeenCalledWith(
        `${BASE_URL}/v1/sessions/sess-abc/operations/op-def/status`,
        expect.anything(),
      );
    });
  });

  describe('fetchResults', () => {
    it('parses raw response into normalized ResultPage with keyed rows', async () => {
      const raw: RawFetchResultsResponse = {
        results: {
          columns: [
            { name: 'id', logicalType: { type: 'BIGINT', nullable: false } },
            { name: 'name', logicalType: { type: 'VARCHAR(255)', nullable: true } },
          ],
          data: [
            { kind: 'INSERT', fields: [42, 'Alice'] },
            { kind: 'INSERT', fields: [99, 'Bob'] },
          ],
        },
        resultType: 'PAYLOAD',
        nextResultUri: '/v1/sessions/sess-abc/operations/op-def/result/1',
      };
      mockFetch.mockResolvedValueOnce(mockJsonResponse(raw));

      const page = await client.fetchResults('sess-abc', 'op-def', 0);

      expect(page.columns).toEqual([
        { columnName: 'id', dataType: 'BIGINT', nullable: false },
        { columnName: 'name', dataType: 'VARCHAR(255)', nullable: true },
      ]);
      expect(page.rows).toEqual([
        { id: 42, name: 'Alice' },
        { id: 99, name: 'Bob' },
      ]);
      expect(page.isEndOfStream).toBe(false);
      expect(page.nextResultUri).toBe(
        '/v1/sessions/sess-abc/operations/op-def/result/1',
      );
      expect(page.resultKind).toBe('SUCCESS_WITH_CONTENT');
    });

    it('handles empty result page', async () => {
      const raw: RawFetchResultsResponse = {
        results: {
          columns: [
            { name: 'id', logicalType: { type: 'BIGINT', nullable: true } },
          ],
          data: [],
        },
        resultType: 'PAYLOAD',
        nextResultUri: null,
      };
      mockFetch.mockResolvedValueOnce(mockJsonResponse(raw));

      const page = await client.fetchResults('sess-abc', 'op-def', 0);

      expect(page.rows).toEqual([]);
      expect(page.columns).toHaveLength(1);
      expect(page.resultKind).toBe('SUCCESS');
    });

    it('detects end-of-stream', async () => {
      const raw: RawFetchResultsResponse = {
        results: { columns: [], data: [] },
        resultType: 'EOS',
        nextResultUri: null,
      };
      mockFetch.mockResolvedValueOnce(mockJsonResponse(raw));

      const page = await client.fetchResults('sess-abc', 'op-def', 1);

      expect(page.isEndOfStream).toBe(true);
    });
  });

  describe('closeSession', () => {
    it('sends DELETE to the session endpoint', async () => {
      mockFetch.mockResolvedValueOnce(
        mockJsonResponse(undefined, 204) as Response,
      );

      await client.closeSession('sess-abc');

      expect(mockFetch).toHaveBeenCalledWith(
        `${BASE_URL}/v1/sessions/sess-abc`,
        expect.objectContaining({ method: 'DELETE' }),
      );
    });
  });

  describe('cancelOperation', () => {
    it('sends POST to the cancel endpoint', async () => {
      mockFetch.mockResolvedValueOnce(mockJsonResponse({}));

      await client.cancelOperation('sess-abc', 'op-def');

      expect(mockFetch).toHaveBeenCalledWith(
        `${BASE_URL}/v1/sessions/sess-abc/operations/op-def/cancel`,
        expect.objectContaining({ method: 'POST' }),
      );
    });
  });

  describe('fetchResultStream', () => {
    it('yields pages until isEndOfStream is true', async () => {
      const page1: RawFetchResultsResponse = {
        results: {
          columns: [{ name: 'val', logicalType: { type: 'INT', nullable: false } }],
          data: [{ kind: 'INSERT', fields: [1] }],
        },
        resultType: 'PAYLOAD',
        nextResultUri: '/v1/sessions/s/operations/o/result/1',
      };
      const page2: RawFetchResultsResponse = {
        results: {
          columns: [{ name: 'val', logicalType: { type: 'INT', nullable: false } }],
          data: [{ kind: 'INSERT', fields: [2] }],
        },
        resultType: 'PAYLOAD',
        nextResultUri: '/v1/sessions/s/operations/o/result/2',
      };
      const page3: RawFetchResultsResponse = {
        results: {
          columns: [{ name: 'val', logicalType: { type: 'INT', nullable: false } }],
          data: [{ kind: 'INSERT', fields: [3] }],
        },
        resultType: 'EOS',
        nextResultUri: null,
      };

      mockFetch
        .mockResolvedValueOnce(mockJsonResponse(page1))
        .mockResolvedValueOnce(mockJsonResponse(page2))
        .mockResolvedValueOnce(mockJsonResponse(page3));

      const pages = [];
      for await (const page of client.fetchResultStream('s', 'o', {
        pollIntervalMs: 0,
      })) {
        pages.push(page);
      }

      expect(pages).toHaveLength(3);
      expect(pages[0].rows).toEqual([{ val: 1 }]);
      expect(pages[1].rows).toEqual([{ val: 2 }]);
      expect(pages[2].rows).toEqual([{ val: 3 }]);
      expect(pages[2].isEndOfStream).toBe(true);
    });

    it('stops when AbortSignal is triggered', async () => {
      const page1: RawFetchResultsResponse = {
        results: {
          columns: [{ name: 'val', logicalType: { type: 'INT', nullable: false } }],
          data: [{ kind: 'INSERT', fields: [1] }],
        },
        resultType: 'PAYLOAD',
        nextResultUri: '/v1/sessions/s/operations/o/result/1',
      };

      mockFetch.mockResolvedValue(mockJsonResponse(page1));

      const ac = new AbortController();
      const pages = [];

      for await (const page of client.fetchResultStream('s', 'o', {
        pollIntervalMs: 0,
        signal: ac.signal,
      })) {
        pages.push(page);
        // Abort after first page
        ac.abort();
      }

      expect(pages).toHaveLength(1);
    });
  });

  describe('executeAndStream', () => {
    it('combines session creation, statement submission, and streaming', async () => {
      // openSession
      mockFetch.mockResolvedValueOnce(
        mockJsonResponse<RawOpenSessionResponse>({ sessionHandle: 'sess-1' }),
      );
      // submitStatement
      mockFetch.mockResolvedValueOnce(
        mockJsonResponse<RawSubmitStatementResponse>({ operationHandle: 'op-1' }),
      );
      // fetchResults (first page for column metadata)
      const firstPage: RawFetchResultsResponse = {
        results: {
          columns: [
            { name: 'id', logicalType: { type: 'BIGINT', nullable: false } },
          ],
          data: [{ kind: 'INSERT', fields: [1] }],
        },
        resultType: 'PAYLOAD',
        nextResultUri: '/v1/sessions/sess-1/operations/op-1/result/1',
      };
      mockFetch.mockResolvedValueOnce(mockJsonResponse(firstPage));

      // Remaining stream page (EOS)
      const lastPage: RawFetchResultsResponse = {
        results: {
          columns: [
            { name: 'id', logicalType: { type: 'BIGINT', nullable: false } },
          ],
          data: [{ kind: 'INSERT', fields: [2] }],
        },
        resultType: 'EOS',
        nextResultUri: null,
      };
      mockFetch.mockResolvedValueOnce(mockJsonResponse(lastPage));

      const result = await client.executeAndStream('SELECT * FROM orders', undefined, {
        pollIntervalMs: 0,
      });

      expect(result.sessionHandle).toBe('sess-1');
      expect(result.operationHandle).toBe('op-1');
      expect(result.columns).toEqual([
        { columnName: 'id', dataType: 'BIGINT', nullable: false },
      ]);

      const pages = [];
      for await (const page of result.stream) {
        pages.push(page);
      }
      expect(pages).toHaveLength(1);
      expect(pages[0].rows).toEqual([{ id: 2 }]);
    });
  });

  describe('error handling', () => {
    it('parses SqlGatewayError and throws typed error', async () => {
      mockFetch.mockResolvedValueOnce(
        mockJsonResponse(
          { errors: ["Encountered 'SELCT' at line 1, column 0"] },
          400,
        ),
      );

      const err = await client.openSession().catch((e: unknown) => e) as SqlGatewayClientError;
      expect(err).toBeInstanceOf(SqlGatewayClientError);
      expect(err.message).toBe("Encountered 'SELCT' at line 1, column 0");
      expect(err.statusCode).toBe(400);
    });

    it('wraps network errors with descriptive message', async () => {
      mockFetch.mockRejectedValueOnce(new Error('connect ECONNREFUSED'));

      await expect(client.openSession()).rejects.toThrow(
        /SQL Gateway unreachable/,
      );
    });

    it('handles non-JSON error responses', async () => {
      const res = {
        ok: false,
        status: 500,
        statusText: 'Internal Server Error',
        json: () => Promise.reject(new Error('not json')),
        headers: new Headers(),
      } as unknown as Response;
      mockFetch.mockResolvedValueOnce(res);

      await expect(client.openSession()).rejects.toThrow(
        /SQL Gateway error: 500/,
      );
    });
  });
});
