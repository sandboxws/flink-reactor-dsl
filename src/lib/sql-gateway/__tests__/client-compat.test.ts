import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import {
  SqlGatewayClientError,
  SqlGatewayCompatClient,
} from "../client-compat.js"
import type {
  RawFetchResultsResponse,
  RawGetOperationStatusResponse,
  RawOpenSessionResponse,
  RawSubmitStatementResponse,
} from "../types.js"

// ── Mock fetch ──────────────────────────────────────────────────────────────

let mockFetch: ReturnType<typeof vi.fn>

beforeEach(() => {
  mockFetch = vi.fn()
  vi.stubGlobal("fetch", mockFetch)
})

afterEach(() => {
  vi.restoreAllMocks()
})

function mockJsonResponse<T>(data: T, status = 200): Response {
  return {
    ok: status >= 200 && status < 300,
    status,
    statusText: status === 200 ? "OK" : "Error",
    json: () => Promise.resolve(data),
    text: () => Promise.resolve(JSON.stringify(data)),
    headers: new Headers({ "Content-Type": "application/json" }),
  } as unknown as Response
}

// ── Tests ───────────────────────────────────────────────────────────────────

const BASE_URL = "http://localhost:8083"

describe("SqlGatewayCompatClient — integration with Effect backend", () => {
  let client: SqlGatewayCompatClient

  beforeEach(() => {
    client = new SqlGatewayCompatClient(BASE_URL)
  })

  describe("openSession", () => {
    it("returns session handle (same as original client)", async () => {
      mockFetch.mockResolvedValue(
        mockJsonResponse<RawOpenSessionResponse>({ sessionHandle: "sess-abc" }),
      )

      const handle = await client.openSession()
      expect(handle).toBe("sess-abc")
    })

    it("forwards session properties", async () => {
      mockFetch.mockResolvedValue(
        mockJsonResponse<RawOpenSessionResponse>({ sessionHandle: "sess-xyz" }),
      )

      await client.openSession({
        properties: { "execution.runtime-mode": "streaming" },
      })

      expect(mockFetch).toHaveBeenCalledWith(
        `${BASE_URL}/v1/sessions`,
        expect.objectContaining({
          method: "POST",
          body: JSON.stringify({
            properties: { "execution.runtime-mode": "streaming" },
          }),
        }),
      )
    })
  })

  describe("submitStatement", () => {
    it("returns operation handle", async () => {
      mockFetch.mockResolvedValue(
        mockJsonResponse<RawSubmitStatementResponse>({
          operationHandle: "op-123",
        }),
      )

      const handle = await client.submitStatement("sess-abc", "SELECT 1")
      expect(handle).toBe("op-123")
    })
  })

  describe("getOperationStatus", () => {
    it("returns status string", async () => {
      mockFetch.mockResolvedValue(
        mockJsonResponse<RawGetOperationStatusResponse>({ status: "FINISHED" }),
      )

      const status = await client.getOperationStatus("sess-abc", "op-def")
      expect(status).toBe("FINISHED")
    })
  })

  describe("fetchResults", () => {
    it("returns normalized ResultPage", async () => {
      const raw: RawFetchResultsResponse = {
        results: {
          columns: [
            { name: "id", logicalType: { type: "BIGINT", nullable: false } },
            {
              name: "name",
              logicalType: { type: "VARCHAR(255)", nullable: true },
            },
          ],
          data: [{ kind: "INSERT", fields: [42, "Alice"] }],
        },
        resultType: "PAYLOAD",
        nextResultUri: "/v1/sessions/s/operations/o/result/1",
      }
      mockFetch.mockResolvedValue(mockJsonResponse(raw))

      const page = await client.fetchResults("sess-abc", "op-def", 0)

      expect(page.columns).toEqual([
        { columnName: "id", dataType: "BIGINT", nullable: false },
        { columnName: "name", dataType: "VARCHAR(255)", nullable: true },
      ])
      expect(page.rows).toEqual([{ id: 42, name: "Alice" }])
      expect(page.isEndOfStream).toBe(false)
      expect(page.resultKind).toBe("SUCCESS_WITH_CONTENT")
    })
  })

  describe("closeSession", () => {
    it("sends DELETE and resolves", async () => {
      mockFetch.mockResolvedValue(mockJsonResponse(undefined, 204) as Response)

      await expect(client.closeSession("sess-abc")).resolves.toBeUndefined()
    })
  })

  describe("cancelOperation", () => {
    it("sends POST to cancel endpoint", async () => {
      mockFetch.mockResolvedValue(mockJsonResponse({}))

      await client.cancelOperation("sess-abc", "op-def")

      expect(mockFetch).toHaveBeenCalledWith(
        `${BASE_URL}/v1/sessions/sess-abc/operations/op-def/cancel`,
        expect.objectContaining({ method: "POST" }),
      )
    })
  })

  describe("error mapping", () => {
    it("maps connection errors to SqlGatewayClientError with statusCode 0", async () => {
      mockFetch.mockRejectedValue(new Error("connect ECONNREFUSED"))

      try {
        await client.openSession()
        expect.unreachable()
      } catch (err) {
        expect(err).toBeInstanceOf(SqlGatewayClientError)
        const e = err as SqlGatewayClientError
        expect(e.statusCode).toBe(0)
        expect(e.message).toContain("ECONNREFUSED")
      }
    })

    it("maps response errors to SqlGatewayClientError with correct status", async () => {
      mockFetch.mockResolvedValue(
        mockJsonResponse({ errors: ["Syntax error near 'SELCT'"] }, 400),
      )

      try {
        await client.submitStatement("sess-abc", "SELCT 1")
        expect.unreachable()
      } catch (err) {
        expect(err).toBeInstanceOf(SqlGatewayClientError)
        const e = err as SqlGatewayClientError
        expect(e.statusCode).toBe(400)
        expect(e.message).toContain("SELCT")
      }
    })
  })

  describe("executeAndStream", () => {
    it("combines session creation, submission, and streaming", async () => {
      // openSession
      mockFetch.mockResolvedValueOnce(
        mockJsonResponse<RawOpenSessionResponse>({ sessionHandle: "sess-1" }),
      )
      // submitStatement
      mockFetch.mockResolvedValueOnce(
        mockJsonResponse<RawSubmitStatementResponse>({
          operationHandle: "op-1",
        }),
      )
      // fetchResults (first page)
      const firstPage: RawFetchResultsResponse = {
        results: {
          columns: [
            { name: "id", logicalType: { type: "BIGINT", nullable: false } },
          ],
          data: [{ kind: "INSERT", fields: [1] }],
        },
        resultType: "PAYLOAD",
        nextResultUri: "/v1/sessions/sess-1/operations/op-1/result/1",
      }
      mockFetch.mockResolvedValueOnce(mockJsonResponse(firstPage))

      // Remaining stream page (EOS)
      const lastPage: RawFetchResultsResponse = {
        results: {
          columns: [
            { name: "id", logicalType: { type: "BIGINT", nullable: false } },
          ],
          data: [{ kind: "INSERT", fields: [2] }],
        },
        resultType: "EOS",
        nextResultUri: null,
      }
      mockFetch.mockResolvedValueOnce(mockJsonResponse(lastPage))

      const result = await client.executeAndStream(
        "SELECT * FROM orders",
        undefined,
        {
          pollIntervalMs: 0,
        },
      )

      expect(result.sessionHandle).toBe("sess-1")
      expect(result.operationHandle).toBe("op-1")
      expect(result.columns).toEqual([
        { columnName: "id", dataType: "BIGINT", nullable: false },
      ])

      const pages = []
      for await (const page of result.stream) {
        pages.push(page)
      }
      expect(pages).toHaveLength(1)
      expect(pages[0].rows).toEqual([{ id: 2 }])
    })
  })
})
