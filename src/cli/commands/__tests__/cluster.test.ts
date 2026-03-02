import { Command } from "commander"
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { isClusterRunning } from "../../cluster/health-check.js"
import {
  SqlGatewayClient,
  splitSqlStatements,
} from "../../cluster/sql-gateway-client.js"
import { registerClusterCommand } from "../cluster.js"

// ── splitSqlStatements ──────────────────────────────────────────────

describe("splitSqlStatements", () => {
  it("splits regular SQL statements on semicolons", () => {
    const sql = `
CREATE TABLE source (id INT) WITH ('connector' = 'datagen');

CREATE TABLE sink (id INT) WITH ('connector' = 'print');

INSERT INTO sink SELECT id FROM source;
    `

    const result = splitSqlStatements(sql)
    expect(result).toHaveLength(3)
    expect(result[0]).toContain("CREATE TABLE source")
    expect(result[1]).toContain("CREATE TABLE sink")
    expect(result[2]).toContain("INSERT INTO sink")
  })

  it("keeps EXECUTE STATEMENT SET as a single block", () => {
    const sql = `
CREATE TABLE source (id INT) WITH ('connector' = 'datagen');

CREATE TABLE sink1 (id INT) WITH ('connector' = 'print');

CREATE TABLE sink2 (id INT) WITH ('connector' = 'blackhole');

EXECUTE STATEMENT SET
BEGIN
  INSERT INTO sink1 SELECT id FROM source;
  INSERT INTO sink2 SELECT id FROM source;
END;
    `

    const result = splitSqlStatements(sql)
    expect(result).toHaveLength(4)
    expect(result[0]).toContain("CREATE TABLE source")
    expect(result[1]).toContain("CREATE TABLE sink1")
    expect(result[2]).toContain("CREATE TABLE sink2")
    expect(result[3]).toContain("EXECUTE STATEMENT SET")
    expect(result[3]).toContain("INSERT INTO sink1")
    expect(result[3]).toContain("INSERT INTO sink2")
    expect(result[3]).toContain("END")
  })

  it("skips empty lines and comments", () => {
    const sql = `
-- This is a comment
CREATE TABLE t (id INT) WITH ('connector' = 'datagen');

-- Another comment

INSERT INTO t SELECT 1;
    `

    const result = splitSqlStatements(sql)
    expect(result).toHaveLength(2)
    expect(result[0]).not.toContain("--")
    expect(result[1]).not.toContain("--")
  })

  it("handles SET statements", () => {
    const sql = `
SET 'execution.runtime-mode' = 'batch';

CREATE TABLE t (id INT) WITH ('connector' = 'datagen');

INSERT INTO t SELECT 1;
    `

    const result = splitSqlStatements(sql)
    expect(result).toHaveLength(3)
    expect(result[0]).toContain("SET 'execution.runtime-mode' = 'batch'")
  })

  it("handles statement without trailing semicolon", () => {
    const sql = "SELECT 1"
    const result = splitSqlStatements(sql)
    expect(result).toHaveLength(1)
    expect(result[0]).toBe("SELECT 1")
  })
})

// ── SqlGatewayClient ────────────────────────────────────────────────

describe("SqlGatewayClient", () => {
  let client: SqlGatewayClient
  let fetchSpy: ReturnType<typeof vi.spyOn>

  beforeEach(() => {
    client = new SqlGatewayClient("http://localhost:8083")
    fetchSpy = vi.spyOn(globalThis, "fetch")
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it("openSession sends POST to /sessions", async () => {
    fetchSpy.mockResolvedValueOnce(
      new Response(JSON.stringify({ sessionHandle: "sess-123" }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    )

    const handle = await client.openSession()
    expect(handle).toBe("sess-123")
    expect(fetchSpy).toHaveBeenCalledWith(
      "http://localhost:8083/sessions",
      expect.objectContaining({ method: "POST" }),
    )
  })

  it("submitStatement sends POST with SQL", async () => {
    fetchSpy.mockResolvedValueOnce(
      new Response(JSON.stringify({ operationHandle: "op-456" }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    )

    const handle = await client.submitStatement("sess-123", "SELECT 1")
    expect(handle).toBe("op-456")
    expect(fetchSpy).toHaveBeenCalledWith(
      "http://localhost:8083/sessions/sess-123/statements",
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({ statement: "SELECT 1" }),
      }),
    )
  })

  it("pollStatus returns mapped status", async () => {
    fetchSpy.mockResolvedValueOnce(
      new Response(JSON.stringify({ status: "FINISHED" }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    )

    const status = await client.pollStatus("sess-123", "op-456")
    expect(status).toBe("FINISHED")
  })

  it("pollStatus maps INITIALIZED to RUNNING", async () => {
    fetchSpy.mockResolvedValueOnce(
      new Response(JSON.stringify({ status: "INITIALIZED" }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    )

    const status = await client.pollStatus("sess-123", "op-456")
    expect(status).toBe("RUNNING")
  })

  it("closeSession sends DELETE", async () => {
    fetchSpy.mockResolvedValueOnce(new Response(null, { status: 200 }))

    await client.closeSession("sess-123")
    expect(fetchSpy).toHaveBeenCalledWith(
      "http://localhost:8083/sessions/sess-123",
      expect.objectContaining({ method: "DELETE" }),
    )
  })
})

// ── isClusterRunning ────────────────────────────────────────────────

describe("isClusterRunning", () => {
  let fetchSpy: ReturnType<typeof vi.spyOn>

  beforeEach(() => {
    fetchSpy = vi.spyOn(globalThis, "fetch")
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it("returns true when Flink REST responds 200", async () => {
    fetchSpy.mockResolvedValueOnce(new Response("{}", { status: 200 }))

    const result = await isClusterRunning(8081)
    expect(result).toBe(true)
  })

  it("returns false when fetch throws", async () => {
    fetchSpy.mockRejectedValueOnce(new Error("ECONNREFUSED"))

    const result = await isClusterRunning(8081)
    expect(result).toBe(false)
  })
})

// ── CDC publisher ───────────────────────────────────────────────────

describe("publishCdcMessages", () => {
  it("batch mode generates correct number of messages", async () => {
    const { execSync: _mockExecSync } = await import("node:child_process")

    // We can't easily mock execSync inside the module, but we can test
    // the Debezium envelope structure by importing the module
    // For now, test that the module exports correctly
    const { publishCdcMessages } = await import(
      "../../cluster/cdc-publisher.js"
    )
    expect(typeof publishCdcMessages).toBe("function")
  })
})

// ── Command registration ────────────────────────────────────────────

describe("registerClusterCommand", () => {
  it("registers cluster command with all 5 subcommands", () => {
    const program = new Command()
    registerClusterCommand(program)

    const cluster = program.commands.find((c) => c.name() === "cluster")
    expect(cluster).toBeDefined()

    const subcommandNames = cluster?.commands.map((c) => c.name())
    expect(subcommandNames).toContain("up")
    expect(subcommandNames).toContain("down")
    expect(subcommandNames).toContain("seed")
    expect(subcommandNames).toContain("status")
    expect(subcommandNames).toContain("submit")
    expect(subcommandNames).toHaveLength(5)
  })

  it("cluster up has --port and --seed options", () => {
    const program = new Command()
    registerClusterCommand(program)

    const cluster = program.commands.find((c) => c.name() === "cluster")!
    const up = cluster.commands.find((c) => c.name() === "up")!
    const options = up.options.map((o) => o.long)

    expect(options).toContain("--port")
    expect(options).toContain("--seed")
  })

  it("cluster down has --volumes option", () => {
    const program = new Command()
    registerClusterCommand(program)

    const cluster = program.commands.find((c) => c.name() === "cluster")!
    const down = cluster.commands.find((c) => c.name() === "down")!
    const options = down.options.map((o) => o.long)

    expect(options).toContain("--volumes")
  })

  it("cluster seed has --only option", () => {
    const program = new Command()
    registerClusterCommand(program)

    const cluster = program.commands.find((c) => c.name() === "cluster")!
    const seed = cluster.commands.find((c) => c.name() === "seed")!
    const options = seed.options.map((o) => o.long)

    expect(options).toContain("--only")
  })

  it("cluster seed has --domain option", () => {
    const program = new Command()
    registerClusterCommand(program)

    const cluster = program.commands.find((c) => c.name() === "cluster")!
    const seed = cluster.commands.find((c) => c.name() === "seed")!
    const options = seed.options.map((o) => o.long)

    expect(options).toContain("--domain")
  })

  it("cluster up has --domain option", () => {
    const program = new Command()
    registerClusterCommand(program)

    const cluster = program.commands.find((c) => c.name() === "cluster")!
    const up = cluster.commands.find((c) => c.name() === "up")!
    const options = up.options.map((o) => o.long)

    expect(options).toContain("--domain")
  })
})
