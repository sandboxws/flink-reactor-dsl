import { describe, expect, it } from "vitest"
import { verifySql } from "@/codegen/sql/sql-verifier.js"

describe("verifySql — Tier 1 static checks", () => {
  // ── INSERT INTO / CREATE TABLE pairing ─────────────────────────────

  it("reports error when INSERT INTO target has no matching CREATE TABLE", () => {
    const statements = [
      "INSERT INTO `orders_sink` SELECT * FROM `orders_source`",
    ]
    const diags = verifySql(statements)
    expect(diags).toEqual([
      expect.objectContaining({
        severity: "error",
        category: "sql",
        message: expect.stringContaining("orders_sink"),
      }),
    ])
  })

  it("passes when all INSERT/CREATE pairs match", () => {
    const statements = [
      "CREATE TABLE `orders_source` (\n  `order_id` BIGINT\n) WITH (\n  'connector' = 'kafka'\n)",
      "CREATE TABLE `orders_sink` (\n  `order_id` BIGINT\n) WITH (\n  'connector' = 'kafka'\n)",
      "INSERT INTO `orders_sink` SELECT * FROM `orders_source`",
    ]
    const diags = verifySql(statements)
    expect(diags).toEqual([])
  })

  // ── Balanced parentheses ───────────────────────────────────────────

  it("reports error for unbalanced parentheses", () => {
    const statements = [
      "CREATE TABLE `t` (\n  `id` BIGINT\n WITH (\n  'connector' = 'kafka'\n)",
    ]
    const diags = verifySql(statements)
    expect(diags).toContainEqual(
      expect.objectContaining({
        severity: "error",
        category: "sql",
        message: expect.stringContaining("Unbalanced parentheses"),
      }),
    )
  })

  // ── Backtick-quoted identifier termination ─────────────────────────

  it("reports error for unterminated backtick identifier", () => {
    const statements = ["CREATE TABLE `orders_source (\n  `order_id` BIGINT\n)"]
    const diags = verifySql(statements)
    expect(diags).toContainEqual(
      expect.objectContaining({
        severity: "error",
        category: "sql",
        message: expect.stringContaining("Unterminated backtick"),
      }),
    )
  })

  // ── Empty statement detection ──────────────────────────────────────

  it("reports error for empty statement", () => {
    const statements = [
      "CREATE TABLE `t` (\n  `id` BIGINT\n) WITH (\n  'connector' = 'kafka'\n)",
      "",
      "INSERT INTO `t` SELECT 1",
    ]
    const diags = verifySql(statements)
    expect(diags).toContainEqual(
      expect.objectContaining({
        severity: "error",
        category: "sql",
        message: expect.stringContaining("Empty SQL statement"),
      }),
    )
  })

  // ── Valid SQL passes all checks ────────────────────────────────────

  it("passes all Tier 1 checks for valid SQL", () => {
    const statements = [
      "SET 'execution.checkpointing.interval' = '30s'",
      "CREATE TABLE `orders_source` (\n  `order_id` BIGINT,\n  `amount` DECIMAL(10, 2),\n  `event_time` TIMESTAMP(3)\n) WITH (\n  'connector' = 'kafka',\n  'topic' = 'orders',\n  'properties.bootstrap.servers' = 'localhost:9092',\n  'format' = 'json'\n)",
      "CREATE TABLE `orders_sink` (\n  `order_id` BIGINT,\n  `total` DECIMAL(10, 2)\n) WITH (\n  'connector' = 'kafka',\n  'topic' = 'output',\n  'properties.bootstrap.servers' = 'localhost:9092',\n  'format' = 'json'\n)",
      "INSERT INTO `orders_sink` SELECT `order_id`, SUM(`amount`) AS `total` FROM `orders_source` GROUP BY `order_id`",
    ]
    const diags = verifySql(statements)
    expect(diags).toEqual([])
  })

  // ── Backticks inside string literals ────────────────────────────────

  it("does not flag backticks inside string literals as unterminated", () => {
    const statements = [
      "CREATE TABLE `t` (\n  `id` BIGINT\n) WITH (\n  'connector' = 'kafka',\n  'value' = 'some `backtick` value'\n)",
      "INSERT INTO `t` SELECT 1",
    ]
    const diags = verifySql(statements)
    expect(diags).toEqual([])
  })
})
