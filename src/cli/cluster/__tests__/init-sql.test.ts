import { describe, expect, it } from "vitest"
import { generateInitSql, type InitSqlContext } from "@/cli/cluster/init-sql.js"
import type { SimInitConfig } from "@/core/config.js"

const defaultCtx: InitSqlContext = {
  kafkaBootstrapServers: "kafka:9092",
  postgresHost: "postgres",
  postgresPort: 5432,
  postgresUser: "reactor",
  postgresPassword: "reactor",
}

describe("generateInitSql", () => {
  it("returns empty result for undefined config", () => {
    const result = generateInitSql(undefined, defaultCtx)
    expect(result.ddl).toEqual([])
    expect(result.seeding).toEqual([])
  })

  it("returns empty result for empty config", () => {
    const result = generateInitSql({}, defaultCtx)
    expect(result.ddl).toEqual([])
    expect(result.seeding).toEqual([])
  })

  it("generates built-in JDBC catalogs when includeBuiltinJdbc is true", () => {
    const result = generateInitSql(undefined, defaultCtx, {
      includeBuiltinJdbc: true,
    })

    expect(result.ddl).toHaveLength(4)
    expect(result.ddl.join("\n")).toMatchSnapshot()

    // Verify all 4 catalogs
    const catalogNames = ["pagila", "chinook", "employees", "flink_sink"]
    for (const name of catalogNames) {
      expect(result.ddl.some((s) => s.includes(`\`${name}\``))).toBe(true)
    }
  })

  it("generates JDBC catalog SQL from config", () => {
    const config: SimInitConfig = {
      jdbc: {
        catalogs: [
          {
            name: "my_db",
            baseUrl: "jdbc:postgresql://myhost:5432/",
            defaultDatabase: "mydb",
            username: "admin",
            password: "secret",
          },
        ],
      },
    }

    const result = generateInitSql(config, defaultCtx)
    expect(result.ddl).toHaveLength(1)
    expect(result.ddl[0]).toMatchSnapshot()
  })

  it("generates per-domain Kafka catalog with append-only table", () => {
    const config: SimInitConfig = {
      kafka: {
        catalogs: [
          {
            name: "ecom",
            tables: [
              {
                table: "orders",
                topic: "ecom.orders",
                columns: {
                  orderId: "STRING",
                  amount: "DOUBLE",
                  orderTime: "TIMESTAMP(3)",
                },
                watermark: {
                  column: "orderTime",
                  expression: "orderTime - INTERVAL '5' SECOND",
                },
              },
            ],
          },
        ],
      },
    }

    const result = generateInitSql(config, defaultCtx)
    expect(result.ddl.join("\n\n")).toMatchSnapshot()
  })

  it("generates Kafka table with debezium-json format and primary key", () => {
    const config: SimInitConfig = {
      kafka: {
        catalogs: [
          {
            name: "banking",
            tables: [
              {
                table: "accounts",
                topic: "bank.accounts",
                columns: {
                  accountId: "STRING",
                  name: "STRING",
                  balance: "DOUBLE",
                },
                format: "debezium-json",
                primaryKey: ["accountId"],
              },
            ],
          },
        ],
      },
    }

    const result = generateInitSql(config, defaultCtx)
    const allDdl = result.ddl.join("\n\n")
    expect(allDdl).toContain("PRIMARY KEY (`accountId`) NOT ENFORCED")
    expect(allDdl).toContain("'format' = 'debezium-json'")
    expect(allDdl).toMatchSnapshot()
  })

  it("generates DataGen seeding for Kafka tables", () => {
    const config: SimInitConfig = {
      kafka: {
        catalogs: [
          {
            name: "ecom",
            tables: [
              {
                table: "orders",
                topic: "ecom.orders",
                columns: { orderId: "STRING", amount: "DOUBLE" },
              },
            ],
          },
        ],
      },
    }

    const result = generateInitSql(config, defaultCtx)
    expect(result.seeding).toHaveLength(2) // CREATE TEMPORARY TABLE + INSERT INTO
    expect(result.seeding.join("\n\n")).toMatchSnapshot()
  })

  it("skips DataGen seeding when rowsPerSecond is 0", () => {
    const config: SimInitConfig = {
      kafka: {
        catalogs: [
          {
            name: "ecom",
            tables: [
              {
                table: "orders",
                topic: "ecom.orders",
                columns: { orderId: "STRING" },
                rowsPerSecond: 0,
              },
            ],
          },
        ],
      },
    }

    const result = generateInitSql(config, defaultCtx)
    expect(result.seeding).toHaveLength(0)
  })

  it("uses custom rowsPerSecond in DataGen", () => {
    const config: SimInitConfig = {
      kafka: {
        catalogs: [
          {
            name: "iot",
            tables: [
              {
                table: "readings",
                topic: "iot.readings",
                columns: { value: "DOUBLE" },
                rowsPerSecond: 50,
              },
            ],
          },
        ],
      },
    }

    const result = generateInitSql(config, defaultCtx)
    expect(
      result.seeding.some((s) => s.includes("'rows-per-second' = '50'")),
    ).toBe(true)
  })

  it("preserves kafka.topics without generating Flink tables", () => {
    const config: SimInitConfig = {
      kafka: {
        topics: ["sink.topic-a", "sink.topic-b"],
      },
    }

    const result = generateInitSql(config, defaultCtx)
    expect(result.ddl).toHaveLength(0)
    expect(result.seeding).toHaveLength(0)
  })

  it("restores default_catalog after each Kafka catalog", () => {
    const config: SimInitConfig = {
      kafka: {
        catalogs: [
          {
            name: "ecom",
            tables: [
              {
                table: "orders",
                topic: "ecom.orders",
                columns: { id: "STRING" },
              },
            ],
          },
          {
            name: "iot",
            tables: [
              {
                table: "readings",
                topic: "iot.readings",
                columns: { value: "DOUBLE" },
              },
            ],
          },
        ],
      },
    }

    const result = generateInitSql(config, defaultCtx)
    const restoreStatements = result.ddl.filter((s) =>
      s.includes("USE CATALOG `default_catalog`"),
    )
    expect(restoreStatements).toHaveLength(2)
  })

  it("combines JDBC and Kafka catalogs with built-in JDBC", () => {
    const config: SimInitConfig = {
      kafka: {
        topics: ["sink.alerts"],
        catalogs: [
          {
            name: "ecom",
            tables: [
              {
                table: "orders",
                topic: "ecom.orders",
                columns: { orderId: "STRING" },
              },
            ],
          },
        ],
      },
      jdbc: {
        catalogs: [
          {
            name: "flink_sink",
            baseUrl: "jdbc:postgresql://postgres:5432/",
            defaultDatabase: "flink_sink",
          },
        ],
      },
    }

    const result = generateInitSql(config, defaultCtx, {
      includeBuiltinJdbc: true,
    })

    // 4 built-in JDBC + 1 user JDBC + CREATE CATALOG ecom + USE CATALOG ecom + CREATE TABLE orders + USE CATALOG default_catalog
    expect(result.ddl.length).toBeGreaterThanOrEqual(8)
    expect(result.seeding).toHaveLength(2)
  })

  it("uses k8s context values in generated SQL", () => {
    const k8sCtx: InitSqlContext = {
      kafkaBootstrapServers: "kafka.flink-demo.svc:9092",
      postgresHost: "postgres.flink-demo.svc",
      postgresPort: 5432,
      postgresUser: "admin",
      postgresPassword: "admin",
    }

    const config: SimInitConfig = {
      kafka: {
        catalogs: [
          {
            name: "test",
            tables: [
              {
                table: "events",
                topic: "test.events",
                columns: { id: "STRING" },
              },
            ],
          },
        ],
      },
    }

    const result = generateInitSql(config, k8sCtx)
    expect(
      result.ddl.some((s) => s.includes("kafka.flink-demo.svc:9092")),
    ).toBe(true)
  })
})
