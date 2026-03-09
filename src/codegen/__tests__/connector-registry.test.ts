import { describe, expect, it } from "vitest"
import {
  artifactToJarName,
  artifactToMavenUrl,
  detectJdbcDialect,
  lookupConnector,
  resolveConnectorArtifacts,
  resolveFormatArtifacts,
  resolveJdbcDialectArtifacts,
} from "@/codegen/connector-registry.js"

// ── Connector Lookup ────────────────────────────────────────────────

describe("lookupConnector", () => {
  it("finds Kafka connector", () => {
    const entry = lookupConnector("kafka")
    expect(entry).toBeDefined()
    expect(entry?.connectorId).toBe("kafka")
    expect(entry?.builtIn).toBe(false)
  })

  it("finds FileSystem as built-in", () => {
    const entry = lookupConnector("filesystem")
    expect(entry).toBeDefined()
    expect(entry?.builtIn).toBe(true)
  })

  it("returns undefined for unknown connector", () => {
    expect(lookupConnector("nonexistent")).toBeUndefined()
  })
})

// ── Kafka Connector Resolution ──────────────────────────────────────

describe("resolveConnectorArtifacts: Kafka", () => {
  it("resolves correct JAR for Flink 1.20", () => {
    const artifacts = resolveConnectorArtifacts("kafka", "1.20")
    expect(artifacts).toHaveLength(1)
    expect(artifacts[0].artifactId).toBe("flink-sql-connector-kafka")
    expect(artifacts[0].version).toBe("3.3.0-1.20")
  })

  it("resolves correct JAR for Flink 2.0", () => {
    const artifacts = resolveConnectorArtifacts("kafka", "2.0")
    expect(artifacts).toHaveLength(1)
    expect(artifacts[0].artifactId).toBe("flink-sql-connector-kafka")
    expect(artifacts[0].version).toBe("4.0.1-2.0")
  })

  it("resolves correct JAR for Flink 2.2", () => {
    const artifacts = resolveConnectorArtifacts("kafka", "2.2")
    expect(artifacts).toHaveLength(1)
    expect(artifacts[0].version).toBe("4.0.1-2.0")
  })

  it("resolves correct JAR for Flink 2.1", () => {
    const artifacts = resolveConnectorArtifacts("kafka", "2.1")
    expect(artifacts).toHaveLength(1)
    expect(artifacts[0].version).toBe("4.0.1-2.0")
  })
})

// ── JDBC Connector Resolution ───────────────────────────────────────

describe("resolveConnectorArtifacts: JDBC", () => {
  it("resolves single JAR for Flink 1.20", () => {
    const artifacts = resolveConnectorArtifacts("jdbc", "1.20")
    expect(artifacts).toHaveLength(1)
    expect(artifacts[0].artifactId).toBe("flink-connector-jdbc")
    expect(artifacts[0].version).toBe("3.2.0-1.20")
  })

  it("resolves modular core for Flink 2.0+", () => {
    const artifacts = resolveConnectorArtifacts("jdbc", "2.0")
    expect(artifacts).toHaveLength(1)
    expect(artifacts[0].artifactId).toBe("flink-connector-jdbc-core")
    expect(artifacts[0].version).toBe("3.2.0-2.0")
  })
})

// ── Elasticsearch Connector Resolution ──────────────────────────────

describe("resolveConnectorArtifacts: Elasticsearch 7", () => {
  it("resolves JAR for Flink 1.20", () => {
    const artifacts = resolveConnectorArtifacts("elasticsearch-7", "1.20")
    expect(artifacts).toHaveLength(1)
    expect(artifacts[0].artifactId).toBe("flink-sql-connector-elasticsearch7")
  })

  it("resolves JAR for Flink 2.0+", () => {
    const artifacts = resolveConnectorArtifacts("elasticsearch-7", "2.0")
    expect(artifacts).toHaveLength(1)
    expect(artifacts[0].version).toContain("2.0")
  })
})

// ── FileSystem (built-in) ───────────────────────────────────────────

describe("resolveConnectorArtifacts: FileSystem", () => {
  it("returns empty array for built-in connector", () => {
    expect(resolveConnectorArtifacts("filesystem", "2.0")).toHaveLength(0)
    expect(resolveConnectorArtifacts("filesystem", "1.20")).toHaveLength(0)
  })
})

// ── JDBC Dialect Sub-Registry ───────────────────────────────────────

describe("detectJdbcDialect", () => {
  it("detects PostgreSQL from URL", () => {
    const dialect = detectJdbcDialect("jdbc:postgresql://localhost:5433/mydb")
    expect(dialect).toBeDefined()
    expect(dialect?.dialect).toBe("postgres")
  })

  it("detects MySQL from URL", () => {
    const dialect = detectJdbcDialect("jdbc:mysql://localhost:3306/mydb")
    expect(dialect).toBeDefined()
    expect(dialect?.dialect).toBe("mysql")
  })

  it("detects Oracle from URL", () => {
    const dialect = detectJdbcDialect("jdbc:oracle:thin:@localhost:1521:mydb")
    expect(dialect).toBeDefined()
    expect(dialect?.dialect).toBe("oracle")
  })

  it("detects SQL Server from URL", () => {
    const dialect = detectJdbcDialect(
      "jdbc:sqlserver://localhost:1433;database=mydb",
    )
    expect(dialect).toBeDefined()
    expect(dialect?.dialect).toBe("sqlserver")
  })

  it("detects DB2 from URL", () => {
    const dialect = detectJdbcDialect("jdbc:db2://localhost:50000/mydb")
    expect(dialect).toBeDefined()
    expect(dialect?.dialect).toBe("db2")
  })

  it("returns undefined for unknown URL", () => {
    expect(detectJdbcDialect("jdbc:h2:mem:test")).toBeUndefined()
  })
})

describe("resolveJdbcDialectArtifacts", () => {
  it("resolves PostgreSQL: core + dialect + driver for Flink 2.0+", () => {
    const artifacts = resolveJdbcDialectArtifacts(
      "jdbc:postgresql://localhost:5433/mydb",
      "2.0",
    )
    expect(artifacts).toHaveLength(2)
    // dialect module
    expect(artifacts[0].artifactId).toBe("flink-connector-jdbc-postgres")
    expect(artifacts[0].version).toBe("3.2.0-2.0")
    // driver
    expect(artifacts[1].artifactId).toBe("postgresql")
    expect(artifacts[1].groupId).toBe("org.postgresql")
  })

  it("resolves only driver for Flink 1.20 (single JAR includes dialects)", () => {
    const artifacts = resolveJdbcDialectArtifacts(
      "jdbc:postgresql://localhost:5433/mydb",
      "1.20",
    )
    expect(artifacts).toHaveLength(1)
    expect(artifacts[0].artifactId).toBe("postgresql")
  })

  it("returns empty for unknown dialect URL", () => {
    expect(resolveJdbcDialectArtifacts("jdbc:h2:mem:test", "2.0")).toHaveLength(
      0,
    )
  })
})

// ── Format Dependencies ─────────────────────────────────────────────

describe("resolveFormatArtifacts", () => {
  it("returns empty for JSON (built-in)", () => {
    expect(resolveFormatArtifacts("json")).toHaveLength(0)
  })

  it("returns empty for CSV (built-in)", () => {
    expect(resolveFormatArtifacts("csv")).toHaveLength(0)
  })

  it("returns Avro JAR for avro format", () => {
    const artifacts = resolveFormatArtifacts("avro")
    expect(artifacts).toHaveLength(1)
    expect(artifacts[0].artifactId).toBe("flink-sql-avro")
  })

  it("returns Parquet JAR for parquet format", () => {
    const artifacts = resolveFormatArtifacts("parquet")
    expect(artifacts).toHaveLength(1)
    expect(artifacts[0].artifactId).toBe("flink-sql-parquet")
  })

  it("returns empty for debezium-json (built-in CDC)", () => {
    expect(resolveFormatArtifacts("debezium-json")).toHaveLength(0)
  })

  it("returns empty for unknown format", () => {
    expect(resolveFormatArtifacts("protobuf")).toHaveLength(0)
  })
})

// ── Maven URL Generation ────────────────────────────────────────────

describe("artifactToMavenUrl", () => {
  it("generates correct Maven Central URL", () => {
    const url = artifactToMavenUrl({
      groupId: "org.apache.flink",
      artifactId: "flink-sql-connector-kafka",
      version: "4.0.1-2.0",
    })
    expect(url).toBe(
      "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.1-2.0/flink-sql-connector-kafka-4.0.1-2.0.jar",
    )
  })

  it("uses custom base URL", () => {
    const url = artifactToMavenUrl(
      {
        groupId: "org.apache.flink",
        artifactId: "flink-sql-avro",
        version: "1.20.0",
      },
      "https://nexus.internal/maven-central",
    )
    expect(url).toContain(
      "https://nexus.internal/maven-central/org/apache/flink",
    )
  })
})

describe("artifactToJarName", () => {
  it("generates correct JAR filename", () => {
    expect(
      artifactToJarName({
        groupId: "org.apache.flink",
        artifactId: "flink-sql-connector-kafka",
        version: "4.0.1-2.0",
      }),
    ).toBe("flink-sql-connector-kafka-4.0.1-2.0.jar")
  })
})
