import { beforeEach, describe, expect, it } from "vitest"
import { resolveConnectorArtifacts } from "@/codegen/connector-registry.js"
import { resolveConnectors } from "@/codegen/connector-resolver.js"
import { FlussCatalog } from "@/components/catalogs.js"
import { UDF } from "@/components/escape-hatches.js"
import { Pipeline } from "@/components/pipeline.js"
import { FileSystemSink, FlussSink, KafkaSink } from "@/components/sinks.js"
import {
  JdbcSource,
  KafkaSource,
  PostgresCdcPipelineSource,
} from "@/components/sources.js"
import { resetNodeIdCounter } from "@/core/jsx-runtime.js"
import { Field, Schema } from "@/core/schema.js"
import { secretRef } from "@/core/secret-ref.js"

beforeEach(() => {
  resetNodeIdCounter()
})

const OrderSchema = Schema({
  fields: {
    order_id: Field.BIGINT(),
    amount: Field.DECIMAL(10, 2),
    event_time: Field.TIMESTAMP(3),
  },
})

// ── Kafka Resolution ────────────────────────────────────────────────

describe("Kafka connector resolution", () => {
  it("resolves correct JAR for Flink 2.2", () => {
    const source = KafkaSource({
      topic: "orders",
      format: "json",
      schema: OrderSchema,
    })

    const sink = KafkaSink({
      topic: "output",
      children: [source],
    })

    const pipeline = Pipeline({ name: "test", children: [sink] })

    const result = resolveConnectors(pipeline, { flinkVersion: "2.2" })
    expect(result.jars).toHaveLength(1)
    expect(result.jars[0].artifact.artifactId).toBe("flink-sql-connector-kafka")
    expect(result.jars[0].artifact.version).toBe("4.0.1-2.0")
    expect(result.conflicts).toHaveLength(0)
  })

  it("resolves correct JAR for Flink 1.20", () => {
    const source = KafkaSource({
      topic: "orders",
      schema: OrderSchema,
    })

    const sink = KafkaSink({
      topic: "output",
      children: [source],
    })

    const pipeline = Pipeline({ name: "test", children: [sink] })

    const result = resolveConnectors(pipeline, { flinkVersion: "1.20" })
    expect(result.jars).toHaveLength(1)
    expect(result.jars[0].artifact.version).toBe("3.3.0-1.20")
  })
})

// ── De-duplication ──────────────────────────────────────────────────

describe("de-duplication", () => {
  it("two KafkaSources produce one Kafka JAR", () => {
    const source1 = KafkaSource({
      topic: "orders",
      format: "json",
      schema: OrderSchema,
    })

    const source2 = KafkaSource({
      topic: "users",
      format: "json",
      schema: OrderSchema,
    })

    const sink = KafkaSink({
      topic: "output",
      children: [source1],
    })

    const sink2 = KafkaSink({
      topic: "output2",
      children: [source2],
    })

    const pipeline = Pipeline({ name: "test", children: [sink, sink2] })

    const result = resolveConnectors(pipeline, { flinkVersion: "2.0" })
    // Only one Kafka JAR despite four Kafka components
    const kafkaJars = result.jars.filter(
      (j) => j.artifact.artifactId === "flink-sql-connector-kafka",
    )
    expect(kafkaJars).toHaveLength(1)
  })
})

// ── JDBC PostgreSQL Resolution ──────────────────────────────────────

describe("JDBC PostgreSQL resolution", () => {
  it("resolves core + postgres dialect + driver for Flink 2.0+", () => {
    const source = JdbcSource({
      url: "jdbc:postgresql://localhost:5432/mydb",
      table: "orders",
      schema: OrderSchema,
    })

    const sink = KafkaSink({
      topic: "output",
      children: [source],
    })

    const pipeline = Pipeline({ name: "test", children: [sink] })

    const result = resolveConnectors(pipeline, { flinkVersion: "2.0" })

    const artifactIds = result.jars.map((j) => j.artifact.artifactId)
    expect(artifactIds).toContain("flink-connector-jdbc-core")
    expect(artifactIds).toContain("flink-connector-jdbc-postgres")
    expect(artifactIds).toContain("postgresql")
    // Also Kafka JAR from the sink
    expect(artifactIds).toContain("flink-sql-connector-kafka")
  })

  it("resolves single JDBC JAR + driver for Flink 1.20", () => {
    const source = JdbcSource({
      url: "jdbc:postgresql://localhost:5432/mydb",
      table: "orders",
      schema: OrderSchema,
    })

    const sink = KafkaSink({
      topic: "output",
      children: [source],
    })

    const pipeline = Pipeline({ name: "test", children: [sink] })

    const result = resolveConnectors(pipeline, { flinkVersion: "1.20" })

    const artifactIds = result.jars.map((j) => j.artifact.artifactId)
    expect(artifactIds).toContain("flink-connector-jdbc")
    expect(artifactIds).toContain("postgresql")
    // No separate dialect module for 1.20
    expect(artifactIds).not.toContain("flink-connector-jdbc-postgres")
  })
})

// ── Format Dependencies ─────────────────────────────────────────────

describe("format dependency resolution", () => {
  it("Avro format adds JAR", () => {
    const source = KafkaSource({
      topic: "orders",
      format: "avro",
      schema: OrderSchema,
    })

    const sink = KafkaSink({
      topic: "output",
      children: [source],
    })

    const pipeline = Pipeline({ name: "test", children: [sink] })

    const result = resolveConnectors(pipeline, { flinkVersion: "2.0" })
    const artifactIds = result.jars.map((j) => j.artifact.artifactId)
    expect(artifactIds).toContain("flink-sql-avro")
    expect(artifactIds).toContain("flink-sql-connector-kafka")
  })

  it("JSON format does not add extra JAR", () => {
    const source = KafkaSource({
      topic: "orders",
      format: "json",
      schema: OrderSchema,
    })

    const sink = KafkaSink({
      topic: "output",
      children: [source],
    })

    const pipeline = Pipeline({ name: "test", children: [sink] })

    const result = resolveConnectors(pipeline, { flinkVersion: "2.0" })
    const artifactIds = result.jars.map((j) => j.artifact.artifactId)
    expect(artifactIds).not.toContain("flink-sql-avro")
    expect(artifactIds).not.toContain("flink-sql-parquet")
  })
})

// ── FileSystem (no JARs) ────────────────────────────────────────────

describe("FileSystem connector", () => {
  it("produces no JAR for filesystem sink", () => {
    const source = KafkaSource({
      topic: "orders",
      format: "json",
      schema: OrderSchema,
    })

    const sink = FileSystemSink({
      path: "s3://bucket/output",
      format: "parquet",
      children: [source],
    })

    const pipeline = Pipeline({ name: "test", children: [sink] })

    const result = resolveConnectors(pipeline, { flinkVersion: "2.0" })
    const artifactIds = result.jars.map((j) => j.artifact.artifactId)
    // Kafka JAR from source, Parquet format JAR from sink format, but no filesystem JAR
    expect(artifactIds).toContain("flink-sql-connector-kafka")
    expect(artifactIds).not.toContain("flink-sql-connector-filesystem")
  })
})

// ── Air-gapped mirror ───────────────────────────────────────────────

describe("Maven mirror URL substitution", () => {
  it("uses mirror URL in download URLs", () => {
    const source = KafkaSource({
      topic: "orders",
      schema: OrderSchema,
    })

    const sink = KafkaSink({
      topic: "output",
      children: [source],
    })

    const pipeline = Pipeline({ name: "test", children: [sink] })

    const result = resolveConnectors(pipeline, {
      flinkVersion: "2.0",
      mavenMirror: "https://nexus.internal/maven-central",
    })

    for (const jar of result.jars) {
      expect(jar.downloadUrl).toContain("https://nexus.internal/maven-central")
      expect(jar.downloadUrl).not.toContain("repo1.maven.org")
    }
  })
})

// ── Custom Artifacts ────────────────────────────────────────────────

describe("custom artifacts from config", () => {
  it("includes user-provided custom artifacts", () => {
    const source = KafkaSource({
      topic: "orders",
      schema: OrderSchema,
    })

    const sink = KafkaSink({
      topic: "output",
      children: [source],
    })

    const pipeline = Pipeline({ name: "test", children: [sink] })

    const result = resolveConnectors(pipeline, {
      flinkVersion: "2.0",
      customArtifacts: [
        { groupId: "com.example", artifactId: "custom-udf", version: "1.0.0" },
      ],
    })

    const artifactIds = result.jars.map((j) => j.artifact.artifactId)
    expect(artifactIds).toContain("custom-udf")
    expect(artifactIds).toContain("flink-sql-connector-kafka")
  })
})

// ── Provenance Tracking ─────────────────────────────────────────────

describe("provenance tracking", () => {
  it("tracks which components needed each JAR", () => {
    const source = KafkaSource({
      topic: "orders",
      schema: OrderSchema,
    })

    const sink = KafkaSink({
      topic: "output",
      children: [source],
    })

    const pipeline = Pipeline({ name: "test", children: [sink] })

    const result = resolveConnectors(pipeline, { flinkVersion: "2.0" })
    const kafkaJar = result.jars.find(
      (j) => j.artifact.artifactId === "flink-sql-connector-kafka",
    )
    expect(kafkaJar).toBeDefined()
    // Both the source and sink should be in provenance
    expect(kafkaJar?.provenance.length).toBeGreaterThanOrEqual(1)
  })
})

// ── UDF JAR Inclusion ─────────────────────────────────────────────

describe("UDF JAR inclusion", () => {
  it("includes UDF JAR in resolved JARs", () => {
    const udf = UDF({
      name: "my_hash",
      className: "com.mycompany.HashFunction",
      jarPath: "/opt/flink/lib/udf.jar",
    })

    const source = KafkaSource({
      topic: "orders",
      schema: OrderSchema,
    })

    const sink = KafkaSink({
      topic: "output",
      children: [source],
    })

    const pipeline = Pipeline({ name: "test", children: [udf, sink] })

    const result = resolveConnectors(pipeline, { flinkVersion: "2.0" })
    const udfJar = result.jars.find((j) => j.artifact.artifactId === "udf")
    expect(udfJar).toBeDefined()
    expect(udfJar?.jarName).toBe("udf.jar")
    expect(udfJar?.downloadUrl).toBe("file:///opt/flink/lib/udf.jar")
  })

  it("de-duplicates same UDF JAR from multiple UDFs", () => {
    const udf1 = UDF({
      name: "func_a",
      className: "com.example.FuncA",
      jarPath: "/opt/flink/lib/shared-udf.jar",
    })

    const udf2 = UDF({
      name: "func_b",
      className: "com.example.FuncB",
      jarPath: "/opt/flink/lib/shared-udf.jar",
    })

    const source = KafkaSource({
      topic: "events",
      schema: OrderSchema,
    })

    const sink = KafkaSink({
      topic: "output",
      children: [source],
    })

    const pipeline = Pipeline({ name: "test", children: [udf1, udf2, sink] })

    const result = resolveConnectors(pipeline, { flinkVersion: "2.0" })
    const udfJars = result.jars.filter((j) => j.artifact.groupId === "local")
    expect(udfJars).toHaveLength(1)
    expect(udfJars[0].jarName).toBe("shared-udf.jar")
  })
})

// ── Postgres CDC Pipeline Connector ─────────────────────────────────

describe("PostgresCdcPipelineSource connector resolution", () => {
  it("resolves flink-cdc-pipeline-connector-postgres:3.6.0 at Flink 1.20", () => {
    const source = PostgresCdcPipelineSource({
      hostname: "pg-primary",
      database: "shop",
      username: "postgres",
      password: secretRef("pg-primary-password"),
      schemaList: ["public"],
      tableList: ["public.orders"],
    })

    const pipeline = Pipeline({
      name: "shop-cdc",
      children: [FileSystemSink({ path: "/tmp/ignored", children: [source] })],
    })

    const result = resolveConnectors(pipeline, { flinkVersion: "1.20" })
    const cdcJar = result.jars.find(
      (j) => j.artifact.artifactId === "flink-cdc-pipeline-connector-postgres",
    )
    expect(cdcJar).toBeDefined()
    expect(cdcJar?.artifact.version).toBe("3.6.0")
    expect(cdcJar?.artifact.groupId).toBe("org.apache.flink")
  })

  it("resolves the same coordinate at Flink 2.0", () => {
    const source = PostgresCdcPipelineSource({
      hostname: "pg-primary",
      database: "shop",
      username: "postgres",
      password: secretRef("pg-primary-password"),
      schemaList: ["public"],
      tableList: ["public.orders"],
    })

    const pipeline = Pipeline({
      name: "shop-cdc",
      children: [FileSystemSink({ path: "/tmp/ignored", children: [source] })],
    })

    const result = resolveConnectors(pipeline, { flinkVersion: "2.0" })
    const cdcJar = result.jars.find(
      (j) => j.artifact.artifactId === "flink-cdc-pipeline-connector-postgres",
    )
    expect(cdcJar?.artifact.version).toBe("3.6.0")
  })

  it("attributes provenance to the CDC source node", () => {
    const source = PostgresCdcPipelineSource({
      hostname: "pg-primary",
      database: "shop",
      username: "postgres",
      password: secretRef("pg-primary-password"),
      schemaList: ["public"],
      tableList: ["public.orders"],
    })

    const pipeline = Pipeline({
      name: "shop-cdc",
      children: [FileSystemSink({ path: "/tmp/ignored", children: [source] })],
    })

    const result = resolveConnectors(pipeline, { flinkVersion: "2.0" })
    const cdcJar = result.jars.find(
      (j) => j.artifact.artifactId === "flink-cdc-pipeline-connector-postgres",
    )
    expect(cdcJar?.provenance).toContain(source.id)
  })
})

// ── Fluss CDC Pipeline Connector / branch isolation ─────────────────

describe("FlussSink branch-aware artifact resolution", () => {
  it("resolves the Pipeline Connector Fluss artifact at Flink 1.20", () => {
    const catalog = FlussCatalog({
      name: "fluss",
      bootstrapServers: "fluss:9123",
    })
    const source = PostgresCdcPipelineSource({
      hostname: "pg-primary",
      database: "shop",
      username: "postgres",
      password: secretRef("pg-primary-password"),
      schemaList: ["public"],
      tableList: ["public.orders"],
    })
    const sink = FlussSink({
      catalog: catalog.handle,
      database: "shop",
      table: "orders",
      primaryKey: ["order_id"],
      children: [source],
    })
    const pipeline = Pipeline({
      name: "pg-fluss",
      children: [catalog.node, sink],
    })

    const result = resolveConnectors(pipeline, { flinkVersion: "1.20" })
    const flussJar = result.jars.find(
      (j) => j.artifact.artifactId === "flink-cdc-pipeline-connector-fluss",
    )
    expect(flussJar).toBeDefined()
    expect(flussJar?.artifact.version).toBe("3.6.0")
    expect(flussJar?.artifact.groupId).toBe("org.apache.flink")
  })

  it("resolves both postgres and fluss Pipeline Connector artifacts together", () => {
    const catalog = FlussCatalog({
      name: "fluss",
      bootstrapServers: "fluss:9123",
    })
    const source = PostgresCdcPipelineSource({
      hostname: "pg-primary",
      database: "shop",
      username: "postgres",
      password: secretRef("pg-primary-password"),
      schemaList: ["public"],
      tableList: ["public.orders"],
    })
    const sink = FlussSink({
      catalog: catalog.handle,
      database: "shop",
      table: "orders",
      primaryKey: ["order_id"],
      children: [source],
    })
    const pipeline = Pipeline({
      name: "pg-fluss",
      children: [catalog.node, sink],
    })

    const result = resolveConnectors(pipeline, { flinkVersion: "2.2" })
    const artifactIds = result.jars.map((j) => j.artifact.artifactId)
    expect(artifactIds).toContain("flink-cdc-pipeline-connector-postgres")
    expect(artifactIds).toContain("flink-cdc-pipeline-connector-fluss")
    // Critical: the SQL-branch Fluss connector must NOT leak into a
    // Pipeline-YAML pipeline — that would produce a classpath collision.
    expect(artifactIds).not.toContain("fluss-flink-2.2")
    expect(result.conflicts).toHaveLength(0)
  })

  it("Pipeline-YAML branch does NOT pull the Flink-SQL Fluss connector", () => {
    const catalog = FlussCatalog({
      name: "fluss",
      bootstrapServers: "fluss:9123",
    })
    const source = PostgresCdcPipelineSource({
      hostname: "pg-primary",
      database: "shop",
      username: "postgres",
      password: secretRef("pg-primary-password"),
      schemaList: ["public"],
      tableList: ["public.orders"],
    })
    const sink = FlussSink({
      catalog: catalog.handle,
      database: "shop",
      table: "orders",
      primaryKey: ["order_id"],
      children: [source],
    })
    const pipeline = Pipeline({
      name: "pg-fluss",
      children: [catalog.node, sink],
    })

    const result = resolveConnectors(pipeline, { flinkVersion: "2.2" })
    const sqlFluss = result.jars.find((j) =>
      j.artifact.artifactId.startsWith("fluss-flink-"),
    )
    expect(sqlFluss).toBeUndefined()
  })

  it("SQL-branch FlussSink resolves the SQL connector and NOT the Pipeline Connector", () => {
    const OrderSchema = Schema({
      fields: {
        order_id: Field.BIGINT(),
      },
    })
    const catalog = FlussCatalog({
      name: "fluss",
      bootstrapServers: "fluss:9123",
    })
    const source = KafkaSource({
      topic: "orders",
      format: "json",
      schema: OrderSchema,
    })
    const sink = FlussSink({
      catalog: catalog.handle,
      database: "shop",
      table: "orders",
      primaryKey: ["order_id"],
      children: [source],
    })
    const pipeline = Pipeline({
      name: "kafka-fluss",
      children: [catalog.node, sink],
    })

    const result = resolveConnectors(pipeline, { flinkVersion: "2.2" })
    const artifactIds = result.jars.map((j) => j.artifact.artifactId)
    expect(artifactIds).toContain("fluss-flink-2.2")
    expect(artifactIds).not.toContain("flink-cdc-pipeline-connector-fluss")
  })
})

// ── Apache Fluss 0.9.0-incubating GAV ────────────────────────────────

describe("Fluss connector registry GAV", () => {
  it("resolves fluss-flink-1.20 at Flink 1.20", () => {
    const artifacts = resolveConnectorArtifacts("fluss", "1.20")
    expect(artifacts).toContainEqual({
      groupId: "org.apache.fluss",
      artifactId: "fluss-flink-1.20",
      version: "0.9.0-incubating",
    })
  })

  it("resolves fluss-flink-2.2 at Flink 2.2", () => {
    const artifacts = resolveConnectorArtifacts("fluss", "2.2")
    expect(artifacts).toContainEqual({
      groupId: "org.apache.fluss",
      artifactId: "fluss-flink-2.2",
      version: "0.9.0-incubating",
    })
  })

  it.each([
    "2.0",
    "2.1",
  ] as const)("returns empty at Flink %s (Fluss 0.9.0 publishes only 1.20 and 2.2 artifacts)", (flinkVersion) => {
    const artifacts = resolveConnectorArtifacts("fluss", flinkVersion)
    expect(artifacts).toHaveLength(0)
  })

  it("does not resolve the deprecated Alibaba 0.6.0 coordinate", () => {
    for (const v of ["1.20", "2.0", "2.1", "2.2"] as const) {
      const artifacts = resolveConnectorArtifacts("fluss", v)
      const hasOldGav = artifacts.some(
        (a) =>
          a.groupId === "com.alibaba.fluss" &&
          a.artifactId === "fluss-connector-flink" &&
          a.version === "0.6.0",
      )
      expect(hasOldGav).toBe(false)
    }
  })
})
