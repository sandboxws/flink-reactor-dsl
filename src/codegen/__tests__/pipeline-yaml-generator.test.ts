import { beforeEach, describe, expect, it } from "vitest"
import { generatePipelineYaml } from "@/codegen/pipeline-yaml-generator.js"
import {
  FlussCatalog,
  IcebergCatalog,
  PaimonCatalog,
} from "@/components/catalogs.js"
import { Pipeline } from "@/components/pipeline.js"
import {
  FlussSink,
  IcebergSink,
  KafkaSink,
  PaimonSink,
} from "@/components/sinks.js"
import { KafkaSource, PostgresCdcPipelineSource } from "@/components/sources.js"
import { resetNodeIdCounter } from "@/core/jsx-runtime.js"
import { Field, Schema } from "@/core/schema.js"
import { secretRef } from "@/core/secret-ref.js"

beforeEach(() => {
  resetNodeIdCounter()
})

const OrderSchema = Schema({
  fields: {
    order_id: Field.BIGINT(),
    product: Field.STRING(),
    amount: Field.DECIMAL(10, 2),
  },
})

describe("generatePipelineYaml", () => {
  it("returns null for pipelines without a Pipeline Connector source", () => {
    const source = KafkaSource({
      topic: "orders",
      format: "debezium-json",
      schema: OrderSchema,
    })
    const sink = KafkaSink({ topic: "out", children: [source] })
    const pipeline = Pipeline({ name: "sql-pipeline", children: [sink] })

    expect(generatePipelineYaml(pipeline)).toBeNull()
  })

  it("emits source/sink/pipeline stanzas for Postgres → Iceberg", () => {
    const catalog = IcebergCatalog({
      name: "lake",
      catalogType: "rest",
      uri: "http://iceberg-rest:8181",
    })
    const source = PostgresCdcPipelineSource({
      hostname: "pg-primary",
      port: 5432,
      database: "shop",
      username: "postgres",
      password: secretRef("pg-primary-password"),
      schemaList: ["public"],
      tableList: ["public.orders"],
      snapshotMode: "initial",
      startupMode: "initial",
      chunkSize: 100000,
    })
    const sink = IcebergSink({
      catalog: catalog.handle,
      database: "shop",
      table: "orders",
      formatVersion: 2,
      upsertEnabled: true,
      primaryKey: ["order_id"],
      children: [source],
    })
    const pipeline = Pipeline({
      name: "shop-orders-cdc",
      parallelism: 4,
      children: [catalog.node, sink],
    })

    const yaml = generatePipelineYaml(pipeline)
    expect(yaml).toMatchSnapshot()
  })

  it("renders SecretRef passwords as ${env:VAR} placeholders", () => {
    const catalog = IcebergCatalog({
      name: "lake",
      catalogType: "rest",
      uri: "http://iceberg-rest:8181",
    })
    const source = PostgresCdcPipelineSource({
      hostname: "pg-primary",
      database: "shop",
      username: "postgres",
      password: secretRef("pg-primary-password"),
      schemaList: ["public"],
      tableList: ["public.orders"],
    })
    const sink = IcebergSink({
      catalog: catalog.handle,
      database: "shop",
      table: "orders",
      formatVersion: 2,
      upsertEnabled: true,
      children: [source],
    })
    const pipeline = Pipeline({
      name: "shop-orders-cdc",
      children: [catalog.node, sink],
    })

    const yaml = generatePipelineYaml(pipeline)
    // The placeholder is the form Flink CDC substitutes at startup.
    // YAML 1.2 plain scalars happily contain `$` `{` `}` in block context,
    // so the value emits unquoted; Flink CDC reads either equivalently.
    expect(yaml).toContain("password: ${env:PG_PRIMARY_PASSWORD}")
    expect(yaml).not.toContain("hunter2")
  })

  it("uses explicit envName override when provided", () => {
    const catalog = IcebergCatalog({
      name: "lake",
      catalogType: "rest",
      uri: "http://iceberg-rest:8181",
    })
    const source = PostgresCdcPipelineSource({
      hostname: "pg-primary",
      database: "shop",
      username: "postgres",
      password: secretRef("foo", "password", "CUSTOM_VAR"),
      schemaList: ["public"],
      tableList: ["public.orders"],
    })
    const sink = IcebergSink({
      catalog: catalog.handle,
      database: "shop",
      table: "orders",
      formatVersion: 2,
      upsertEnabled: true,
      children: [source],
    })
    const pipeline = Pipeline({
      name: "shop",
      children: [catalog.node, sink],
    })

    const yaml = generatePipelineYaml(pipeline)
    expect(yaml).toContain("password: ${env:CUSTOM_VAR}")
  })

  it("derives deterministic slot/publication names when not provided", () => {
    const catalog = IcebergCatalog({
      name: "lake",
      catalogType: "rest",
      uri: "http://iceberg-rest:8181",
    })
    const source = PostgresCdcPipelineSource({
      hostname: "pg-primary",
      database: "shop",
      username: "postgres",
      password: secretRef("pg-primary-password"),
      schemaList: ["public"],
      tableList: ["public.orders"],
    })
    const sink = IcebergSink({
      catalog: catalog.handle,
      database: "shop",
      table: "orders",
      formatVersion: 2,
      upsertEnabled: true,
      children: [source],
    })
    const pipeline = Pipeline({
      name: "shop-orders-cdc",
      children: [catalog.node, sink],
    })

    const yaml = generatePipelineYaml(pipeline) as string
    expect(yaml).toContain("slot.name: fr_shop_orders_cdc_slot")
    expect(yaml).toContain("publication.name: fr_shop_orders_cdc_pub")
  })

  it("is deterministic across runs (byte-equal output)", () => {
    const build = () => {
      resetNodeIdCounter()
      const catalog = IcebergCatalog({
        name: "lake",
        catalogType: "rest",
        uri: "http://iceberg-rest:8181",
      })
      const source = PostgresCdcPipelineSource({
        hostname: "pg-primary",
        database: "shop",
        username: "postgres",
        password: secretRef("pg-primary-password"),
        schemaList: ["public"],
        tableList: ["public.orders"],
      })
      const sink = IcebergSink({
        catalog: catalog.handle,
        database: "shop",
        table: "orders",
        formatVersion: 2,
        upsertEnabled: true,
        children: [source],
      })
      return Pipeline({
        name: "shop",
        children: [catalog.node, sink],
      })
    }

    const yaml1 = generatePipelineYaml(build())
    const yaml2 = generatePipelineYaml(build())
    expect(yaml1).toBe(yaml2)
  })

  // ── IcebergSink MoR prop threading ───────────────────────────────

  function buildMorPipeline(
    extraSinkProps: Record<string, unknown>,
  ): ReturnType<typeof Pipeline> {
    const catalog = IcebergCatalog({
      name: "lake",
      catalogType: "rest",
      uri: "http://iceberg-rest:8181",
    })
    const source = PostgresCdcPipelineSource({
      hostname: "pg-primary",
      database: "shop",
      username: "postgres",
      password: secretRef("pg-primary-password"),
      schemaList: ["public"],
      tableList: ["public.orders"],
    })
    const sink = IcebergSink({
      catalog: catalog.handle,
      database: "shop",
      table: "orders",
      formatVersion: 2,
      upsertEnabled: true,
      primaryKey: ["order_id"],
      ...extraSinkProps,
      children: [source],
    })
    return Pipeline({
      name: "shop-orders-cdc",
      children: [catalog.node, sink],
    })
  }

  it("threads equalityFieldColumns into the sink stanza", () => {
    const yaml = generatePipelineYaml(
      buildMorPipeline({ equalityFieldColumns: ["order_id", "region"] }),
    ) as string
    // Block-context plain scalars in YAML 1.2 can contain commas; both
    // quoted and unquoted forms parse identically for Flink CDC.
    expect(yaml).toContain(
      "table.properties.equality-field-columns: order_id,region",
    )
  })

  it("threads commitIntervalSeconds as commit-interval-ms (×1000)", () => {
    const yaml = generatePipelineYaml(
      buildMorPipeline({ commitIntervalSeconds: 2 }),
    ) as string
    expect(yaml).toContain("table.properties.commit-interval-ms: '2000'")
  })

  it("threads writeDistributionMode as write.distribution-mode", () => {
    const yaml = generatePipelineYaml(
      buildMorPipeline({ writeDistributionMode: "hash" }),
    ) as string
    expect(yaml).toContain("table.properties.write.distribution-mode: hash")
  })

  it("threads targetFileSizeMB as write.target-file-size-bytes (×1MiB)", () => {
    const yaml = generatePipelineYaml(
      buildMorPipeline({ targetFileSizeMB: 256 }),
    ) as string
    expect(yaml).toContain(
      "table.properties.write.target-file-size-bytes: '268435456'",
    )
  })

  it("threads writeParquetCompression as write.parquet.compression-codec", () => {
    const yaml = generatePipelineYaml(
      buildMorPipeline({ writeParquetCompression: "zstd" }),
    ) as string
    expect(yaml).toContain(
      "table.properties.write.parquet.compression-codec: zstd",
    )
  })

  it("emits no MoR keys when no MoR props are set", () => {
    const catalog = IcebergCatalog({
      name: "lake",
      catalogType: "rest",
      uri: "http://iceberg-rest:8181",
    })
    const source = PostgresCdcPipelineSource({
      hostname: "pg-primary",
      database: "shop",
      username: "postgres",
      password: secretRef("pg-primary-password"),
      schemaList: ["public"],
      tableList: ["public.orders"],
    })
    const sink = IcebergSink({
      catalog: catalog.handle,
      database: "shop",
      table: "orders",
      formatVersion: 2,
      upsertEnabled: true,
      primaryKey: ["order_id"],
      children: [source],
    })
    const pipeline = Pipeline({
      name: "shop",
      children: [catalog.node, sink],
    })
    const yaml = generatePipelineYaml(pipeline) as string
    expect(yaml).not.toContain("commit-interval-ms")
    expect(yaml).not.toContain("write.distribution-mode")
    expect(yaml).not.toContain("write.target-file-size-bytes")
    expect(yaml).not.toContain("write.parquet.compression-codec")
  })

  it("emits the full MoR configuration snapshot", () => {
    const pipeline = buildMorPipeline({
      equalityFieldColumns: ["order_id"],
      commitIntervalSeconds: 10,
      writeDistributionMode: "hash",
      targetFileSizeMB: 256,
      writeParquetCompression: "zstd",
    })
    expect(generatePipelineYaml(pipeline)).toMatchSnapshot()
  })

  it("emits a paimon sink stanza for PaimonSink", () => {
    const catalog = PaimonCatalog({
      name: "lake",
      warehouse: "s3://lake/warehouse",
    })
    const source = PostgresCdcPipelineSource({
      hostname: "pg-primary",
      database: "shop",
      username: "postgres",
      password: secretRef("pg-primary-password"),
      schemaList: ["public"],
      tableList: ["public.orders"],
    })
    const sink = PaimonSink({
      catalog: catalog.handle,
      database: "shop",
      table: "orders",
      primaryKey: ["order_id"],
      mergeEngine: "deduplicate",
      children: [source],
    })
    const pipeline = Pipeline({
      name: "shop",
      children: [catalog.node, sink],
    })

    const yaml = generatePipelineYaml(pipeline) as string
    expect(yaml).toContain("type: paimon")
    expect(yaml).toContain("table.properties.primary-key: order_id")
    expect(yaml).toContain("table.properties.merge-engine: deduplicate")
  })

  it("throws when connected to a non-supported sink", () => {
    const source = PostgresCdcPipelineSource({
      hostname: "pg-primary",
      database: "shop",
      username: "postgres",
      password: secretRef("pg-primary-password"),
      schemaList: ["public"],
      tableList: ["public.orders"],
    })
    const sink = KafkaSink({ topic: "orders", children: [source] })
    const pipeline = Pipeline({
      name: "shop",
      children: [sink],
    })

    expect(() => generatePipelineYaml(pipeline)).toThrow(
      /Pipeline Connector cannot emit a sink stanza for component 'KafkaSink'/,
    )
  })

  // ── FlussSink stanza ─────────────────────────────────────────────

  it("emits a fluss sink stanza for PostgresCdcPipelineSource → FlussSink", () => {
    const catalog = FlussCatalog({
      name: "fluss",
      bootstrapServers: "fluss-coordinator:9123",
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
      buckets: 8,
      children: [source],
    })
    const pipeline = Pipeline({
      name: "shop-orders-cdc",
      parallelism: 4,
      children: [catalog.node, sink],
    })

    const yaml = generatePipelineYaml(pipeline) as string
    expect(yaml).toContain("type: fluss")
    expect(yaml).toContain("bootstrap.servers: fluss-coordinator:9123")
    expect(yaml).toContain("table.properties.bucket.num: '8'")
    expect(yaml).toContain("table.properties.bucket.key: order_id")
    expect(yaml).toContain("schema.evolution.behavior: lenient")
    expect(yaml).toMatchSnapshot()
  })

  it("forwards SASL credentials from FlussCatalog to the sink stanza", () => {
    const catalog = FlussCatalog({
      name: "fluss",
      bootstrapServers: "fluss-coordinator:9123",
      securityProtocol: "SASL_PLAINTEXT",
      saslMechanism: "PLAIN",
      saslUsername: "fr-writer",
      saslPassword: secretRef("fluss-sasl-password"),
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
      name: "shop-orders-cdc",
      children: [catalog.node, sink],
    })

    const yaml = generatePipelineYaml(pipeline) as string
    expect(yaml).toContain(
      "properties.client.security.protocol: SASL_PLAINTEXT",
    )
    expect(yaml).toContain("properties.client.security.sasl.mechanism: PLAIN")
    expect(yaml).toContain(
      "properties.client.security.sasl.username: fr-writer",
    )
    expect(yaml).toContain(
      "properties.client.security.sasl.password: ${env:FLUSS_SASL_PASSWORD}",
    )
    expect(yaml).toMatchSnapshot()
  })

  it("schema.evolution.behavior defaults to lenient and is overridable", () => {
    const buildPipeline = (
      schemaEvolution?: "lenient" | "strict",
    ): ReturnType<typeof Pipeline> => {
      resetNodeIdCounter()
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
      const sinkProps = {
        catalog: catalog.handle,
        database: "shop",
        table: "orders",
        primaryKey: ["order_id"],
        ...(schemaEvolution !== undefined ? { schemaEvolution } : {}),
        children: [source],
      } as Parameters<typeof FlussSink>[0]
      const sink = FlussSink(sinkProps)
      return Pipeline({
        name: "shop",
        children: [catalog.node, sink],
      })
    }

    const defaultYaml = generatePipelineYaml(buildPipeline()) as string
    expect(defaultYaml).toContain("schema.evolution.behavior: lenient")

    const strictYaml = generatePipelineYaml(buildPipeline("strict")) as string
    expect(strictYaml).toContain("schema.evolution.behavior: strict")
  })

  it("omits bucket.key when primaryKey is empty (Log table fallthrough)", () => {
    // The cross-node validator rejects FlussSink without primaryKey downstream
    // of PostgresCdcPipelineSource, but the YAML emitter's contract is "omit
    // the key when primaryKey is empty" so Fluss falls back to round-robin
    // bucketing — we cover the emitter shape independently from validation.
    const catalog = FlussCatalog({
      name: "fluss",
      bootstrapServers: "fluss:9123",
    })
    const cdcSource = PostgresCdcPipelineSource({
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
      buckets: 4,
      children: [cdcSource],
    })
    const pipeline = Pipeline({
      name: "shop",
      children: [catalog.node, sink],
    })

    const yaml = generatePipelineYaml(pipeline) as string
    expect(yaml).not.toContain("bucket.key")
    expect(yaml).toContain("table.properties.bucket.num: '4'")
  })
})
