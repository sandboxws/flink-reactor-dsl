import { beforeEach, describe, expect, it } from "vitest"
import { generatePipelineYaml } from "@/codegen/pipeline-yaml-generator.js"
import { IcebergCatalog, PaimonCatalog } from "@/components/catalogs.js"
import { Pipeline } from "@/components/pipeline.js"
import { IcebergSink, KafkaSink, PaimonSink } from "@/components/sinks.js"
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
    // toYaml quotes strings that contain YAML special characters; the
    // resulting single-quoted literal is what Flink CDC substitutes.
    expect(yaml).toContain("password: '${env:PG_PRIMARY_PASSWORD}'")
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
    expect(yaml).toContain("password: '${env:CUSTOM_VAR}'")
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
})
