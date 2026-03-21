import { IcebergCatalog } from "@/components/catalogs"
import { Pipeline } from "@/components/pipeline"
import { IcebergSink } from "@/components/sinks"
import { KafkaSource } from "@/components/sources"
import { createElement } from "@/core/jsx-runtime"
import { Field, Schema } from "@/core/schema"

const OrderSchema = Schema({
  fields: {
    orderId: Field.BIGINT(),
    customerId: Field.BIGINT(),
    product: Field.STRING(),
    amount: Field.DECIMAL(10, 2),
    status: Field.STRING(),
    createdAt: Field.TIMESTAMP(3),
    updatedAt: Field.TIMESTAMP(3),
  },
  primaryKey: { columns: ["orderId"] },
})

const iceberg = IcebergCatalog({
  name: "lakehouse",
  catalogType: "rest",
  uri: "http://iceberg-rest:8181",
})

export default (
  <Pipeline
    name="cdc-to-lakehouse"
    mode="streaming"
    parallelism={2}
    stateBackend="rocksdb"
    checkpoint={{ interval: "30s", mode: "exactly-once" }}
    flinkConfig={{
      "s3.endpoint": "http://seaweedfs.flink-demo.svc:8333",
      "s3.path.style.access": "true",
      "s3.access-key": "admin",
      "s3.secret-key": "admin",
      "state.checkpoints.dir": "s3://flink-state/checkpoints/cdc-to-lakehouse",
      "state.savepoints.dir": "s3://flink-state/savepoints/cdc-to-lakehouse",
    }}
  >
    {iceberg.node}
    <KafkaSource
      topic="dbserver1.inventory.orders"
      schema={OrderSchema}
      format="debezium-json"
      bootstrapServers="kafka:9092"
      consumerGroup="cdc-lakehouse"
    />
    <IcebergSink
      catalog={iceberg.handle}
      database="inventory"
      table="orders"
      primaryKey={["orderId"]}
      formatVersion={2}
      upsertEnabled
    />
  </Pipeline>
)
