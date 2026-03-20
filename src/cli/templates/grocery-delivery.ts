import type { ScaffoldOptions, TemplateFile } from "@/cli/commands/new.js"
import { sharedFiles } from "./shared.js"

export function getGroceryDeliveryTemplates(
  opts: ScaffoldOptions,
): TemplateFile[] {
  return [
    ...sharedFiles(opts),
    {
      path: "schemas/grocery.ts",
      content: `import { Schema, Field } from 'flink-reactor';

export const GroceryOrderSchema = Schema({
  fields: {
    orderId: Field.STRING(),
    storeId: Field.STRING(),
    customerId: Field.STRING(),
    itemCount: Field.INT(),
    totalAmount: Field.DOUBLE(),
    orderTime: Field.TIMESTAMP(3),
  },
  watermark: { column: 'orderTime', expression: "orderTime - INTERVAL '5' SECOND" },
});

export const StoreInventorySchema = Schema({
  fields: {
    storeId: Field.STRING(),
    productId: Field.STRING(),
    stockLevel: Field.INT(),
    substitutionId: Field.STRING(),
    updateTime: Field.TIMESTAMP(3),
  },
  primaryKey: { columns: ['storeId', 'productId'] },
});

export const RatingSchema = Schema({
  fields: {
    orderId: Field.STRING(),
    storeId: Field.STRING(),
    shopperRating: Field.DOUBLE(),
    storeRating: Field.DOUBLE(),
    itemQuality: Field.DOUBLE(),
    ratingTime: Field.TIMESTAMP(3),
  },
  watermark: { column: 'ratingTime', expression: "ratingTime - INTERVAL '5' SECOND" },
});
`,
    },
    {
      path: "pipelines/grocery-order-fulfillment/index.tsx",
      content: `import {
  Pipeline, KafkaSource, KafkaSink, JdbcSink,
  TemporalJoin, Route, StatementSet,
} from 'flink-reactor';
import { GroceryOrderSchema, StoreInventorySchema } from '@/schemas/grocery';

export default (
  <Pipeline
    name="grocery-order-fulfillment"
    mode="streaming"
    parallelism={4}
    stateBackend="rocksdb"
    checkpoint={{ interval: "30s", mode: "exactly-once" }}
    flinkConfig={{
      "state.checkpoints.dir": "s3://flink-state/checkpoints/grocery-order-fulfillment",
      "state.savepoints.dir": "s3://flink-state/savepoints/grocery-order-fulfillment",
      "s3.endpoint": "http://seaweedfs.flink-demo.svc:8333",
      "s3.path.style.access": "true",
    }}
  >
    <KafkaSource topic="grocery.orders" schema={GroceryOrderSchema} bootstrapServers="kafka:9092" consumerGroup="grocery-fulfillment" />
    <TemporalJoin
      rightSource={<KafkaSource topic="grocery.store-inventory" schema={StoreInventorySchema} format="debezium-json" bootstrapServers="kafka:9092" consumerGroup="grocery-inventory" />}
      on="storeId"
    />
    <StatementSet>
      <Route>
        <JdbcSink table="fulfillment_queue" url="jdbc:postgresql://postgres:5432/flink_sink" condition="stockLevel > 0" />
        <KafkaSink topic="grocery.substitution-alerts" bootstrapServers="kafka:9092" condition="stockLevel = 0" />
      </Route>
    </StatementSet>
  </Pipeline>
);
`,
    },
    {
      path: "pipelines/grocery-store-rankings/index.tsx",
      content: `import {
  Pipeline, KafkaSource, JdbcSink,
  Deduplicate, TumbleWindow, Aggregate,
} from 'flink-reactor';
import { RatingSchema } from '@/schemas/grocery';

export default (
  <Pipeline
    name="grocery-store-rankings"
    mode="streaming"
    parallelism={2}
    stateBackend="rocksdb"
    checkpoint={{ interval: "30s", mode: "exactly-once" }}
    flinkConfig={{
      "state.checkpoints.dir": "s3://flink-state/checkpoints/grocery-store-rankings",
      "state.savepoints.dir": "s3://flink-state/savepoints/grocery-store-rankings",
      "s3.endpoint": "http://seaweedfs.flink-demo.svc:8333",
      "s3.path.style.access": "true",
    }}
  >
    <KafkaSource topic="grocery.ratings" schema={RatingSchema} bootstrapServers="kafka:9092" consumerGroup="grocery-rankings" />
    <Deduplicate on="orderId" keepFirst />
    <TumbleWindow size="15 MINUTE" on="ratingTime" />
    <Aggregate
      groupBy={['storeId']}
      select={{
        storeId: 'storeId',
        avgRating: 'AVG(storeRating)',
        ratingCount: 'COUNT(*)',
        windowEnd: 'WINDOW_END',
      }}
    />
    <JdbcSink table="store_rankings" url="jdbc:postgresql://postgres:5432/flink_sink" mode="upsert" />
  </Pipeline>
);
`,
    },
  ]
}
