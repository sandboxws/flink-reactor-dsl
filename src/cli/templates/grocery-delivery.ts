import type { ScaffoldOptions, TemplateFile } from "@/cli/commands/new.js"
import { sharedFiles } from "./shared.js"

export function getGroceryDeliveryTemplates(
  opts: ScaffoldOptions,
): TemplateFile[] {
  return [
    ...sharedFiles(opts),
    {
      path: "flink-reactor.config.ts",
      content: `import { defineConfig } from '@flink-reactor/dsl';

export default defineConfig({
  flink: { version: '${opts.flinkVersion}' },

  environments: {
    minikube: {
      cluster: { url: 'http://localhost:8081' },
      kafka: { bootstrapServers: 'kafka:9092' },
      sim: {
        init: {
          kafka: {
            topics: ['grocery.substitution-alerts'],
            catalogs: [{
              name: 'grocery',
              tables: [
                {
                  table: 'orders',
                  topic: 'grocery.orders',
                  format: 'json',
                  columns: {
                    orderId: 'STRING',
                    storeId: 'STRING',
                    customerId: 'STRING',
                    itemCount: 'INT',
                    totalAmount: 'DOUBLE',
                    orderTime: 'TIMESTAMP(3)',
                  },
                  watermark: { column: 'orderTime', expression: "orderTime - INTERVAL '5' SECOND" },
                },
                {
                  table: 'store_inventory',
                  topic: 'grocery.store-inventory',
                  format: 'debezium-json',
                  columns: {
                    storeId: 'STRING',
                    productId: 'STRING',
                    stockLevel: 'INT',
                    substitutionId: 'STRING',
                    updateTime: 'TIMESTAMP(3)',
                  },
                  primaryKey: ['storeId', 'productId'],
                },
                {
                  table: 'ratings',
                  topic: 'grocery.ratings',
                  format: 'json',
                  columns: {
                    orderId: 'STRING',
                    storeId: 'STRING',
                    shopperRating: 'DOUBLE',
                    storeRating: 'DOUBLE',
                    itemQuality: 'DOUBLE',
                    ratingTime: 'TIMESTAMP(3)',
                  },
                  watermark: { column: 'ratingTime', expression: "ratingTime - INTERVAL '5' SECOND" },
                },
              ],
            }],
          },
          jdbc: {
            catalogs: [{
              name: 'flink_sink',
              baseUrl: 'jdbc:postgresql://postgres:5432/',
              defaultDatabase: 'flink_sink',
            }],
          },
        },
      },
      pipelines: { '*': { parallelism: 2 } },
    },
    production: {
      cluster: { url: 'https://flink-prod:8081' },
      kubernetes: { namespace: 'flink-prod' },
      pipelines: { '*': { parallelism: 4 } },
    },
  },
});
`,
    },
    {
      path: "schemas/grocery.ts",
      content: `import { Schema, Field } from '@flink-reactor/dsl';

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
  watermark: { column: 'updateTime', expression: "updateTime - INTERVAL '5' SECOND" },
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
  TemporalJoin, Route,
} from '@flink-reactor/dsl';
import { GroceryOrderSchema, StoreInventorySchema } from '@/schemas/grocery';

const orders = KafkaSource({
  topic: "grocery.orders",
  schema: GroceryOrderSchema,
  bootstrapServers: "kafka:9092",
  consumerGroup: "grocery-fulfillment",
});

const inventory = KafkaSource({
  topic: "grocery.store-inventory",
  schema: StoreInventorySchema,
  format: "debezium-json",
  bootstrapServers: "kafka:9092",
  consumerGroup: "grocery-inventory",
});

const enriched = TemporalJoin({
  stream: orders,
  temporal: inventory,
  on: "storeId = storeId",
  asOf: "orderTime",
});

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
    {orders}
    {inventory}
    {enriched}
    <Route>
      <Route.Branch condition="stockLevel > 0">
        <JdbcSink table="fulfillment_queue" url="jdbc:postgresql://postgres:5432/flink_sink" />
      </Route.Branch>
      <Route.Branch condition="stockLevel = 0">
        <KafkaSink topic="grocery.substitution-alerts" bootstrapServers="kafka:9092" />
      </Route.Branch>
    </Route>
  </Pipeline>
);
`,
    },
    {
      path: "pipelines/grocery-store-rankings/index.tsx",
      content: `import {
  Pipeline, KafkaSource, JdbcSink,
  Deduplicate, TumbleWindow, Aggregate,
} from '@flink-reactor/dsl';
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
    <Deduplicate key={['orderId']} order="ratingTime" keep="first" />
    <TumbleWindow size="15 MINUTE" on="ratingTime" />
    <Aggregate
      groupBy={['storeId']}
      select={{
        storeId: 'storeId',
        avgRating: 'AVG(storeRating)',
        ratingCount: 'COUNT(*)',
        windowEnd: 'window_end',
      }}
    />
    <JdbcSink table="store_rankings" url="jdbc:postgresql://postgres:5432/flink_sink" upsertMode keyFields={['storeId']} />
  </Pipeline>
);
`,
    },
  ]
}
