import type { ScaffoldOptions, TemplateFile } from "@/cli/commands/new.js"
import { sharedFiles } from "./shared.js"

export function getEcommerceTemplates(opts: ScaffoldOptions): TemplateFile[] {
  return [
    ...sharedFiles(opts),

    // ── Schemas ──────────────────────────────────────────────────────

    {
      path: "schemas/ecommerce.ts",
      content: `import { Schema, Field } from '@flink-reactor/dsl';

export const OrderSchema = Schema({
  fields: {
    orderId: Field.STRING(),
    customerId: Field.STRING(),
    amount: Field.DOUBLE(),
    currency: Field.STRING(),
    status: Field.STRING(),
    orderTime: Field.TIMESTAMP(3),
  },
  watermark: { column: 'orderTime', expression: "orderTime - INTERVAL '5' SECOND" },
});

export const OrderItemSchema = Schema({
  fields: {
    orderId: Field.STRING(),
    productId: Field.STRING(),
    quantity: Field.INT(),
    unitPrice: Field.DOUBLE(),
    itemTime: Field.TIMESTAMP(3),
  },
  watermark: { column: 'itemTime', expression: "itemTime - INTERVAL '5' SECOND" },
});

export const ProductSchema = Schema({
  fields: {
    productId: Field.STRING(),
    name: Field.STRING(),
    category: Field.STRING(),
    price: Field.DOUBLE(),
    stock: Field.INT(),
    updateTime: Field.TIMESTAMP(3),
  },
  primaryKey: { columns: ['productId'] },
});

export const CustomerSchema = Schema({
  fields: {
    customerId: Field.STRING(),
    name: Field.STRING(),
    email: Field.STRING(),
    tier: Field.STRING(),
    updateTime: Field.TIMESTAMP(3),
  },
  primaryKey: { columns: ['customerId'] },
});
`,
    },

    // ── Pipeline E1: Order Enrichment (3-way join) ──────────────────

    {
      path: "pipelines/ecom-order-enrichment/index.tsx",
      content: `import {
  Pipeline,
  KafkaSource,
  KafkaSink,
  IntervalJoin,
  TemporalJoin,
} from '@flink-reactor/dsl';
import { OrderSchema, OrderItemSchema, ProductSchema } from '@/schemas/ecommerce';

const orders = KafkaSource({
  topic: "ecom.orders",
  schema: OrderSchema,
  bootstrapServers: "kafka:9092",
  consumerGroup: "ecom-enrichment-orders",
});

const items = KafkaSource({
  topic: "ecom.order-items",
  schema: OrderItemSchema,
  bootstrapServers: "kafka:9092",
  consumerGroup: "ecom-enrichment-items",
});

const products = KafkaSource({
  topic: "ecom.products",
  schema: ProductSchema,
  format: "debezium-json",
  bootstrapServers: "kafka:9092",
  consumerGroup: "ecom-enrichment-products",
});

const ordersWithItems = IntervalJoin({
  left: orders,
  right: items,
  on: "orders.orderId = items.orderId",
  interval: { from: "orders.orderTime", to: "orders.orderTime + INTERVAL '30' SECOND" },
});

const enriched = TemporalJoin({
  stream: ordersWithItems,
  temporal: products,
  on: "productId = productId",
  asOf: "orderTime",
});

export default (
  <Pipeline
    name="ecom-order-enrichment"
    mode="streaming"
    parallelism={4}
    stateBackend="rocksdb"
    checkpoint={{ interval: "30s", mode: "exactly-once" }}
    restartStrategy={{ type: "fixed-delay", attempts: 3, delay: "10s" }}
    flinkConfig={{
      "state.checkpoints.dir": "s3://flink-state/checkpoints/ecom-order-enrichment",
      "state.savepoints.dir": "s3://flink-state/savepoints/ecom-order-enrichment",
      "s3.endpoint": "http://seaweedfs.flink-demo.svc:8333",
      "s3.path.style.access": "true",
    }}
  >
    {orders}
    {items}
    {products}
    {enriched}
    <KafkaSink
      topic="ecom.order-enriched"
      bootstrapServers="kafka:9092"
    />
  </Pipeline>
);
`,
    },

    // ── Pipeline E2: Revenue Analytics (sliding window + Top-N) ─────

    {
      path: "pipelines/ecom-revenue-analytics/index.tsx",
      content: `import {
  Pipeline,
  KafkaSource,
  KafkaSink,
  JdbcSink,
  SlideWindow,
  Aggregate,
  Route,
} from '@flink-reactor/dsl';
import { OrderSchema } from '@/schemas/ecommerce';

export default (
  <Pipeline
    name="ecom-revenue-analytics"
    mode="streaming"
    parallelism={4}
    stateBackend="rocksdb"
    checkpoint={{ interval: "30s", mode: "exactly-once" }}
    restartStrategy={{ type: "fixed-delay", attempts: 3, delay: "10s" }}
    flinkConfig={{
      "state.checkpoints.dir": "s3://flink-state/checkpoints/ecom-revenue-analytics",
      "state.savepoints.dir": "s3://flink-state/savepoints/ecom-revenue-analytics",
      "s3.endpoint": "http://seaweedfs.flink-demo.svc:8333",
      "s3.path.style.access": "true",
    }}
  >
    <KafkaSource
      topic="ecom.order-enriched"
      schema={OrderSchema}
      bootstrapServers="kafka:9092"
      consumerGroup="ecom-revenue"
    />
    <Route>
      <Route.Branch condition="1 = 1">
        <SlideWindow size="5 MINUTE" slide="1 MINUTE" on="orderTime" />
        <Aggregate
          groupBy={['category']}
          select={{
            category: 'category',
            totalRevenue: 'SUM(amount)',
            orderCount: 'COUNT(*)',
            windowStart: 'WINDOW_START',
            windowEnd: 'WINDOW_END',
          }}
        />
        <JdbcSink
          table="revenue_by_category"
          url="jdbc:postgresql://postgres:5432/flink_sink"
        />
      </Route.Branch>
      <Route.Branch condition="amount > 500">
        <KafkaSink
          topic="ecom.revenue-alerts"
          bootstrapServers="kafka:9092"
        />
      </Route.Branch>
    </Route>
  </Pipeline>
);
`,
    },

    // ── Pipeline E3: Customer 360 (lookup join + session window) ────

    {
      path: "pipelines/ecom-customer-360/index.tsx",
      content: `import {
  Pipeline,
  KafkaSource,
  JdbcSink,
  LookupJoin,
  SessionWindow,
  Aggregate,
} from '@flink-reactor/dsl';
import { OrderSchema, CustomerSchema } from '@/schemas/ecommerce';

const orders = KafkaSource({
  topic: "ecom.orders",
  schema: OrderSchema,
  bootstrapServers: "kafka:9092",
  consumerGroup: "ecom-customer360",
});

export default (
  <Pipeline
    name="ecom-customer-360"
    mode="streaming"
    parallelism={2}
    stateBackend="rocksdb"
    checkpoint={{ interval: "30s", mode: "exactly-once" }}
    restartStrategy={{ type: "fixed-delay", attempts: 3, delay: "10s" }}
    flinkConfig={{
      "state.checkpoints.dir": "s3://flink-state/checkpoints/ecom-customer-360",
      "state.savepoints.dir": "s3://flink-state/savepoints/ecom-customer-360",
      "s3.endpoint": "http://seaweedfs.flink-demo.svc:8333",
      "s3.path.style.access": "true",
    }}
  >
    {orders}
    {LookupJoin({
      input: orders,
      table: "customers",
      url: "jdbc:postgresql://postgres:5432/flink_sink",
      on: "customerId = customerId",
    })}
    <SessionWindow gap="30 MINUTE" on="orderTime" />
    <Aggregate
      groupBy={['customerId', 'name', 'tier']}
      select={{
        customerId: 'customerId',
        customerName: 'name',
        tier: 'tier',
        sessionOrders: 'COUNT(*)',
        sessionRevenue: 'SUM(amount)',
        windowStart: 'SESSION_START',
        windowEnd: 'SESSION_END',
      }}
    />
    <JdbcSink
      table="customer_sessions"
      url="jdbc:postgresql://postgres:5432/flink_sink"
      upsertMode
      keyFields={['customerId']}
    />
  </Pipeline>
);
`,
    },

    // ── Data Pump: DataGen → Kafka for all ecommerce topics ─────────

    {
      path: "pipelines/pump-ecom/index.tsx",
      content: `import {
  Pipeline,
  DataGenSource,
  KafkaSink,
  StatementSet,
} from '@flink-reactor/dsl';
import { OrderSchema, OrderItemSchema, ProductSchema, CustomerSchema } from '@/schemas/ecommerce';

export default (
  <Pipeline
    name="pump-ecom"
    mode="streaming"
    parallelism={4}
    stateBackend="rocksdb"
    checkpoint={{ interval: "60s", mode: "exactly-once" }}
    flinkConfig={{
      "state.checkpoints.dir": "s3://flink-state/checkpoints/pump-ecom",
      "state.savepoints.dir": "s3://flink-state/savepoints/pump-ecom",
      "s3.endpoint": "http://seaweedfs.flink-demo.svc:8333",
      "s3.path.style.access": "true",
    }}
  >
    <StatementSet>
      {/* Orders: 2000/s */}
      <DataGenSource schema={OrderSchema} rowsPerSecond={2000} />
      <KafkaSink topic="ecom.orders" bootstrapServers="kafka:9092" />

      {/* Order Items: 6000/s (~3 items per order) */}
      <DataGenSource schema={OrderItemSchema} rowsPerSecond={6000} />
      <KafkaSink topic="ecom.order-items" bootstrapServers="kafka:9092" />

      {/* Products CDC: 200/s (price/stock changes) */}
      <DataGenSource schema={ProductSchema} rowsPerSecond={200} />
      <KafkaSink topic="ecom.products" bootstrapServers="kafka:9092" />

      {/* Customers CDC: 100/s (profile updates) */}
      <DataGenSource schema={CustomerSchema} rowsPerSecond={100} />
      <KafkaSink topic="ecom.customers" bootstrapServers="kafka:9092" />
    </StatementSet>
  </Pipeline>
);
`,
    },

    // ── Tests ────────────────────────────────────────────────────────

    {
      path: "tests/pipelines/ecom-order-enrichment.test.ts",
      content: `import { describe, it } from 'vitest';

describe('ecom-order-enrichment pipeline', () => {
  it.todo('synthesizes valid Flink SQL with interval + temporal join');
});
`,
    },
  ]
}
