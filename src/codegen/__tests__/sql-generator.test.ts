import { describe, it, expect, beforeEach } from 'vitest';
import { resetNodeIdCounter } from '../../core/jsx-runtime.js';
import { Schema, Field } from '../../core/schema.js';
import { KafkaSource, JdbcSource } from '../../components/sources.js';
import { KafkaSink, JdbcSink, FileSystemSink } from '../../components/sinks.js';
import { Filter, Map, Aggregate, Deduplicate, TopN } from '../../components/transforms.js';
import { Join, TemporalJoin, LookupJoin, IntervalJoin } from '../../components/joins.js';
import { TumbleWindow } from '../../components/windows.js';
import { PaimonCatalog } from '../../components/catalogs.js';
import { CatalogSource } from '../../components/catalog-source.js';
import { Pipeline } from '../../components/pipeline.js';
import { generateSql } from '../sql-generator.js';

beforeEach(() => {
  resetNodeIdCounter();
});

// ── Shared test schemas ─────────────────────────────────────────────

const OrderSchema = Schema({
  fields: {
    order_id: Field.BIGINT(),
    user_id: Field.STRING(),
    amount: Field.DECIMAL(10, 2),
    event_time: Field.TIMESTAMP(3),
  },
  watermark: {
    column: 'event_time',
    expression: "`event_time` - INTERVAL '5' SECOND",
  },
});

const UserSchema = Schema({
  fields: {
    user_id: Field.STRING(),
    name: Field.STRING(),
    email: Field.STRING(),
  },
});

const EventSchema = Schema({
  fields: {
    event_id: Field.BIGINT(),
    user_id: Field.STRING(),
    event_type: Field.STRING(),
    event_time: Field.TIMESTAMP(3),
    payload: Field.STRING(),
  },
  watermark: {
    column: 'event_time',
    expression: "`event_time` - INTERVAL '5' SECOND",
  },
});

// ── 6.1: KafkaSource → Filter → KafkaSink ──────────────────────────

describe('6.1: KafkaSource → Filter → KafkaSink', () => {
  it('produces correct DDL + DML', () => {
    const source = KafkaSource({
      topic: 'orders',
      format: 'json',
      bootstrapServers: 'kafka:9092',
      schema: OrderSchema,
    });

    const filter = Filter({
      condition: '`amount` > 100',
      children: [source],
    });

    const sink = KafkaSink({
      topic: 'large-orders',
      format: 'json',
      children: [filter],
    });

    const pipeline = Pipeline({
      name: 'filter-pipeline',
      children: [sink],
    });

    const result = generateSql(pipeline);
    expect(result.sql).toMatchSnapshot();
  });
});

// ── 6.2: Two-table inner join ───────────────────────────────────────

describe('6.2: Two-table inner join', () => {
  it('produces correct JOIN SQL', () => {
    const orders = KafkaSource({
      topic: 'orders',
      schema: OrderSchema,
      bootstrapServers: 'kafka:9092',
    });

    const users = KafkaSource({
      topic: 'users',
      schema: UserSchema,
      bootstrapServers: 'kafka:9092',
    });

    const joined = Join({
      left: orders,
      right: users,
      on: '`KafkaSource_0`.`user_id` = `KafkaSource_1`.`user_id`',
      type: 'inner',
    });

    const sink = KafkaSink({
      topic: 'enriched-orders',
      children: [joined],
    });

    const pipeline = Pipeline({
      name: 'join-pipeline',
      children: [sink],
    });

    const result = generateSql(pipeline);
    expect(result.sql).toMatchSnapshot();
  });
});

// ── 6.3: Windowed aggregation with TUMBLE TVF ──────────────────────

describe('6.3: Windowed aggregation with TUMBLE', () => {
  it('produces correct TVF + GROUP BY', () => {
    const source = KafkaSource({
      topic: 'orders',
      schema: OrderSchema,
      bootstrapServers: 'kafka:9092',
    });

    const agg = Aggregate({
      groupBy: ['user_id'],
      select: { total_amount: 'SUM(`amount`)', order_count: 'COUNT(*)' },
    });

    // Tree: Sink → TumbleWindow(children: [Aggregate, Source])
    // Window wraps the source; aggregate defines the computation
    const window = TumbleWindow({
      size: '1 hour',
      on: 'event_time',
      children: [agg, source],
    });

    const sink = KafkaSink({
      topic: 'hourly-totals',
      children: [window],
    });

    const pipeline = Pipeline({
      name: 'windowed-pipeline',
      children: [sink],
    });

    const result = generateSql(pipeline);
    expect(result.sql).toMatchSnapshot();
  });
});

// ── 6.4: Deduplicate → ROW_NUMBER ──────────────────────────────────

describe('6.4: Deduplicate produces ROW_NUMBER pattern', () => {
  it('produces correct ROW_NUMBER with WHERE rownum = 1', () => {
    const source = KafkaSource({
      topic: 'events',
      schema: EventSchema,
      bootstrapServers: 'kafka:9092',
    });

    const dedup = Deduplicate({
      key: ['event_id'],
      order: 'event_time',
      keep: 'first',
      children: [source],
    });

    const sink = KafkaSink({
      topic: 'deduped-events',
      children: [dedup],
    });

    const pipeline = Pipeline({
      name: 'dedup-pipeline',
      children: [sink],
    });

    const result = generateSql(pipeline);
    expect(result.sql).toMatchSnapshot();
  });
});

// ── 6.5: TopN → ROW_NUMBER ─────────────────────────────────────────

describe('6.5: TopN produces ROW_NUMBER pattern', () => {
  it('produces correct ROW_NUMBER with WHERE rownum <= N', () => {
    const source = KafkaSource({
      topic: 'orders',
      schema: OrderSchema,
      bootstrapServers: 'kafka:9092',
    });

    const topN = TopN({
      partitionBy: ['user_id'],
      orderBy: { amount: 'DESC' },
      n: 5,
      children: [source],
    });

    const sink = KafkaSink({
      topic: 'top-orders',
      children: [topN],
    });

    const pipeline = Pipeline({
      name: 'topn-pipeline',
      children: [sink],
    });

    const result = generateSql(pipeline);
    expect(result.sql).toMatchSnapshot();
  });
});

// ── 6.6: Multi-sink → STATEMENT SET ────────────────────────────────

describe('6.6: Multi-sink produces STATEMENT SET', () => {
  it('wraps multiple INSERT INTO in STATEMENT SET', () => {
    const source = KafkaSource({
      topic: 'events',
      schema: EventSchema,
      bootstrapServers: 'kafka:9092',
    });

    const errorSink = KafkaSink({ topic: 'errors' });
    const allSink = KafkaSink({ topic: 'all-events' });

    const errorFilter = Filter({
      condition: "`event_type` = 'ERROR'",
      children: [source],
    });

    // Source feeds into both sinks via different paths
    const pipeline = Pipeline({
      name: 'multi-sink-pipeline',
      children: [
        KafkaSink({
          topic: 'errors',
          children: [errorFilter],
        }),
        KafkaSink({
          topic: 'all-events',
          children: [source],
        }),
      ],
    });

    const result = generateSql(pipeline);
    expect(result.sql).toMatchSnapshot();
  });
});

// ── 6.7: Catalog pipeline → CREATE CATALOG ─────────────────────────

describe('6.7: Catalog pipeline produces CREATE CATALOG', () => {
  it('generates CREATE CATALOG and catalog-qualified references', () => {
    const { node: catalogNode, handle } = PaimonCatalog({
      name: 'lake',
      warehouse: 's3://bucket/paimon',
    });

    const catSource = CatalogSource({
      catalog: handle,
      database: 'analytics',
      table: 'page_views',
    });

    const sink = KafkaSink({
      topic: 'page-view-alerts',
      children: [catSource],
    });

    const pipeline = Pipeline({
      name: 'catalog-pipeline',
      children: [catalogNode, sink],
    });

    const result = generateSql(pipeline);
    expect(result.sql).toMatchSnapshot();
  });
});

// ── 6.8: CDC source with debezium-json ──────────────────────────────

describe('6.8: CDC source with debezium-json', () => {
  it('produces Kafka source DDL with debezium-json format', () => {
    const source = KafkaSource({
      topic: 'dbserver1.inventory.orders',
      format: 'debezium-json',
      bootstrapServers: 'kafka:9092',
      schema: Schema({
        fields: {
          order_id: Field.BIGINT(),
          product: Field.STRING(),
          quantity: Field.INT(),
          price: Field.DECIMAL(10, 2),
        },
        primaryKey: { columns: ['order_id'] },
      }),
    });

    const sink = JdbcSink({
      url: 'jdbc:postgresql://localhost:5432/warehouse',
      table: 'orders_snapshot',
      upsertMode: true,
      keyFields: ['order_id'],
      children: [source],
    });

    const pipeline = Pipeline({
      name: 'cdc-pipeline',
      children: [sink],
    });

    const result = generateSql(pipeline);
    expect(result.sql).toMatchSnapshot();
  });
});

// ── 6.9: LookupJoin with async config ──────────────────────────────

describe('6.9: LookupJoin with async config', () => {
  it('produces lookup join with proc_time and FOR SYSTEM_TIME AS OF', () => {
    const stream = KafkaSource({
      topic: 'orders',
      schema: OrderSchema,
      bootstrapServers: 'kafka:9092',
    });

    const lookupJoined = LookupJoin({
      input: stream,
      table: 'dim_users',
      url: 'jdbc:mysql://localhost:3306/mydb',
      on: '`KafkaSource_0`.`user_id` = `dim_users`.`user_id`',
      async: { enabled: true, capacity: 100, timeout: '10s' },
      cache: { type: 'lru', maxRows: 10000, ttl: '1h' },
    });

    const sink = KafkaSink({
      topic: 'enriched-orders',
      children: [lookupJoined],
    });

    const pipeline = Pipeline({
      name: 'lookup-pipeline',
      children: [sink],
    });

    const result = generateSql(pipeline);
    expect(result.sql).toMatchSnapshot();
  });
});

// ── 6.10: IntervalJoin with time bounds ─────────────────────────────

describe('6.10: IntervalJoin with time bounds', () => {
  it('produces BETWEEN ... AND ... time predicates', () => {
    const orders = KafkaSource({
      topic: 'orders',
      schema: OrderSchema,
      bootstrapServers: 'kafka:9092',
    });

    const shipments = KafkaSource({
      topic: 'shipments',
      schema: Schema({
        fields: {
          order_id: Field.BIGINT(),
          shipped_time: Field.TIMESTAMP(3),
          carrier: Field.STRING(),
        },
        watermark: {
          column: 'shipped_time',
          expression: "`shipped_time` - INTERVAL '5' SECOND",
        },
      }),
      bootstrapServers: 'kafka:9092',
    });

    const joined = IntervalJoin({
      left: orders,
      right: shipments,
      on: '`KafkaSource_0`.`order_id` = `KafkaSource_1`.`order_id`',
      interval: {
        from: 'event_time',
        to: "event_time + INTERVAL '7' DAY",
      },
      type: 'inner',
    });

    const sink = KafkaSink({
      topic: 'order-shipments',
      children: [joined],
    });

    const pipeline = Pipeline({
      name: 'interval-join-pipeline',
      children: [sink],
    });

    const result = generateSql(pipeline);
    expect(result.sql).toMatchSnapshot();
  });
});

// ── 6.11: Anti join → WHERE NOT EXISTS ──────────────────────────────

describe('6.11: Anti join produces WHERE NOT EXISTS', () => {
  it('generates correlated subquery with NOT EXISTS', () => {
    const allOrders = KafkaSource({
      topic: 'orders',
      schema: OrderSchema,
      bootstrapServers: 'kafka:9092',
    });

    const cancelledOrders = KafkaSource({
      topic: 'cancellations',
      schema: Schema({
        fields: {
          order_id: Field.BIGINT(),
          cancelled_at: Field.TIMESTAMP(3),
        },
      }),
      bootstrapServers: 'kafka:9092',
    });

    const antiJoined = Join({
      left: allOrders,
      right: cancelledOrders,
      on: '`KafkaSource_0`.`order_id` = `KafkaSource_1`.`order_id`',
      type: 'anti',
    });

    const sink = KafkaSink({
      topic: 'active-orders',
      children: [antiJoined],
    });

    const pipeline = Pipeline({
      name: 'anti-join-pipeline',
      children: [sink],
    });

    const result = generateSql(pipeline);
    expect(result.sql).toMatchSnapshot();
  });
});

// ── 6.12: Broadcast hint in join SQL ────────────────────────────────

describe('6.12: Broadcast hint in join SQL', () => {
  it('generates /*+ BROADCAST(alias) */ hint', () => {
    const orders = KafkaSource({
      topic: 'orders',
      schema: OrderSchema,
      bootstrapServers: 'kafka:9092',
    });

    const dimProducts = KafkaSource({
      topic: 'products',
      schema: Schema({
        fields: {
          product_id: Field.STRING(),
          product_name: Field.STRING(),
          category: Field.STRING(),
        },
      }),
      bootstrapServers: 'kafka:9092',
    });

    const joined = Join({
      left: orders,
      right: dimProducts,
      on: '`KafkaSource_0`.`product_id` = `KafkaSource_1`.`product_id`',
      type: 'inner',
      hints: { broadcast: 'right' },
    });

    const sink = KafkaSink({
      topic: 'enriched-orders',
      children: [joined],
    });

    const pipeline = Pipeline({
      name: 'broadcast-pipeline',
      children: [sink],
    });

    const result = generateSql(pipeline);
    expect(result.sql).toMatchSnapshot();
  });
});
