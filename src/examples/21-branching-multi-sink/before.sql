-- Branching Multi-Sink: Order Routing
-- A single order stream splits into three paths:
--   1. High-value orders (>= $1000) → Kafka
--   2. Failed orders → Kafka alerts
--   3. Regional revenue aggregates (1-min window) → JDBC

CREATE TABLE `raw_orders` (
  `order_id` STRING,
  `customer_id` STRING,
  `product_id` STRING,
  `quantity` INT,
  `unit_price` DECIMAL(10, 2),
  `order_time` TIMESTAMP(3),
  `region` STRING,
  `order_status` STRING,
  WATERMARK FOR `order_time` AS `order_time` - INTERVAL '10' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'raw_orders',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

CREATE TABLE `high_value_orders` (
  `order_id` STRING,
  `customer_id` STRING,
  `product_id` STRING,
  `total_amount` DECIMAL(10, 2),
  `order_time` TIMESTAMP(3),
  `region` STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'high_value_orders',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

CREATE TABLE `failed_orders_alerts` (
  `order_id` STRING,
  `customer_id` STRING,
  `total_amount` DECIMAL(10, 2),
  `order_time` TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'failed_orders_alerts',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

CREATE TABLE `regional_metrics_per_minute` (
  `region` STRING,
  `revenue` DECIMAL(10, 2),
  `order_count` BIGINT,
  `window_start` TIMESTAMP(3),
  `window_end` TIMESTAMP(3),
  PRIMARY KEY (`region`, `window_start`) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://db:5432/analytics',
  'table-name' = 'regional_metrics_per_minute',
  'driver' = 'org.postgresql.Driver'
);

-- Create a view for the computed total_amount
CREATE TEMPORARY VIEW `orders_enriched` AS
SELECT
  `order_id`, `customer_id`, `product_id`,
  (`quantity` * `unit_price`) AS `total_amount`,
  `order_time`, `region`, `order_status`
FROM `raw_orders`;

-- Branch 1: High-value orders
INSERT INTO `high_value_orders`
SELECT `order_id`, `customer_id`, `product_id`, `total_amount`, `order_time`, `region`
FROM `orders_enriched`
WHERE `total_amount` >= 1000;

-- Branch 2: Failed order alerts
INSERT INTO `failed_orders_alerts`
SELECT `order_id`, `customer_id`, `total_amount`, `order_time`
FROM `orders_enriched`
WHERE `order_status` = 'FAILED';

-- Branch 3: Regional aggregates
INSERT INTO `regional_metrics_per_minute`
SELECT
  `region`,
  SUM(`total_amount`) AS `revenue`,
  COUNT(*) AS `order_count`,
  `window_start`,
  `window_end`
FROM TABLE(
  TUMBLE(TABLE `orders_enriched`, DESCRIPTOR(`order_time`), INTERVAL '1' MINUTE)
)
GROUP BY `region`, `window_start`, `window_end`;
