-- Debezium CDC → running aggregation
-- Exercises: CDC aggregation, retract stream, stateful processing

CREATE TABLE `cdc_products` (
  `id` INT,
  `name` STRING,
  `category` STRING,
  `price` DOUBLE,
  `quantity` INT
) WITH (
  'connector' = 'kafka',
  'topic' = 'cdc.inventory.products',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-cdc-aggregate',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'debezium-json'
);

CREATE TABLE `category_stats` (
  `category` STRING,
  `product_count` BIGINT,
  `total_inventory_value` DOUBLE,
  `avg_price` DOUBLE,
  `max_price` DOUBLE
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO `category_stats`
SELECT
  `category`,
  COUNT(*) AS `product_count`,
  SUM(`price` * `quantity`) AS `total_inventory_value`,
  AVG(`price`) AS `avg_price`,
  MAX(`price`) AS `max_price`
FROM `cdc_products`
GROUP BY `category`;
