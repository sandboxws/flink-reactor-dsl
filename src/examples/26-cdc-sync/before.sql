-- CDC Multi-Output Sync
-- Consumes Debezium CDC events from MySQL products table
-- and fans out to 4 destinations:
--   1. PostgreSQL replica (upsert)
--   2. Changelog archive → Kafka
--   3. Low stock alerts (stock < 10) → Kafka
--   4. Category stats (aggregated) → PostgreSQL

CREATE TABLE `products_cdc` (
  `product_id` BIGINT,
  `product_name` STRING,
  `category` STRING,
  `price` DECIMAL(10, 2),
  `stock_quantity` INT,
  `updated_at` TIMESTAMP(3),
  PRIMARY KEY (`product_id`) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'mysql.inventory.products',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'debezium-json',
  'scan.startup.mode' = 'earliest-offset'
);

CREATE TABLE `products_replica` (
  `product_id` BIGINT,
  `product_name` STRING,
  `category` STRING,
  `price` DECIMAL(10, 2),
  `stock_quantity` INT,
  `updated_at` TIMESTAMP(3),
  PRIMARY KEY (`product_id`) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://db:5432/replica',
  'table-name' = 'products_replica',
  'driver' = 'org.postgresql.Driver'
);

CREATE TABLE `products_changelog_archive` (
  `product_id` BIGINT,
  `product_name` STRING,
  `category` STRING,
  `price` DECIMAL(10, 2),
  `stock_quantity` INT,
  `updated_at` TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'products_changelog_archive',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

CREATE TABLE `low_stock_alerts` (
  `product_id` BIGINT,
  `product_name` STRING,
  `price` DECIMAL(10, 2),
  `stock_quantity` INT,
  `updated_at` TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'low_stock_alerts',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

CREATE TABLE `category_stats` (
  `category` STRING,
  `product_count` BIGINT,
  `avg_price` DECIMAL(10, 2),
  `last_update` TIMESTAMP(3),
  PRIMARY KEY (`category`) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://db:5432/analytics',
  'table-name' = 'category_stats',
  'driver' = 'org.postgresql.Driver'
);

-- Sink 1: PostgreSQL replica (upsert)
INSERT INTO `products_replica`
SELECT * FROM `products_cdc`;

-- Sink 2: Changelog archive
INSERT INTO `products_changelog_archive`
SELECT * FROM `products_cdc`;

-- Sink 3: Low stock alerts
INSERT INTO `low_stock_alerts`
SELECT `product_id`, `product_name`, `price`, `stock_quantity`, `updated_at`
FROM `products_cdc`
WHERE `stock_quantity` < 10;

-- Sink 4: Category stats
INSERT INTO `category_stats`
SELECT
  `category`,
  COUNT(*) AS `product_count`,
  AVG(`price`) AS `avg_price`,
  MAX(`updated_at`) AS `last_update`
FROM `products_cdc`
GROUP BY `category`;
