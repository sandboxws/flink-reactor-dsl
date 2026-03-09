-- Bounded Batch ETL: Sales Data Processing
-- Reads historical sales from S3 Parquet, joins with product dimension,
-- then branches to two aggregations:
--   1. Daily category sales → S3 Parquet
--   2. Daily brand performance → PostgreSQL

SET 'execution.runtime-mode' = 'batch';

CREATE TABLE `sales_2024` (
  `sale_id` STRING,
  `store_id` STRING,
  `product_id` STRING,
  `customer_id` STRING,
  `quantity` INT,
  `amount` DECIMAL(10, 2),
  `sale_date` DATE,
  `sale_timestamp` TIMESTAMP(3)
) WITH (
  'connector' = 'filesystem',
  'path' = 's3://data-warehouse/sales/2024/',
  'format' = 'parquet'
);

CREATE TABLE `dim_products` (
  `product_id` STRING,
  `product_name` STRING,
  `category` STRING,
  `subcategory` STRING,
  `brand` STRING,
  PRIMARY KEY (`product_id`) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://db:5432/catalog',
  'table-name' = 'products',
  'driver' = 'org.postgresql.Driver'
);

CREATE TABLE `daily_category_sales` (
  `category` STRING,
  `subcategory` STRING,
  `sale_date` DATE,
  `total_revenue` DECIMAL(10, 2),
  `total_units` BIGINT,
  `unique_customers` BIGINT
) WITH (
  'connector' = 'filesystem',
  'path' = 's3://data-warehouse/aggregates/daily_category_sales/',
  'format' = 'parquet',
  'sink.partition-commit.trigger' = 'process-time'
);

CREATE TABLE `daily_brand_performance` (
  `brand` STRING,
  `sale_date` DATE,
  `brand_revenue` DECIMAL(10, 2),
  `transaction_count` BIGINT,
  PRIMARY KEY (`brand`, `sale_date`) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://db:5432/analytics',
  'table-name' = 'daily_brand_performance',
  'driver' = 'org.postgresql.Driver'
);

-- Create enriched view with broadcast join
CREATE TEMPORARY VIEW `sales_enriched` AS
SELECT /*+ BROADCAST(p) */
  s.`sale_id`, s.`store_id`, s.`customer_id`, s.`quantity`,
  s.`amount`, s.`sale_date`,
  p.`product_name`, p.`category`, p.`subcategory`, p.`brand`
FROM `sales_2024` AS s
JOIN `dim_products` AS p
  ON s.`product_id` = p.`product_id`;

-- Aggregation 1: Daily category sales
INSERT INTO `daily_category_sales`
SELECT
  `category`, `subcategory`, `sale_date`,
  SUM(`amount`) AS `total_revenue`,
  SUM(`quantity`) AS `total_units`,
  COUNT(DISTINCT `customer_id`) AS `unique_customers`
FROM `sales_enriched`
GROUP BY `category`, `subcategory`, `sale_date`;

-- Aggregation 2: Daily brand performance
INSERT INTO `daily_brand_performance`
SELECT
  `brand`, `sale_date`,
  SUM(`amount`) AS `brand_revenue`,
  COUNT(*) AS `transaction_count`
FROM `sales_enriched`
GROUP BY `brand`, `sale_date`;
