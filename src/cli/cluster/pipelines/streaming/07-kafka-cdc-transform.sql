-- Debezium CDC → enrich → print
-- Exercises: Kafka connector, Debezium format, computed columns

CREATE TABLE `cdc_products` (
  `id` INT,
  `name` STRING,
  `category` STRING,
  `price` DOUBLE,
  `quantity` INT,
  `proc_time` AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'cdc.inventory.products',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-cdc-transform',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'debezium-json'
);

CREATE TABLE `enriched_products` (
  `id` INT,
  `name` STRING,
  `category` STRING,
  `price` DOUBLE,
  `quantity` INT,
  `inventory_value` DOUBLE,
  `price_tier` STRING
) WITH (
  'connector' = 'print'
);

INSERT INTO `enriched_products`
SELECT
  `id`,
  `name`,
  `category`,
  `price`,
  `quantity`,
  `price` * `quantity` AS `inventory_value`,
  CASE
    WHEN `price` >= 300 THEN 'premium'
    WHEN `price` >= 100 THEN 'mid-range'
    ELSE 'budget'
  END AS `price_tier`
FROM `cdc_products`;
