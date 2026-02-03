-- Temporal Join: Forex Order Conversion
-- Joins forex orders with a versioned currency rate table
-- to convert order amounts using the rate valid at order time.

CREATE TABLE `currency_rates` (
  `currency_pair` STRING,
  `exchange_rate` DECIMAL(12, 6),
  `rate_time` TIMESTAMP(3),
  WATERMARK FOR `rate_time` AS `rate_time` - INTERVAL '1' SECOND,
  PRIMARY KEY (`currency_pair`) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'currency_rates',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

CREATE TABLE `forex_orders` (
  `order_id` STRING,
  `currency_pair` STRING,
  `amount` DECIMAL(12, 2),
  `order_time` TIMESTAMP(3),
  WATERMARK FOR `order_time` AS `order_time` - INTERVAL '1' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'forex_orders',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

CREATE TABLE `converted_orders` (
  `order_id` STRING,
  `currency_pair` STRING,
  `amount` DECIMAL(12, 2),
  `order_time` TIMESTAMP(3),
  `exchange_rate` DECIMAL(12, 6),
  `converted_amount` DECIMAL(12, 2)
) WITH (
  'connector' = 'kafka',
  'topic' = 'converted_orders',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

INSERT INTO `converted_orders`
SELECT
  o.`order_id`,
  o.`currency_pair`,
  o.`amount`,
  o.`order_time`,
  r.`exchange_rate`,
  (o.`amount` * r.`exchange_rate`) AS `converted_amount`
FROM `forex_orders` AS o
JOIN `currency_rates` FOR SYSTEM_TIME AS OF o.`order_time` AS r
  ON o.`currency_pair` = r.`currency_pair`;
