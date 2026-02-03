-- Filter and Project
-- Reads an orders table, filters for high-value orders (amount > 100),
-- and projects only the relevant columns.

CREATE TABLE `orders` (
  `order_id` BIGINT,
  `user_id` STRING,
  `product_id` STRING,
  `amount` DECIMAL(10, 2),
  `order_time` TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

CREATE TABLE `high_value_orders` (
  `order_id` BIGINT,
  `user_id` STRING,
  `amount` DECIMAL(10, 2),
  `order_time` TIMESTAMP(3)
) WITH (
  'connector' = 'print'
);

INSERT INTO `high_value_orders`
SELECT
  `order_id`,
  `user_id`,
  `amount`,
  `order_time`
FROM `orders`
WHERE `amount` > 100;
