-- High-value order filter
-- Exercises: simple streaming filter, backpressure under load

CREATE TABLE `orders` (
  `order_id` INT,
  `customer_id` INT,
  `amount` DOUBLE,
  `currency` STRING,
  `order_time` TIMESTAMP(3),
  WATERMARK FOR `order_time` AS `order_time` - INTERVAL '3' SECOND
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '50',
  'fields.order_id.min' = '1',
  'fields.order_id.max' = '999999',
  'fields.customer_id.min' = '1',
  'fields.customer_id.max' = '500',
  'fields.amount.min' = '1',
  'fields.amount.max' = '5000',
  'fields.currency.length' = '3'
);

CREATE TABLE `high_value_orders` (
  `order_id` INT,
  `customer_id` INT,
  `amount` DOUBLE,
  `currency` STRING,
  `order_time` TIMESTAMP(3)
) WITH (
  'connector' = 'print'
);

INSERT INTO `high_value_orders`
SELECT `order_id`, `customer_id`, `amount`, `currency`, `order_time`
FROM `orders`
WHERE `amount` > 1000;
