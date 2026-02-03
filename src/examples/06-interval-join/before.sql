-- Interval Join: Order Fulfillment Tracking
-- Joins orders with shipments within a 7-day window
-- to compute fulfillment times per order.

CREATE TABLE `orders` (
  `order_id` STRING,
  `user_id` STRING,
  `product_id` STRING,
  `amount` DECIMAL(10, 2),
  `order_time` TIMESTAMP(3),
  WATERMARK FOR `order_time` AS `order_time` - INTERVAL '10' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

CREATE TABLE `shipments` (
  `shipment_id` STRING,
  `order_id` STRING,
  `carrier` STRING,
  `ship_time` TIMESTAMP(3),
  WATERMARK FOR `ship_time` AS `ship_time` - INTERVAL '10' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'shipments',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

CREATE TABLE `order_fulfillment` (
  `order_id` STRING,
  `user_id` STRING,
  `amount` DECIMAL(10, 2),
  `carrier` STRING,
  `fulfillment_time` INTERVAL DAY TO SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'order_fulfillment',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

INSERT INTO `order_fulfillment`
SELECT
  o.`order_id`,
  o.`user_id`,
  o.`amount`,
  s.`carrier`,
  (s.`ship_time` - o.`order_time`) AS `fulfillment_time`
FROM `orders` AS o
JOIN `shipments` AS s
  ON o.`order_id` = s.`order_id`
  AND s.`ship_time` BETWEEN o.`order_time`
  AND o.`order_time` + INTERVAL '7' DAY;
