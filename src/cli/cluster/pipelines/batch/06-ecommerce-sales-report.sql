-- Ecommerce daily sales report: revenue and units by category
-- Exercises: batch mode, datagen source, GROUP BY aggregate, business metrics

SET 'execution.runtime-mode' = 'batch';

CREATE TABLE `orders` (
  `order_id` INT,
  `product_id` INT,
  `category` STRING,
  `amount` DOUBLE,
  `quantity` INT,
  `sale_date` STRING
) WITH (
  'connector' = 'datagen',
  'number-of-rows' = '10000',
  'fields.order_id.min' = '1',
  'fields.order_id.max' = '999999',
  'fields.product_id.min' = '1',
  'fields.product_id.max' = '100',
  'fields.category.length' = '8',
  'fields.amount.min' = '5',
  'fields.amount.max' = '500',
  'fields.quantity.min' = '1',
  'fields.quantity.max' = '10',
  'fields.sale_date.length' = '10'
);

CREATE TABLE `sales_report` (
  `category` STRING,
  `sale_date` STRING,
  `total_revenue` DOUBLE,
  `total_units` BIGINT,
  `avg_order_value` DOUBLE,
  `num_orders` BIGINT
) WITH (
  'connector' = 'print'
);

INSERT INTO `sales_report`
SELECT
  `category`,
  `sale_date`,
  SUM(`amount` * `quantity`) AS `total_revenue`,
  SUM(CAST(`quantity` AS BIGINT)) AS `total_units`,
  AVG(`amount`) AS `avg_order_value`,
  COUNT(*) AS `num_orders`
FROM `orders`
GROUP BY `category`, `sale_date`;
