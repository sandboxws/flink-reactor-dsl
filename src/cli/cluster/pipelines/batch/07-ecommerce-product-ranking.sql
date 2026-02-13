-- Ecommerce product ranking: Top-N products by revenue
-- Exercises: batch mode, datagen source, Top-N with ROW_NUMBER, ranking

SET 'execution.runtime-mode' = 'batch';

CREATE TABLE `sales` (
  `product_id` INT,
  `product_name` STRING,
  `category` STRING,
  `amount` DOUBLE,
  `quantity` INT
) WITH (
  'connector' = 'datagen',
  'number-of-rows' = '5000',
  'fields.product_id.min' = '1',
  'fields.product_id.max' = '50',
  'fields.product_name.length' = '10',
  'fields.category.length' = '8',
  'fields.amount.min' = '10',
  'fields.amount.max' = '500',
  'fields.quantity.min' = '1',
  'fields.quantity.max' = '5'
);

CREATE TABLE `product_ranking` (
  `product_id` INT,
  `total_revenue` DOUBLE,
  `total_units` BIGINT,
  `rank_num` BIGINT
) WITH (
  'connector' = 'print'
);

INSERT INTO `product_ranking`
SELECT `product_id`, `total_revenue`, `total_units`, `rank_num`
FROM (
  SELECT
    `product_id`,
    SUM(`amount` * `quantity`) AS `total_revenue`,
    SUM(CAST(`quantity` AS BIGINT)) AS `total_units`,
    ROW_NUMBER() OVER (ORDER BY SUM(`amount` * `quantity`) DESC) AS `rank_num`
  FROM `sales`
  GROUP BY `product_id`
)
WHERE `rank_num` <= 10;
