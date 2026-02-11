-- Bounded datagen → GROUP BY aggregate
-- Exercises: batch execution mode, bounded source, FINISHED job state

SET 'execution.runtime-mode' = 'batch';

CREATE TABLE `sales` (
  `product_id` INT,
  `region` STRING,
  `amount` DOUBLE,
  `sale_date` STRING
) WITH (
  'connector' = 'datagen',
  'number-of-rows' = '10000',
  'fields.product_id.min' = '1',
  'fields.product_id.max' = '50',
  'fields.region.length' = '5',
  'fields.amount.min' = '10',
  'fields.amount.max' = '500',
  'fields.sale_date.length' = '10'
);

CREATE TABLE `sales_summary` (
  `region` STRING,
  `total_sales` DOUBLE,
  `avg_sale` DOUBLE,
  `num_transactions` BIGINT
) WITH (
  'connector' = 'print'
);

INSERT INTO `sales_summary`
SELECT
  `region`,
  SUM(`amount`) AS `total_sales`,
  AVG(`amount`) AS `avg_sale`,
  COUNT(*) AS `num_transactions`
FROM `sales`
GROUP BY `region`;
