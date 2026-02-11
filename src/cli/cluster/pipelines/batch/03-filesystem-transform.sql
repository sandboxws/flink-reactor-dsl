-- CSV file → computed columns → print
-- Exercises: filesystem connector, CSV format, batch transform, FINISHED job state

SET 'execution.runtime-mode' = 'batch';

CREATE TABLE `transactions` (
  `transaction_id` STRING,
  `customer_id` STRING,
  `product` STRING,
  `amount` DOUBLE,
  `currency` STRING,
  `timestamp` STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '/opt/flink/data/sample-transactions.csv',
  'format' = 'csv',
  'csv.ignore-parse-errors' = 'true',
  'csv.allow-comments' = 'true'
);

CREATE TABLE `transaction_report` (
  `transaction_id` STRING,
  `customer_id` STRING,
  `product` STRING,
  `amount` DOUBLE,
  `currency` STRING,
  `amount_usd` DOUBLE,
  `amount_tier` STRING
) WITH (
  'connector' = 'print'
);

INSERT INTO `transaction_report`
SELECT
  `transaction_id`,
  `customer_id`,
  `product`,
  `amount`,
  `currency`,
  CASE
    WHEN `currency` = 'EUR' THEN `amount` * 1.08
    ELSE `amount`
  END AS `amount_usd`,
  CASE
    WHEN `amount` >= 1000 THEN 'high'
    WHEN `amount` >= 100 THEN 'medium'
    ELSE 'low'
  END AS `amount_tier`
FROM `transactions`
WHERE `transaction_id` IS NOT NULL AND `transaction_id` <> 'transaction_id';
