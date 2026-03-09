-- Continuous Group Aggregate
-- Computes running totals per user from a transaction stream,
-- writing results to a PostgreSQL table via JDBC.

CREATE TABLE `transactions` (
  `user_id` STRING,
  `amount` DECIMAL(10, 2),
  `transaction_time` TIMESTAMP(3),
  `category` STRING,
  WATERMARK FOR `transaction_time` AS `transaction_time` - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'transactions',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

CREATE TABLE `user_totals` (
  `user_id` STRING,
  `total_amount` DECIMAL(10, 2),
  `txn_count` BIGINT,
  PRIMARY KEY (`user_id`) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://db:5432/analytics',
  'table-name' = 'user_totals',
  'driver' = 'org.postgresql.Driver'
);

INSERT INTO `user_totals`
SELECT
  `user_id`,
  SUM(`amount`) AS `total_amount`,
  COUNT(*) AS `txn_count`
FROM `transactions`
GROUP BY `user_id`;
