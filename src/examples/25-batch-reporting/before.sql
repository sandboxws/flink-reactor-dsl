-- Bounded Batch Reporting: Monthly Financial Report
-- Reads monthly transaction data, joins with merchant dimension,
-- then produces 3 reports:
--   1. Category summary → S3 Parquet
--   2. Country report → PostgreSQL
--   3. Fraud extract → S3 CSV

SET 'execution.runtime-mode' = 'batch';

CREATE TABLE `transactions_jan_2024` (
  `transaction_id` STRING,
  `account_id` STRING,
  `merchant_id` STRING,
  `amount` DECIMAL(10, 2),
  `currency` STRING,
  `transaction_type` STRING,
  `transaction_date` DATE,
  `is_fraud` BOOLEAN
) WITH (
  'connector' = 'filesystem',
  'path' = 's3://data-warehouse/transactions/year=2024/month=01/',
  'format' = 'parquet'
);

CREATE TABLE `dim_merchants` (
  `merchant_id` STRING,
  `merchant_name` STRING,
  `merchant_category` STRING,
  `merchant_country` STRING,
  PRIMARY KEY (`merchant_id`) NOT ENFORCED
) WITH (
  'connector' = 'filesystem',
  'path' = 's3://data-warehouse/merchants/',
  'format' = 'parquet'
);

CREATE TABLE `category_summary` (
  `merchant_category` STRING,
  `transaction_date` DATE,
  `total_volume` DECIMAL(10, 2),
  `txn_count` BIGINT,
  `fraud_count` BIGINT
) WITH (
  'connector' = 'filesystem',
  'path' = 's3://reports/monthly/category_summary/',
  'format' = 'parquet'
);

CREATE TABLE `monthly_country_report` (
  `merchant_country` STRING,
  `transaction_type` STRING,
  `country_volume` DECIMAL(10, 2),
  `avg_txn_amount` DECIMAL(10, 2),
  `unique_accounts` BIGINT,
  PRIMARY KEY (`merchant_country`, `transaction_type`) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://db:5432/reporting',
  'table-name' = 'monthly_country_report',
  'driver' = 'org.postgresql.Driver'
);

CREATE TABLE `fraud_extract` (
  `transaction_id` STRING,
  `account_id` STRING,
  `merchant_name` STRING,
  `amount` DECIMAL(10, 2),
  `transaction_date` DATE
) WITH (
  'connector' = 'filesystem',
  'path' = 's3://reports/fraud/january_2024/',
  'format' = 'csv'
);

-- Join transactions with merchants (broadcast small dimension)
CREATE TEMPORARY VIEW `txn_enriched` AS
SELECT /*+ BROADCAST(m) */
  t.`transaction_id`, t.`account_id`, t.`amount`, t.`currency`,
  t.`transaction_type`, t.`transaction_date`, t.`is_fraud`,
  m.`merchant_name`, m.`merchant_category`, m.`merchant_country`
FROM `transactions_jan_2024` AS t
JOIN `dim_merchants` AS m
  ON t.`merchant_id` = m.`merchant_id`;

-- Report 1: Category summary
INSERT INTO `category_summary`
SELECT
  `merchant_category`, `transaction_date`,
  SUM(`amount`) AS `total_volume`,
  COUNT(*) AS `txn_count`,
  SUM(CASE WHEN `is_fraud` THEN 1 ELSE 0 END) AS `fraud_count`
FROM `txn_enriched`
GROUP BY `merchant_category`, `transaction_date`;

-- Report 2: Country report
INSERT INTO `monthly_country_report`
SELECT
  `merchant_country`, `transaction_type`,
  SUM(`amount`) AS `country_volume`,
  AVG(`amount`) AS `avg_txn_amount`,
  COUNT(DISTINCT `account_id`) AS `unique_accounts`
FROM `txn_enriched`
GROUP BY `merchant_country`, `transaction_type`;

-- Report 3: Fraud extract
INSERT INTO `fraud_extract`
SELECT `transaction_id`, `account_id`, `merchant_name`, `amount`, `transaction_date`
FROM `txn_enriched`
WHERE `is_fraud` = true;
