-- CEP Fraud Detection: Rapid Geo-Change Pattern
-- Uses MATCH_RECOGNIZE to detect when a card has a high-value transaction,
-- followed by transactions from rapidly changing locations,
-- ending with another high-value transaction — all within 10 minutes.

CREATE TABLE `card_transactions` (
  `transaction_id` STRING,
  `card_id` STRING,
  `merchant_id` STRING,
  `amount` DECIMAL(10, 2),
  `location` STRING,
  `transaction_time` TIMESTAMP(3),
  WATERMARK FOR `transaction_time` AS `transaction_time` - INTERVAL '30' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'card_transactions',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

CREATE TABLE `fraud_alerts` (
  `card_id` STRING,
  `first_txn` STRING,
  `last_txn` STRING,
  `total_amount` DECIMAL(10, 2),
  `txn_count` BIGINT,
  `fraud_type` STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'fraud_alerts',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

INSERT INTO `fraud_alerts`
SELECT
  `card_id`,
  `first_txn`,
  `last_txn`,
  `total_amount`,
  `txn_count`,
  'RAPID_GEO_CHANGE' AS `fraud_type`
FROM `card_transactions`
MATCH_RECOGNIZE (
  PARTITION BY `card_id`
  ORDER BY `transaction_time`
  MEASURES
    A.`transaction_id` AS `first_txn`,
    C.`transaction_id` AS `last_txn`,
    A.`amount` + SUM(B.`amount`) + C.`amount` AS `total_amount`,
    COUNT(B.*) + 2 AS `txn_count`
  AFTER MATCH SKIP TO NEXT ROW
  PATTERN (A B+ C) WITHIN INTERVAL '10' MINUTE
  DEFINE
    A AS A.`amount` > 1000,
    B AS B.`location` <> A.`location`,
    C AS C.`amount` > 500 AND C.`location` <> B.`location`
) AS `fraud`;
