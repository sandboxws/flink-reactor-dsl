-- Simple ETL: Log Filtering
-- Filters raw log events for ERROR and WARN levels
-- and writes them to an Elasticsearch index.

CREATE TABLE `raw_logs` (
  `log_id` STRING,
  `timestamp` TIMESTAMP(3),
  `level` STRING,
  `message` STRING,
  `service_name` STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'raw_logs',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

CREATE TABLE `error_logs` (
  `log_id` STRING,
  `timestamp` TIMESTAMP(3),
  `level` STRING,
  `message` STRING,
  `service_name` STRING
) WITH (
  'connector' = 'elasticsearch-7',
  'hosts' = 'http://elasticsearch:9200',
  'index' = 'error_logs'
);

INSERT INTO `error_logs`
SELECT
  `log_id`,
  `timestamp`,
  `level`,
  `message`,
  `service_name`
FROM `raw_logs`
WHERE `level` IN ('ERROR', 'WARN');
