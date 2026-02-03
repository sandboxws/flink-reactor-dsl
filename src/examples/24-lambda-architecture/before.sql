-- Lambda Architecture: Clickstream Pipeline
-- A single clickstream source fans out to 4 sinks:
--   1. Raw data lake archive (S3 Parquet, partitioned by date/hour)
--   2. Real-time page metrics (1-min tumbling window) → Kafka
--   3. User activity summary (continuous upsert) → PostgreSQL
--   4. Error event alerts → Kafka

CREATE TABLE `clickstream` (
  `session_id` STRING,
  `user_id` STRING,
  `page_url` STRING,
  `referrer` STRING,
  `event_type` STRING,
  `event_time` TIMESTAMP(3),
  `user_agent` STRING,
  `ip_address` STRING,
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '10' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'clickstream',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

CREATE TABLE `data_lake_archive` (
  `session_id` STRING,
  `user_id` STRING,
  `page_url` STRING,
  `referrer` STRING,
  `event_type` STRING,
  `event_time` TIMESTAMP(3),
  `user_agent` STRING,
  `ip_address` STRING,
  `dt` DATE,
  `hr` INT
) PARTITIONED BY (`dt`, `hr`) WITH (
  'connector' = 'filesystem',
  'path' = 's3://data-lake/clickstream/raw/',
  'format' = 'parquet',
  'sink.rolling-policy.file-size' = '256MB',
  'sink.rolling-policy.rollover-interval' = '10min'
);

CREATE TABLE `realtime_page_metrics` (
  `page_url` STRING,
  `view_count` BIGINT,
  `unique_visitors` BIGINT,
  `window_start` TIMESTAMP(3),
  `window_end` TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'realtime_page_metrics',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

CREATE TABLE `user_activity_summary` (
  `user_id` STRING,
  `total_events` BIGINT,
  `session_count` BIGINT,
  `last_seen` TIMESTAMP(3),
  PRIMARY KEY (`user_id`) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://db:5432/analytics',
  'table-name' = 'user_activity_summary',
  'driver' = 'org.postgresql.Driver'
);

CREATE TABLE `error_events_alerts` (
  `session_id` STRING,
  `user_id` STRING,
  `page_url` STRING,
  `event_time` TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'error_events_alerts',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

-- Sink 1: Archive to data lake
INSERT INTO `data_lake_archive`
SELECT *, CAST(`event_time` AS DATE) AS `dt`, HOUR(`event_time`) AS `hr`
FROM `clickstream`;

-- Sink 2: Real-time page metrics
INSERT INTO `realtime_page_metrics`
SELECT
  `page_url`,
  COUNT(*) AS `view_count`,
  COUNT(DISTINCT `user_id`) AS `unique_visitors`,
  `window_start`,
  `window_end`
FROM TABLE(
  TUMBLE(TABLE `clickstream`, DESCRIPTOR(`event_time`), INTERVAL '1' MINUTE)
)
GROUP BY `page_url`, `window_start`, `window_end`;

-- Sink 3: User activity summary (upsert)
INSERT INTO `user_activity_summary`
SELECT
  `user_id`,
  COUNT(*) AS `total_events`,
  COUNT(DISTINCT `session_id`) AS `session_count`,
  MAX(`event_time`) AS `last_seen`
FROM `clickstream`
GROUP BY `user_id`;

-- Sink 4: Error alerts
INSERT INTO `error_events_alerts`
SELECT `session_id`, `user_id`, `page_url`, `event_time`
FROM `clickstream`
WHERE `event_type` IN ('error', 'exception');
