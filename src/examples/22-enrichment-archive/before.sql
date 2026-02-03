-- Enrichment + Archive Pipeline
-- A single event stream branches into three outputs:
--   1. Raw archive to S3 as Parquet
--   2. Enriched events (with user profiles) → Kafka
--   3. Premium user conversions (filtered) → Kafka

CREATE TABLE `user_events` (
  `event_id` STRING,
  `user_id` STRING,
  `event_type` STRING,
  `event_data` STRING,
  `event_time` TIMESTAMP(3),
  `processed_time` TIMESTAMP(3),
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '30' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'user_events',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

CREATE TABLE `user_profiles` (
  `user_id` STRING,
  `user_tier` STRING,
  `user_segment` STRING,
  `signup_date` DATE,
  PRIMARY KEY (`user_id`) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://db:5432/users',
  'table-name' = 'user_profiles',
  'driver' = 'org.postgresql.Driver',
  'lookup.cache' = 'LRU',
  'lookup.cache.max-rows' = '50000',
  'lookup.cache.ttl' = '300s'
);

CREATE TABLE `raw_archive` (
  `event_id` STRING,
  `user_id` STRING,
  `event_type` STRING,
  `event_data` STRING,
  `event_time` TIMESTAMP(3),
  `processed_time` TIMESTAMP(3)
) WITH (
  'connector' = 'filesystem',
  'path' = 's3://data-lake/raw/user_events',
  'format' = 'parquet',
  'sink.partition-commit.trigger' = 'process-time',
  'sink.partition-commit.delay' = '1 min',
  'sink.rolling-policy.file-size' = '128MB',
  'sink.rolling-policy.rollover-interval' = '15min'
);

CREATE TABLE `enriched_user_events` (
  `event_id` STRING,
  `user_id` STRING,
  `event_type` STRING,
  `event_data` STRING,
  `event_time` TIMESTAMP(3),
  `user_tier` STRING,
  `user_segment` STRING,
  `signup_date` DATE
) WITH (
  'connector' = 'kafka',
  'topic' = 'enriched_user_events',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

CREATE TABLE `premium_user_conversions` (
  `user_id` STRING,
  `event_type` STRING,
  `event_time` TIMESTAMP(3),
  `user_tier` STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'premium_user_conversions',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

-- Branch 1: Raw archive (no enrichment)
INSERT INTO `raw_archive`
SELECT * FROM `user_events`;

-- Create enriched view for branches 2 & 3
CREATE TEMPORARY VIEW `enriched` AS
SELECT
  e.`event_id`, e.`user_id`, e.`event_type`, e.`event_data`,
  e.`event_time`, p.`user_tier`, p.`user_segment`, p.`signup_date`
FROM `user_events` AS e
JOIN `user_profiles` FOR SYSTEM_TIME AS OF e.`proc_time` AS p
  ON e.`user_id` = p.`user_id`;

-- Branch 2: Enriched events
INSERT INTO `enriched_user_events`
SELECT * FROM `enriched`;

-- Branch 3: Premium conversions only
INSERT INTO `premium_user_conversions`
SELECT `user_id`, `event_type`, `event_time`, `user_tier`
FROM `enriched`
WHERE `user_tier` = 'premium'
  AND `event_type` IN ('purchase', 'subscription');
