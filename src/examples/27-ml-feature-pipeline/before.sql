-- ML Feature Pipeline (Batch)
-- Reads user interaction data and produces 3 feature tables:
--   1. User engagement features (per-user aggregates)
--   2. Item popularity features (per-item aggregates + conversion rate)
--   3. User-item interaction pairs

SET 'execution.runtime-mode' = 'batch';

CREATE TABLE `user_interactions` (
  `user_id` STRING,
  `item_id` STRING,
  `interaction_type` STRING,
  `interaction_time` TIMESTAMP(3),
  `duration_seconds` DOUBLE
) WITH (
  'connector' = 'filesystem',
  'path' = 's3://ml-data/user_interactions/',
  'format' = 'parquet'
);

CREATE TABLE `user_engagement_features` (
  `user_id` STRING,
  `total_interactions` BIGINT,
  `unique_items` BIGINT,
  `total_duration` DOUBLE,
  `avg_duration` DOUBLE
) WITH (
  'connector' = 'filesystem',
  'path' = 's3://ml-features/user_engagement_features/',
  'format' = 'parquet'
);

CREATE TABLE `item_popularity_features` (
  `item_id` STRING,
  `view_count` BIGINT,
  `unique_viewers` BIGINT,
  `purchase_count` BIGINT,
  `conversion_rate` DOUBLE
) WITH (
  'connector' = 'filesystem',
  'path' = 's3://ml-features/item_popularity_features/',
  'format' = 'parquet'
);

CREATE TABLE `user_item_pairs` (
  `user_id` STRING,
  `item_id` STRING,
  `interaction_count` BIGINT,
  `last_interaction` TIMESTAMP(3)
) WITH (
  'connector' = 'filesystem',
  'path' = 's3://ml-features/user_item_pairs/',
  'format' = 'parquet'
);

-- Feature table 1: User engagement
INSERT INTO `user_engagement_features`
SELECT
  `user_id`,
  COUNT(*) AS `total_interactions`,
  COUNT(DISTINCT `item_id`) AS `unique_items`,
  SUM(`duration_seconds`) AS `total_duration`,
  AVG(`duration_seconds`) AS `avg_duration`
FROM `user_interactions`
GROUP BY `user_id`;

-- Feature table 2: Item popularity with conversion rate
INSERT INTO `item_popularity_features`
SELECT
  `item_id`,
  `view_count`,
  `unique_viewers`,
  `purchase_count`,
  (CAST(`purchase_count` AS DOUBLE) / CAST(`view_count` AS DOUBLE)) AS `conversion_rate`
FROM (
  SELECT
    `item_id`,
    COUNT(*) AS `view_count`,
    COUNT(DISTINCT `user_id`) AS `unique_viewers`,
    SUM(CASE WHEN `interaction_type` = 'purchase' THEN 1 ELSE 0 END) AS `purchase_count`
  FROM `user_interactions`
  GROUP BY `item_id`
);

-- Feature table 3: User-item pairs
INSERT INTO `user_item_pairs`
SELECT
  `user_id`,
  `item_id`,
  COUNT(*) AS `interaction_count`,
  MAX(`interaction_time`) AS `last_interaction`
FROM `user_interactions`
GROUP BY `user_id`, `item_id`;
