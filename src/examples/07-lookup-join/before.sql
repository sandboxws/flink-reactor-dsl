-- Lookup Join: User Action Enrichment
-- Enriches streaming user actions with user profile data from a
-- JDBC dimension table using async lookup with LRU cache.

CREATE TABLE `user_actions` (
  `action_id` STRING,
  `user_id` STRING,
  `action_type` STRING,
  `action_time` TIMESTAMP(3),
  `metadata` STRING,
  `proc_time` AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'user_actions',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

CREATE TABLE `user_profiles` (
  `user_id` STRING,
  `user_name` STRING,
  `user_tier` STRING,
  `user_region` STRING,
  PRIMARY KEY (`user_id`) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://db:3306/users',
  'table-name' = 'user_profiles',
  'driver' = 'com.mysql.cj.jdbc.Driver',
  'lookup.cache' = 'LRU',
  'lookup.cache.max-rows' = '10000',
  'lookup.cache.ttl' = '60s',
  'lookup.max-retries' = '3'
);

CREATE TABLE `premium_user_actions` (
  `action_id` STRING,
  `user_id` STRING,
  `action_type` STRING,
  `action_time` TIMESTAMP(3),
  `user_name` STRING,
  `user_tier` STRING,
  `user_region` STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'premium_user_actions',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

INSERT INTO `premium_user_actions`
SELECT
  a.`action_id`,
  a.`user_id`,
  a.`action_type`,
  a.`action_time`,
  p.`user_name`,
  p.`user_tier`,
  p.`user_region`
FROM `user_actions` AS a
JOIN `user_profiles` FOR SYSTEM_TIME AS OF a.`proc_time` AS p
  ON a.`user_id` = p.`user_id`
WHERE p.`user_tier` IN ('premium', 'enterprise');
