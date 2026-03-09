-- Broadcast Anti-Join: Blacklist Filtering
-- Filters out events from blacklisted users by broadcasting
-- a small blacklist table and performing a LEFT ANTI JOIN.

CREATE TABLE `user_events` (
  `event_id` STRING,
  `user_id` STRING,
  `event_type` STRING,
  `product_id` STRING,
  `event_time` TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'user_events',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

CREATE TABLE `blacklisted_users` (
  `user_id` STRING,
  `reason` STRING,
  `blocked_until` TIMESTAMP(3),
  PRIMARY KEY (`user_id`) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://db:5433/moderation',
  'table-name' = 'blacklist',
  'driver' = 'org.postgresql.Driver'
);

CREATE TABLE `valid_events` (
  `event_id` STRING,
  `user_id` STRING,
  `event_type` STRING,
  `product_id` STRING,
  `event_time` TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'valid_events',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

INSERT INTO `valid_events`
SELECT /*+ BROADCAST(b) */
  e.`event_id`,
  e.`user_id`,
  e.`event_type`,
  e.`product_id`,
  e.`event_time`
FROM `user_events` AS e
LEFT ANTI JOIN `blacklisted_users` AS b
  ON e.`user_id` = b.`user_id`;
