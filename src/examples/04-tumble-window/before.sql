-- Tumbling Window Analytics
-- Counts page views per user in 1-minute tumbling windows,
-- then filters for active users with more than 5 views.

CREATE TABLE `clickstream` (
  `user_id` STRING,
  `page_url` STRING,
  `session_id` STRING,
  `event_time` TIMESTAMP(3),
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '10' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'clickstream',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

CREATE TABLE `active_users_per_minute` (
  `user_id` STRING,
  `page_views` BIGINT,
  `unique_pages` BIGINT,
  `window_start` TIMESTAMP(3),
  `window_end` TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'active_users_per_minute',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

INSERT INTO `active_users_per_minute`
SELECT
  `user_id`,
  `page_views`,
  `unique_pages`,
  `window_start`,
  `window_end`
FROM (
  SELECT
    `user_id`,
    COUNT(*) AS `page_views`,
    COUNT(DISTINCT `page_url`) AS `unique_pages`,
    `window_start`,
    `window_end`
  FROM TABLE(
    TUMBLE(TABLE `clickstream`, DESCRIPTOR(`event_time`), INTERVAL '1' MINUTE)
  )
  GROUP BY `user_id`, `window_start`, `window_end`
)
WHERE `page_views` > 5;
