-- Session Window: User Activity Sessions
-- Groups user activity into sessions with 30-minute inactivity gaps,
-- computing session duration and activity sequence.

CREATE TABLE `user_activity` (
  `user_id` STRING,
  `activity_type` STRING,
  `page_url` STRING,
  `activity_time` TIMESTAMP(3),
  `device_info` STRING,
  WATERMARK FOR `activity_time` AS `activity_time` - INTERVAL '1' MINUTE
) WITH (
  'connector' = 'kafka',
  'topic' = 'user_activity',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

CREATE TABLE `user_sessions` (
  `user_id` STRING,
  `total_activities` BIGINT,
  `unique_pages` BIGINT,
  `session_start` TIMESTAMP(3),
  `session_end` TIMESTAMP(3),
  `session_duration` INTERVAL DAY TO SECOND,
  `activity_sequence` STRING,
  PRIMARY KEY (`user_id`, `session_start`) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://db:5432/analytics',
  'table-name' = 'user_sessions',
  'driver' = 'org.postgresql.Driver'
);

INSERT INTO `user_sessions`
SELECT
  `user_id`,
  `total_activities`,
  `unique_pages`,
  `session_start`,
  `session_end`,
  (`session_end` - `session_start`) AS `session_duration`,
  `activity_sequence`
FROM (
  SELECT
    `user_id`,
    COUNT(*) AS `total_activities`,
    COUNT(DISTINCT `page_url`) AS `unique_pages`,
    MIN(`activity_time`) AS `session_start`,
    MAX(`activity_time`) AS `session_end`,
    LISTAGG(`activity_type`) AS `activity_sequence`
  FROM TABLE(
    SESSION(
      TABLE `user_activity`,
      DESCRIPTOR(`activity_time`),
      INTERVAL '30' MINUTE
    )
  )
  GROUP BY `user_id`, `window_start`, `window_end`
);
