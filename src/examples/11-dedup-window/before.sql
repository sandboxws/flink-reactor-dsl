-- Deduplication + Windowed Aggregate
-- Deduplicates raw events by event_id, then computes
-- hourly event counts per user and event type.

CREATE TABLE `raw_events` (
  `event_id` STRING,
  `user_id` STRING,
  `event_type` STRING,
  `event_data` STRING,
  `event_time` TIMESTAMP(3),
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'raw_events',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

CREATE TABLE `hourly_user_events` (
  `user_id` STRING,
  `event_type` STRING,
  `event_count` BIGINT,
  `window_start` TIMESTAMP(3),
  `window_end` TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'hourly_user_events',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

INSERT INTO `hourly_user_events`
SELECT
  `user_id`,
  `event_type`,
  COUNT(*) AS `event_count`,
  `window_start`,
  `window_end`
FROM TABLE(
  TUMBLE(
    TABLE (
      -- Deduplicate: keep first row per event_id ordered by event_time
      SELECT *
      FROM (
        SELECT *,
          ROW_NUMBER() OVER (
            PARTITION BY `event_id`
            ORDER BY `event_time` ASC
          ) AS `rownum`
        FROM `raw_events`
      )
      WHERE `rownum` = 1
    ),
    DESCRIPTOR(`event_time`),
    INTERVAL '1' HOUR
  )
)
GROUP BY `user_id`, `event_type`, `window_start`, `window_end`;
