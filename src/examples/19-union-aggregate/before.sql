-- Union + Window Aggregate: Cross-Platform Metrics
-- Unions mobile, web, and IoT event streams, then computes
-- hourly platform metrics in tumbling windows.

CREATE TABLE `mobile_events` (
  `event_id` STRING,
  `user_id` STRING,
  `event_type` STRING,
  `event_time` TIMESTAMP(3),
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '30' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'mobile_events',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

CREATE TABLE `web_events` (
  `event_id` STRING,
  `user_id` STRING,
  `event_type` STRING,
  `event_time` TIMESTAMP(3),
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '30' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'web_events',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

CREATE TABLE `iot_events` (
  `event_id` STRING,
  `device_id` STRING,
  `event_type` STRING,
  `event_time` TIMESTAMP(3),
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '30' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'iot_events',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

CREATE TABLE `platform_hourly_metrics` (
  `platform` STRING,
  `event_type` STRING,
  `event_count` BIGINT,
  `unique_users` BIGINT,
  `window_start` TIMESTAMP(3),
  `window_end` TIMESTAMP(3),
  PRIMARY KEY (`platform`, `event_type`, `window_start`) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://db:5432/analytics',
  'table-name' = 'platform_hourly_metrics',
  'driver' = 'org.postgresql.Driver'
);

-- Tag each source with its platform, then UNION ALL
INSERT INTO `platform_hourly_metrics`
SELECT
  `platform`,
  `event_type`,
  COUNT(*) AS `event_count`,
  COUNT(DISTINCT `user_id`) AS `unique_users`,
  `window_start`,
  `window_end`
FROM TABLE(
  TUMBLE(
    TABLE (
      SELECT `event_id`, `user_id`, `event_type`, `event_time`, 'mobile' AS `platform`
      FROM `mobile_events`

      UNION ALL

      SELECT `event_id`, `user_id`, `event_type`, `event_time`, 'web' AS `platform`
      FROM `web_events`

      UNION ALL

      SELECT `event_id`, `device_id` AS `user_id`, `event_type`, `event_time`, 'iot' AS `platform`
      FROM `iot_events`
    ),
    DESCRIPTOR(`event_time`),
    INTERVAL '1' HOUR
  )
)
GROUP BY `platform`, `event_type`, `window_start`, `window_end`;
