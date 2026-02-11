-- Tumble window page-view counts
-- Exercises: window aggregation, watermarks, checkpointing

CREATE TABLE `page_views` (
  `user_id` INT,
  `page_url` STRING,
  `view_time` TIMESTAMP(3),
  WATERMARK FOR `view_time` AS `view_time` - INTERVAL '5' SECOND
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '100',
  'fields.user_id.min' = '1',
  'fields.user_id.max' = '1000',
  'fields.page_url.length' = '10'
);

CREATE TABLE `page_view_counts` (
  `window_start` TIMESTAMP(3),
  `window_end` TIMESTAMP(3),
  `page_url` STRING,
  `view_count` BIGINT
) WITH (
  'connector' = 'print'
);

INSERT INTO `page_view_counts`
SELECT
  TUMBLE_START(`view_time`, INTERVAL '1' MINUTE) AS `window_start`,
  TUMBLE_END(`view_time`, INTERVAL '1' MINUTE) AS `window_end`,
  `page_url`,
  COUNT(*) AS `view_count`
FROM `page_views`
GROUP BY
  TUMBLE(`view_time`, INTERVAL '1' MINUTE),
  `page_url`;
