-- Multi-Stream Funnel Join
-- Joins page views → clicks → conversions with interval joins
-- to build a user-level funnel aggregate.

CREATE TABLE `page_views` (
  `session_id` STRING,
  `user_id` STRING,
  `page_url` STRING,
  `view_time` TIMESTAMP(3),
  WATERMARK FOR `view_time` AS `view_time` - INTERVAL '30' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'page_views',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

CREATE TABLE `clicks` (
  `click_id` STRING,
  `session_id` STRING,
  `element_id` STRING,
  `click_time` TIMESTAMP(3),
  WATERMARK FOR `click_time` AS `click_time` - INTERVAL '30' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'clicks',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

CREATE TABLE `conversions` (
  `conversion_id` STRING,
  `session_id` STRING,
  `product_id` STRING,
  `revenue` DECIMAL(10, 2),
  `conversion_time` TIMESTAMP(3),
  WATERMARK FOR `conversion_time` AS `conversion_time` - INTERVAL '30' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'conversions',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

CREATE TABLE `user_funnel_metrics` (
  `user_id` STRING,
  `total_views` BIGINT,
  `total_clicks` BIGINT,
  `total_conversions` BIGINT,
  `total_revenue` DECIMAL(10, 2),
  PRIMARY KEY (`user_id`) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://db:5433/analytics',
  'table-name' = 'user_funnel_metrics',
  'driver' = 'org.postgresql.Driver'
);

-- Two successive interval joins, then group aggregate
INSERT INTO `user_funnel_metrics`
SELECT
  `user_id`,
  COUNT(*) AS `total_views`,
  COUNT(`click_id`) AS `total_clicks`,
  COUNT(`conversion_id`) AS `total_conversions`,
  SUM(`revenue`) AS `total_revenue`
FROM (
  SELECT
    pv.`session_id`,
    pv.`user_id`,
    pv.`page_url`,
    pv.`view_time`,
    c.`click_id`,
    c.`element_id`,
    c.`click_time`,
    cv.`conversion_id`,
    cv.`product_id`,
    cv.`revenue`,
    cv.`conversion_time`
  FROM (
    SELECT
      pv.`session_id`,
      pv.`user_id`,
      pv.`page_url`,
      pv.`view_time`,
      c.`click_id`,
      c.`element_id`,
      c.`click_time`
    FROM `page_views` AS pv
    LEFT JOIN `clicks` AS c
      ON pv.`session_id` = c.`session_id`
      AND c.`click_time` BETWEEN pv.`view_time`
      AND pv.`view_time` + INTERVAL '30' MINUTE
  ) AS pv_clicks
  LEFT JOIN `conversions` AS cv
    ON pv_clicks.`session_id` = cv.`session_id`
    AND cv.`conversion_time` BETWEEN pv_clicks.`view_time`
    AND pv_clicks.`view_time` + INTERVAL '1' HOUR
)
GROUP BY `user_id`;
