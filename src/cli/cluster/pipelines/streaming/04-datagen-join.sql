-- Two datagen sources joined on user_id
-- Exercises: multi-source DAG, equi-join, state management

CREATE TABLE `clicks` (
  `user_id` INT,
  `url` STRING,
  `click_time` TIMESTAMP(3),
  WATERMARK FOR `click_time` AS `click_time` - INTERVAL '10' SECOND
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '80',
  'fields.user_id.min' = '1',
  'fields.user_id.max' = '50',
  'fields.url.length' = '12'
);

CREATE TABLE `impressions` (
  `user_id` INT,
  `ad_id` INT,
  `impression_time` TIMESTAMP(3),
  WATERMARK FOR `impression_time` AS `impression_time` - INTERVAL '10' SECOND
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '60',
  'fields.user_id.min' = '1',
  'fields.user_id.max' = '50',
  'fields.ad_id.min' = '1',
  'fields.ad_id.max' = '100'
);

CREATE TABLE `click_through` (
  `user_id` INT,
  `url` STRING,
  `ad_id` INT,
  `click_time` TIMESTAMP(3),
  `impression_time` TIMESTAMP(3)
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO `click_through`
SELECT
  c.`user_id`,
  c.`url`,
  i.`ad_id`,
  c.`click_time`,
  i.`impression_time`
FROM `clicks` c
JOIN `impressions` i
  ON c.`user_id` = i.`user_id`
  AND c.`click_time` BETWEEN i.`impression_time` - INTERVAL '5' MINUTE AND i.`impression_time` + INTERVAL '5' MINUTE;
