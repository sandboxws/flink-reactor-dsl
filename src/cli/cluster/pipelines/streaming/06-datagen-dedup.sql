-- ROW_NUMBER dedup pattern
-- Exercises: deduplication, state cleanup, high-throughput processing

CREATE TABLE `raw_events` (
  `event_id` STRING,
  `user_id` INT,
  `action` STRING,
  `event_time` TIMESTAMP(3),
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '10' SECOND
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '100',
  'fields.event_id.length' = '8',
  'fields.user_id.min' = '1',
  'fields.user_id.max' = '200',
  'fields.action.length' = '6'
);

CREATE TABLE `deduped_events` (
  `event_id` STRING,
  `user_id` INT,
  `action` STRING,
  `event_time` TIMESTAMP(3)
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO `deduped_events`
SELECT `event_id`, `user_id`, `action`, `event_time`
FROM (
  SELECT
    `event_id`,
    `user_id`,
    `action`,
    `event_time`,
    ROW_NUMBER() OVER (PARTITION BY `event_id` ORDER BY `event_time` ASC) AS `row_num`
  FROM `raw_events`
)
WHERE `row_num` = 1;
