-- STATEMENT SET fan-out to 3 sinks
-- Exercises: multi-sink DAG topology, STATEMENT SET, diverse vertex graph

CREATE TABLE `events` (
  `event_id` INT,
  `event_type` STRING,
  `user_id` INT,
  `payload` STRING,
  `event_time` TIMESTAMP(3),
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '200',
  'fields.event_id.min' = '1',
  'fields.event_id.max' = '999999',
  'fields.event_type.length' = '5',
  'fields.user_id.min' = '1',
  'fields.user_id.max' = '200',
  'fields.payload.length' = '20'
);

CREATE TABLE `all_events_archive` (
  `event_id` INT,
  `event_type` STRING,
  `user_id` INT,
  `payload` STRING,
  `event_time` TIMESTAMP(3)
) WITH (
  'connector' = 'blackhole'
);

CREATE TABLE `event_log` (
  `event_id` INT,
  `event_type` STRING,
  `user_id` INT,
  `event_time` TIMESTAMP(3)
) WITH (
  'connector' = 'print'
);

CREATE TABLE `event_metrics` (
  `event_type` STRING,
  `event_count` BIGINT
) WITH (
  'connector' = 'blackhole'
);

EXECUTE STATEMENT SET
BEGIN
  INSERT INTO `all_events_archive`
  SELECT `event_id`, `event_type`, `user_id`, `payload`, `event_time`
  FROM `events`;

  INSERT INTO `event_log`
  SELECT `event_id`, `event_type`, `user_id`, `event_time`
  FROM `events`;

  INSERT INTO `event_metrics`
  SELECT `event_type`, COUNT(*) AS `event_count`
  FROM `events`
  GROUP BY `event_type`;
END;
