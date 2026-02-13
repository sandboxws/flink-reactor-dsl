-- IoT daily sensor report: average readings by device and type
-- Exercises: batch mode, datagen source, GROUP BY aggregate, IoT telemetry

SET 'execution.runtime-mode' = 'batch';

CREATE TABLE `sensor_readings` (
  `device_id` STRING,
  `sensor_type` STRING,
  `value` DOUBLE,
  `reading_date` STRING
) WITH (
  'connector' = 'datagen',
  'number-of-rows' = '5000',
  'fields.device_id.length' = '7',
  'fields.sensor_type.length' = '5',
  'fields.value.min' = '10',
  'fields.value.max' = '100',
  'fields.reading_date.length' = '10'
);

CREATE TABLE `daily_sensor_report` (
  `device_id` STRING,
  `sensor_type` STRING,
  `avg_value` DOUBLE,
  `min_value` DOUBLE,
  `max_value` DOUBLE,
  `reading_count` BIGINT
) WITH (
  'connector' = 'print'
);

INSERT INTO `daily_sensor_report`
SELECT
  `device_id`,
  `sensor_type`,
  AVG(`value`) AS `avg_value`,
  MIN(`value`) AS `min_value`,
  MAX(`value`) AS `max_value`,
  COUNT(*) AS `reading_count`
FROM `sensor_readings`
GROUP BY `device_id`, `sensor_type`;
