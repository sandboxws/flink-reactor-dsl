-- Hopping (Sliding) Window
-- Computes 5-minute rolling averages of sensor readings
-- with 1-minute slide intervals for IoT monitoring.

CREATE TABLE `sensor_readings` (
  `sensor_id` STRING,
  `temperature` DOUBLE,
  `humidity` DOUBLE,
  `reading_time` TIMESTAMP(3),
  WATERMARK FOR `reading_time` AS `reading_time` - INTERVAL '30' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'sensor_readings',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

CREATE TABLE `sensor_metrics` (
  `sensor_id` STRING,
  `avg_temp` DOUBLE,
  `max_temp` DOUBLE,
  `min_temp` DOUBLE,
  `avg_humidity` DOUBLE,
  `window_start` TIMESTAMP(3),
  `window_end` TIMESTAMP(3),
  `temp_variance` DOUBLE,
  PRIMARY KEY (`sensor_id`, `window_start`) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://db:5432/iot',
  'table-name' = 'sensor_metrics',
  'driver' = 'org.postgresql.Driver'
);

INSERT INTO `sensor_metrics`
SELECT
  `sensor_id`,
  `avg_temp`,
  `max_temp`,
  `min_temp`,
  `avg_humidity`,
  `window_start`,
  `window_end`,
  (`max_temp` - `min_temp`) AS `temp_variance`
FROM (
  SELECT
    `sensor_id`,
    AVG(`temperature`) AS `avg_temp`,
    MAX(`temperature`) AS `max_temp`,
    MIN(`temperature`) AS `min_temp`,
    AVG(`humidity`) AS `avg_humidity`,
    `window_start`,
    `window_end`
  FROM TABLE(
    HOP(
      TABLE `sensor_readings`,
      DESCRIPTOR(`reading_time`),
      INTERVAL '1' MINUTE,
      INTERVAL '5' MINUTE
    )
  )
  GROUP BY `sensor_id`, `window_start`, `window_end`
);
