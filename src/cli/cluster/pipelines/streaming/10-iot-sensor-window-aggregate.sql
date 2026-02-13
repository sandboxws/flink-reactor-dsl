-- IoT sensor 1-minute windowed averages
-- Exercises: Kafka CDC source, tumble window, watermarks, IoT telemetry

CREATE TABLE `iot_telemetry` (
  `device_id` STRING,
  `sensor_type` STRING,
  `value` DOUBLE,
  `unit` STRING,
  `reading_time` TIMESTAMP(3),
  `location` STRING,
  WATERMARK FOR `reading_time` AS `reading_time` - INTERVAL '30' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'iot.telemetry.readings',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-iot-window-agg',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'debezium-json'
);

CREATE TABLE `sensor_1min_avg` (
  `window_start` TIMESTAMP(3),
  `window_end` TIMESTAMP(3),
  `device_id` STRING,
  `sensor_type` STRING,
  `avg_value` DOUBLE,
  `min_value` DOUBLE,
  `max_value` DOUBLE,
  `reading_count` BIGINT
) WITH (
  'connector' = 'print'
);

INSERT INTO `sensor_1min_avg`
SELECT
  TUMBLE_START(`reading_time`, INTERVAL '1' MINUTE) AS `window_start`,
  TUMBLE_END(`reading_time`, INTERVAL '1' MINUTE) AS `window_end`,
  `device_id`,
  `sensor_type`,
  AVG(`value`) AS `avg_value`,
  MIN(`value`) AS `min_value`,
  MAX(`value`) AS `max_value`,
  COUNT(*) AS `reading_count`
FROM `iot_telemetry`
GROUP BY
  TUMBLE(`reading_time`, INTERVAL '1' MINUTE),
  `device_id`,
  `sensor_type`;
