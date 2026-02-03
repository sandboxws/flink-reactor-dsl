-- IoT Sensor Branching Pipeline
-- A single sensor data stream branches to 3 outputs:
--   1. High temperature alerts (temp > 100°) → Kafka
--   2. 5-minute aggregated metrics → JDBC
--   3. Raw archive → S3 Parquet

CREATE TABLE `iot_sensor_data` (
  `device_id` STRING,
  `sensor_type` STRING,
  `reading_value` DOUBLE,
  `reading_time` TIMESTAMP(3),
  `location_id` STRING,
  WATERMARK FOR `reading_time` AS `reading_time` - INTERVAL '30' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'iot_sensor_data',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

CREATE TABLE `high_temp_alerts` (
  `device_id` STRING,
  `sensor_type` STRING,
  `reading_value` DOUBLE,
  `reading_time` TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'high_temp_alerts',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

CREATE TABLE `sensor_metrics_5min` (
  `device_id` STRING,
  `sensor_type` STRING,
  `avg_value` DOUBLE,
  `min_value` DOUBLE,
  `max_value` DOUBLE,
  `window_end` TIMESTAMP(3),
  PRIMARY KEY (`device_id`, `sensor_type`, `window_end`) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://db:5432/iot',
  'table-name' = 'sensor_metrics_5min',
  'driver' = 'org.postgresql.Driver'
);

CREATE TABLE `iot_archive` (
  `device_id` STRING,
  `sensor_type` STRING,
  `reading_value` DOUBLE,
  `reading_time` TIMESTAMP(3),
  `location_id` STRING,
  `dt` DATE
) PARTITIONED BY (`dt`) WITH (
  'connector' = 'filesystem',
  'path' = 's3://iot-archive/raw/',
  'format' = 'parquet'
);

-- Branch 1: High temperature alerts
INSERT INTO `high_temp_alerts`
SELECT `device_id`, `sensor_type`, `reading_value`, `reading_time`
FROM `iot_sensor_data`
WHERE `sensor_type` = 'temperature' AND `reading_value` > 100;

-- Branch 2: 5-minute windowed metrics
INSERT INTO `sensor_metrics_5min`
SELECT
  `device_id`,
  `sensor_type`,
  AVG(`reading_value`) AS `avg_value`,
  MIN(`reading_value`) AS `min_value`,
  MAX(`reading_value`) AS `max_value`,
  `window_end`
FROM TABLE(
  TUMBLE(TABLE `iot_sensor_data`, DESCRIPTOR(`reading_time`), INTERVAL '5' MINUTE)
)
GROUP BY `device_id`, `sensor_type`, `window_start`, `window_end`;

-- Branch 3: Raw archive to S3
INSERT INTO `iot_archive`
SELECT *, CAST(`reading_time` AS DATE) AS `dt`
FROM `iot_sensor_data`;
